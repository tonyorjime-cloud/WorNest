from core.auth import current_user, is_admin, user_role
from core.db import fetch_df


def is_reviewer():
    return is_admin()


def is_sub_admin():
    return user_role() == 'sub_admin' or is_admin()


def is_section_head():
    return user_role() == 'section_head' or is_admin()


def can_import_csv():
    return is_admin()


def can_manage_projects():
    # create/edit/delete projects
    return is_admin()


def _get_user_permissions(user_id: int) -> dict:
    if user_id is None:
        return {"can_assign_tasks": 0, "can_confirm_task_completion": 0, "can_upload_project_docs": 0}
    df = fetch_df(
        "SELECT can_assign_tasks, can_confirm_task_completion, can_upload_project_docs FROM user_permissions WHERE user_id=?",
        (int(user_id),),
    )
    if df.empty:
        return {"can_assign_tasks": 0, "can_confirm_task_completion": 0, "can_upload_project_docs": 0}
    r = df.iloc[0].to_dict()
    return {
        "can_assign_tasks": int(r.get("can_assign_tasks") or 0),
        "can_confirm_task_completion": int(r.get("can_confirm_task_completion") or 0),
        "can_upload_project_docs": int(r.get("can_upload_project_docs") or 0),
    }


def has_perm(flag: str) -> bool:
    u = current_user()
    if not u:
        return False
    if is_admin():
        return True
    perms = _get_user_permissions(int(u.get("id")))
    return int(perms.get(flag) or 0) == 1


def can_upload_core_docs():
    # Core project documents (drawings, approvals, etc.)
    # Admin always. Sub-admin only when explicitly enabled.
    if is_admin():
        return True
    return (user_role() == 'sub_admin') and has_perm('can_upload_project_docs')


def can_assign_tasks():
    # Create/assign tasks
    if is_admin():
        return True
    return (user_role() == 'section_head') and has_perm('can_assign_tasks')


def can_confirm_task_completion():
    if is_admin():
        return True
    return (user_role() == 'section_head') and has_perm('can_confirm_task_completion')


def can_approve_leave():
    return is_admin()
