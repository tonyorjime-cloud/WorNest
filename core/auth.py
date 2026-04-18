import streamlit as st


def current_user():
    return st.session_state.get("user")


def user_role():
    u = current_user()
    if not u:
        return None
    r = (u.get('role') or '').strip()
    if r:
        return r
    # Backward compatibility
    return 'admin' if int(u.get('is_admin', 0) or 0) == 1 else 'staff'


def is_admin():
    return user_role() == 'admin' or int((current_user() or {}).get('is_admin', 0) or 0) == 1


def current_staff_id():
    u = current_user()
    if not u:
        return None
    sid = u.get("staff_id")
    try:
        return int(sid) if sid is not None else None
    except Exception:
        return None
