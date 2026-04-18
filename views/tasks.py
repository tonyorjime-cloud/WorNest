import html
import os
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import streamlit as st
from dateutil import parser as dtparser

from core.auth import current_staff_id, is_admin, user_role
from core.dates import _monthly_points_window, _parse_date_safe, _today
from core.db import execute, fetch_df
from core.permissions import can_assign_tasks, can_confirm_task_completion, is_section_head
from core.tasks_service import (
    _report_scoreboard,
    _task_points,
    can_download_task_files,
    can_upload_task_files,
    compliance_snapshot,
    current_staff_section,
    file_download_button,
    run_task_reminders,
    save_uploaded_file,
    send_push,
    smtp_configured,
)


def page_tasks():
    st.markdown("<div class='worknest-header'><h2>🗂️ Tasks & Performance</h2></div>", unsafe_allow_html=True)

    today = _today()
    monthly_start, monthly_end = _monthly_points_window(today)
    comp = compliance_snapshot(today)

    st.markdown("### ⏰ Reminders")
    ass = fetch_df(
        """
        SELECT
            ta.id AS assignment_id,
            t.title,
            s.name AS staff,
            COALESCE(p.code || '  ' || p.name, p.code, '') AS project,
            t.due_date,
            ta.status
        FROM task_assignments ta
        JOIN tasks t ON t.id=ta.task_id
        JOIN staff s ON s.id=ta.staff_id
        LEFT JOIN projects p ON p.id=t.project_id
        WHERE ta.status!='Completed'
        ORDER BY date(t.due_date) ASC
    """
    )
    if ass.empty:
        st.caption("No open assignments, so no reminders.")
    else:
        ass["due_date"] = ass["due_date"].astype(str)
        ass["days_to_due"] = ass["due_date"].apply(lambda d: (dtparser.parse(d).date() - date.today()).days if d else None)
        due_soon = ass[(ass["days_to_due"].notna()) & (ass["days_to_due"] >= 0) & (ass["days_to_due"] <= 2)].copy()
        overdue = ass[(ass["days_to_due"].notna()) & (ass["days_to_due"] < 0)].copy()

        c1, c2 = st.columns(2)
        with c1:
            st.caption("Due soon (02 days)")
            st.dataframe(
                due_soon[["project", "title", "staff", "due_date", "days_to_due"]]
                if not due_soon.empty
                else pd.DataFrame(columns=["project", "title", "staff", "due_date", "days_to_due"]),
                width='stretch',
            )
        with c2:
            st.caption("Overdue")
            st.dataframe(
                overdue[["project", "title", "staff", "due_date", "days_to_due"]]
                if not overdue.empty
                else pd.DataFrame(columns=["project", "title", "staff", "due_date", "days_to_due"]),
                width='stretch',
            )

        if is_admin():
            st.caption("Email reminders are optional. Configure SMTP_* env vars to enable sending.")
            if st.button("📨 Run reminder email check now", key="run_reminders_now"):
                stats = run_task_reminders()
                if smtp_configured():
                    st.success(
                        f"Checked {stats['checked']} assignments. Sent {stats['sent']} emails. "
                        f"Skipped {stats['skipped']}. Errors {stats['errors']}."
                    )
                else:
                    st.warning("SMTP is not configured, so no emails were sent. In-app reminders above still work.")
    staff = fetch_df("SELECT id,name,section,email FROM staff ORDER BY name")
    projects = fetch_df("SELECT id,code,name,start_date,next_due_date FROM projects ORDER BY code")
    st.subheader("Tasks")
    titles = fetch_df("SELECT id,title FROM tasks ORDER BY id DESC")
    mode_options = ["Edit existing"] if not can_assign_tasks() else ["Create new", "Edit existing"]
    mode = st.radio("Mode", mode_options, horizontal=True, key="tsk_mode")

    if (not can_assign_tasks()) and mode == "Create new":
        mode = "Edit existing"

    if mode == "Edit existing" and titles.empty:
        st.info("No tasks to edit. Switch to 'Create new'.")
        mode = "Create new"
    if mode == "Edit existing":
        label_map = {f"#{r['id']}  {r['title']}": int(r['id']) for _, r in titles.iterrows()}
        pick = st.selectbox("Select task", list(label_map.keys()), key="tsk_pick")
        tid = label_map[pick]
        trow = fetch_df("SELECT * FROM tasks WHERE id=?", (tid,)).iloc[0]
        task_dict = dict(trow)
        tkey = f"_{int(tid)}"
        can_edit = can_assign_tasks()
        if (not is_admin()) and user_role() == 'section_head':
            sec = current_staff_section()
            if sec:
                staff_allowed = staff[staff["section"].fillna("").str.strip() == sec].copy()
            else:
                staff_allowed = staff.iloc[0:0].copy()
        else:
            staff_allowed = staff.copy()

        if (not is_admin()) and user_role() == 'section_head':
            existing_ass = fetch_df(
                "SELECT s.section FROM task_assignments ta JOIN staff s ON s.id=ta.staff_id WHERE ta.task_id=?",
                (tid,),
            )
            if not existing_ass.empty:
                sec = current_staff_section() or ""
                bad = existing_ass["section"].fillna("").str.strip().apply(lambda x: x != sec).any()
                if bad:
                    can_edit = False
        current_assignees = fetch_df(
            "SELECT name FROM task_assignments ta JOIN staff s ON s.id=ta.staff_id WHERE ta.task_id=?",
            (tid,),
        )["name"].tolist()
        proj_opt = [""] + [f"{r['code']}  {r['name']}" for _, r in projects.iterrows()]
        proj_value = ""
        if pd.notna(trow["project_id"]):
            pr = projects[projects["id"] == int(trow["project_id"])]
            if not pr.empty:
                proj_value = f"{pr['code'].iloc[0]}  {pr['name'].iloc[0]}"
        assigned_by_name = ""
        try:
            creator = fetch_df("SELECT name FROM staff WHERE id=?", (int(trow.get("created_by")),))
            if not creator.empty:
                assigned_by_name = str(creator["name"].iloc[0])
        except Exception:
            assigned_by_name = ""
        title_value = str(trow["title"] or "")
        desc_value = str(trow["description"] or "")
        date_assigned = dtparser.parse(trow["date_assigned"]).date()
        due = dtparser.parse(trow["due_date"]).date()
        da = int(max((due - date_assigned).days + 1, 1))

        st.markdown(
            f"""
            <div style="background:#ffffff;border:1px solid #d9d9d9;border-radius:10px;padding:16px 18px;margin:8px 0 14px 0;color:#111;">
              <div style="font-size:1.15rem;font-weight:700;margin-bottom:10px;">{html.escape(title_value)}</div>
              <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:8px 18px;line-height:1.6;">
                <div><span style="font-weight:700;">Assigned by:</span> {html.escape(assigned_by_name or '')}</div>
                <div><span style="font-weight:700;">Date assigned:</span> {html.escape(str(date_assigned))}</div>
                <div><span style="font-weight:700;">Due date:</span> {html.escape(str(due))}</div>
                <div><span style="font-weight:700;">Days allotted:</span> {da}</div>
                <div><span style="font-weight:700;">Project:</span> {html.escape(proj_value)}</div>
                <div><span style="font-weight:700;">Assignees:</span> {html.escape(', '.join(current_assignees) if current_assignees else '')}</div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown("**Description**")
        st.markdown(
            f"""
            <div style="background:#f8f9fa;padding:16px;border-radius:10px;border:1px solid #d9d9d9;color:#111;white-space:pre-wrap;line-height:1.65;margin-bottom:16px;">
              {html.escape(desc_value) if desc_value else '<span style="color:#555;">No description provided.</span>'}
            </div>
            """,
            unsafe_allow_html=True,
        )

        if can_edit:
            st.markdown("### Edit task")
            title = st.text_input("Title", value=title_value, key=f"tsk_title{tkey}")
            desc = st.text_area("Description", value=desc_value, key=f"tsk_desc{tkey}", height=240)
            date_assigned = st.date_input("Date assigned", value=date_assigned, key=f"tsk_da{tkey}")
            due = st.date_input("Due date", value=due, key=f"tsk_due{tkey}")
            da = int(max((due - date_assigned).days + 1, 1))
            st.write(f"Days allotted (auto): **{da}**")
            proj = st.selectbox(
                "Project (optional)",
                proj_opt,
                index=proj_opt.index(proj_value) if proj_value in proj_opt else 0,
                key=f"tsk_proj{tkey}",
            )
            assignees = st.multiselect(
                "Assignees",
                staff_allowed["name"].tolist(),
                key=f"tsk_asg{tkey}",
                default=current_assignees,
            )
        else:
            title = title_value
            desc = desc_value
            proj = proj_value
            assignees = current_assignees
        colA, colB, colC = st.columns(3)
        with colA:
            if can_edit and st.button("💾 Save", key=f"tsk_save{tkey}"):
                execute(
                    "UPDATE tasks SET title=?,description=?,date_assigned=?,days_allotted=?,due_date=?,project_id=? WHERE id=?",
                    (
                        title,
                        desc or None,
                        str(date_assigned),
                        int(da),
                        str(due),
                        int(projects[projects['code'] == proj.split('  ')[0]]['id'].iloc[0]) if proj != "" else None,
                        tid,
                    ),
                )
                execute("DELETE FROM task_assignments WHERE task_id=?", (tid,))
                for nm in assignees:
                    sid = int(staff[staff["name"] == nm]["id"].iloc[0])
                    execute("INSERT INTO task_assignments (task_id,staff_id,status) VALUES (?,?,?)", (tid, sid, "In progress"))
                st.success("Task updated.")
                st.rerun()
        with colB:
            can_confirm = can_confirm_task_completion()
            btn_label = " Confirm Completed (today)" if not is_admin() else " Admin: Certify Completed (today)"
            if st.button(btn_label, key=f"tsk_done{tkey}", disabled=not can_confirm):
                if (not is_admin()) and user_role() == 'section_head':
                    sec = current_staff_section() or ""
                    chk = fetch_df(
                        """SELECT s.section FROM task_assignments ta
                                           JOIN staff s ON s.id=ta.staff_id
                                           WHERE ta.task_id=?""",
                        (tid,),
                    )
                    if (not chk.empty) and chk["section"].fillna("").str.strip().apply(lambda x: x != sec).any():
                        st.error("You can only confirm completion for tasks assigned within your section.")
                        st.stop()

                today_d = date.today()
                today_str = str(today_d)
                da = _parse_date_safe(trow["date_assigned"]) or today_d
                ass = fetch_df("SELECT id, staff_id FROM task_assignments WHERE task_id=?", (tid,))
                for _, ar in ass.iterrows():
                    days_taken = int((today_d - da).days)
                    execute(
                        "UPDATE task_assignments SET status='Completed', completed_date=?, days_taken=? WHERE id=?",
                        (today_str, days_taken, int(ar["id"])),
                    )
                    task_pts = _task_points(trow["date_assigned"], int(trow.get("days_allotted") or 0), today_str)
                    try:
                        execute(
                            "INSERT OR IGNORE INTO points (staff_id, source, source_id, points, awarded_at) VALUES (?,?,?,?,?)",
                            (int(ar["staff_id"]), "task", int(ar["id"]), int(task_pts), datetime.now().isoformat(timespec="seconds")),
                        )
                    except Exception:
                        pass
                st.success("Completion confirmed for all assignees.")
                st.rerun()

            if is_admin():
                if st.button("🗑️ Admin: Delete Task", key=f"tsk_del{tkey}"):
                    execute("DELETE FROM task_assignments WHERE task_id=?", (tid,))
                    execute("DELETE FROM tasks WHERE id=?", (tid,))
                    st.success("Task deleted.")
                    st.rerun()
            else:
                st.caption("Delete is Admin-only.")
        with colC:
            st.caption("Scores only computed for **Completed** tasks. Overdue **In progress** tasks are flagged below.")

    if mode == "Edit existing":
        st.markdown("####  Task Attachments")
        attach_files = st.file_uploader(
            "Attach files (PDF/Image)",
            type=["pdf", "png", "jpg", "jpeg"],
            accept_multiple_files=True,
            key=f"tsk_attach_{tid}",
        )
        if st.button("📎 Upload Attachment(s)", key=f"tsk_attach_btn_{tid}"):
            if not can_upload_task_files(task_dict):
                st.error("You don't have permission to upload attachments for this task.")
                st.stop()
            if not attach_files:
                st.error("Select one or more files first.")
            else:
                ok = 0
                for f in attach_files:
                    path = save_uploaded_file(f, f"task_{tid}/attachments")
                    if path:
                        execute(
                            """INSERT INTO task_documents (task_id,file_path,original_name,uploaded_at,uploader_staff_id)
                                   VALUES (?,?,?,?,?)""",
                            (int(tid), path, getattr(f, "name", None), datetime.now().isoformat(timespec="seconds"), current_staff_id()),
                        )
                        ok += 1
                st.success(f"Uploaded {ok} attachment(s).")
                st.rerun()

        adf = fetch_df(
            "SELECT id, original_name, file_path, uploaded_at FROM task_documents WHERE task_id=? ORDER BY uploaded_at DESC",
            (int(tid),),
        )
        if adf.empty:
            st.caption("No attachments yet.")
        else:
            for _, r in adf.iterrows():
                c1, c2, c3 = st.columns([4, 1, 1])
                with c1:
                    nm = r["original_name"] if pd.notna(r["original_name"]) else os.path.basename(r["file_path"])
                    st.write(f"**{nm}**  \n*{r['uploaded_at']}*")
                with c2:
                    if can_download_task_files(task_dict):
                        file_download_button("⬇️ Download", r["file_path"], key=f"tsk_adl_{tid}_{int(r['id'])}")
                    else:
                        st.caption("🔒")
                with c3:
                    if is_admin() and st.button("🗑️", key=f"tsk_adel_{tid}_{int(r['id'])}"):
                        execute("DELETE FROM task_documents WHERE id=?", (int(r["id"]),))
                        st.success("Attachment removed.")
                        st.rerun()

    else:
        if (not is_admin()) and user_role() == 'section_head':
            sec = current_staff_section()
            if sec:
                staff_allowed_new = staff[staff["section"].fillna("").str.strip() == sec].copy()
            else:
                staff_allowed_new = staff.iloc[0:0].copy()
        else:
            staff_allowed_new = staff.copy()

        title = st.text_input("Title", key="tsk_title_new")
        desc = st.text_area("Description", key="tsk_desc_new")
        date_assigned = st.date_input("Date assigned", value=date.today(), key="tsk_da_new")
        due = st.date_input("Due date", value=date.today() + timedelta(days=7), key="tsk_due_new")
        da = int(max((due - date_assigned).days + 1, 1))
        st.write(f"Days allotted (auto): **{da}**")
        proj_opt = [""] + [f"{r['code']}  {r['name']}" for _, r in projects.iterrows()]
        proj = st.selectbox("Project (optional)", proj_opt, key="tsk_proj_new")
        assignees = st.multiselect("Assignees", staff_allowed_new["name"].tolist(), key="tsk_asg_new")
        if can_assign_tasks() and st.button("? Create Task", key="tsk_create"):
            pid = int(projects[projects['code'] == proj.split('  ')[0]]['id'].iloc[0]) if proj != "" else None
            tid = execute(
                "INSERT INTO tasks (title,description,date_assigned,days_allotted,due_date,project_id,created_by_staff_id) VALUES (?,?,?,?,?,?,?)",
                (title, desc or None, str(date_assigned), int(da), str(due), pid, current_staff_id()),
            )
            for nm in assignees:
                sid = int(staff[staff["name"] == nm]["id"].iloc[0])
                execute("INSERT INTO task_assignments (task_id,staff_id,status) VALUES (?,?,?)", (tid, sid, "In progress"))
            try:
                if "email" in staff.columns:
                    emails = staff[staff["name"].isin(assignees)]["email"].dropna().astype(str).tolist()
                else:
                    emails = []
                if emails:
                    send_push(emails, "WorkNest: New Task", f"You have been assigned: {title[:80]}")
            except Exception:
                pass
            st.success("Task created.")
            st.rerun()

    st.subheader("Assignments")
    df = fetch_df(
        """
        SELECT
            ta.id,
            t.title,
            s.name AS staff,
            COALESCE(p.code || '  ' || p.name, p.code, '') AS project,
            t.due_date,
            ta.status,
            ta.completed_date,
            t.date_assigned,
            t.days_allotted
        FROM task_assignments ta
        JOIN tasks t ON t.id=ta.task_id
        JOIN staff s ON s.id=ta.staff_id
        LEFT JOIN projects p ON p.id=t.project_id
        ORDER BY date(t.due_date) ASC, project, t.title, s.name
    """
    )
    if df.empty:
        st.info("No assignments yet.")
    else:
        df["overdue"] = df.apply(lambda r: (r["status"] != "Completed") and (date.today() > dtparser.parse(r["due_date"]).date()), axis=1)

        def score_row(r):
            if r["status"] != "Completed" or pd.isna(r["completed_date"]):
                return 0
            due_local = dtparser.parse(r["due_date"]).date()
            cd = dtparser.parse(r["completed_date"]).date()
            late = max((cd - due_local).days, 0)
            return max(0, 100 - 5 * late)

        df["score"] = df.apply(score_row, axis=1)
        st.dataframe(df[["project", "title", "staff", "due_date", "status", "completed_date", "days_allotted", "overdue", "score"]], width='stretch')

    st.divider()
    st.subheader("📊 Performance Scoreboard")
    st.caption("Global totals come from the points ledger. Report fairness is shown separately so staff with more projects do not gain an automatic advantage.")

    perf = fetch_df(
        """
        SELECT
            s.id,
            s.name,
            s.rank,
            s.section,
            COALESCE(SUM(CASE WHEN p.source = 'task' THEN p.points END), 0) AS task_points,
            COALESCE(SUM(CASE WHEN p.source IN ('biweekly','report') THEN p.points END), 0) AS report_points,
            COALESCE(SUM(CASE WHEN p.source = 'test' THEN p.points END), 0) AS test_points,
            COALESCE(SUM(p.points), 0) AS total_score,
            COALESCE(SUM(CASE WHEN COALESCE(NULLIF(p.awarded_at, '')::date, CURRENT_DATE) >= date(?) AND COALESCE(NULLIF(p.awarded_at, '')::date, CURRENT_DATE) <= date(?) THEN p.points END), 0) AS monthly_score
        FROM staff s
        LEFT JOIN points p ON p.staff_id = s.id
        GROUP BY s.id, s.name, s.rank, s.section
        ORDER BY total_score DESC, s.name ASC;
    """,
        (monthly_start.isoformat(), monthly_end.isoformat()),
    )

    fairness = _report_scoreboard(today=today)
    monthly_fairness = _report_scoreboard(today=today, month_start=monthly_start, month_end=monthly_end)
    if perf.empty:
        st.info("No points recorded yet.")
    else:
        perf = perf.merge(
            fairness[['staff_id', 'expected_reports', 'approved_reports', 'on_time_reports', 'late_reports', 'very_late_reports', 'missed_reports', 'report_score_pct']],
            how='left',
            left_on='id',
            right_on='staff_id',
        )
        perf['_report_score_sort'] = perf['report_score_pct'].fillna(-1.0)
        perf = perf.sort_values(['total_score', 'monthly_score', '_report_score_sort', 'name'], ascending=[False, False, False, True])
        perf['report_score_pct_display'] = perf['report_score_pct'].map(lambda v: '' if pd.isna(v) else f"{float(v):.1f}%")
        g1, g2, g3 = st.columns(3)
        winner = perf.iloc[0]
        g1.success(f"🏆 Global leader: **{winner['name']}** — {int(winner['total_score'])} pts")
        if not perf.empty:
            month_winner = perf.sort_values(['monthly_score', 'total_score', '_report_score_sort', 'name'], ascending=[False, False, False, True]).iloc[0]
            g2.info(f"🗓 Monthly leader: **{month_winner['name']}** — {int(month_winner['monthly_score'])} pts")
        fair_candidates = fairness[fairness['expected_reports'] > 0] if not fairness.empty else fairness
        if fair_candidates is not None and not fair_candidates.empty:
            fair_winner = fair_candidates.iloc[0]
            g3.info(f"⚖️ Best report score: **{fair_winner['name']}** — {float(fair_winner['report_score_pct']):.1f}%")
        elif comp.get('no_obligation'):
            g3.info("⚖️ Best report score: no live cycle due yet")
        st.dataframe(
            perf[
                [
                    'name',
                    'rank',
                    'section',
                    'task_points',
                    'report_points',
                    'test_points',
                    'monthly_score',
                    'total_score',
                    'expected_reports',
                    'approved_reports',
                    'on_time_reports',
                    'late_reports',
                    'very_late_reports',
                    'missed_reports',
                    'report_score_pct_display',
                ]
            ].rename(
                columns={
                    'name': 'Staff',
                    'rank': 'Rank',
                    'section': 'Section',
                    'task_points': 'Task pts',
                    'report_points': 'Report pts',
                    'test_points': 'Test pts',
                    'monthly_score': 'Monthly pts',
                    'total_score': 'Global pts',
                    'expected_reports': 'Expected reports',
                    'approved_reports': 'Approved reports',
                    'on_time_reports': 'On-time',
                    'late_reports': 'Late',
                    'very_late_reports': 'Very late',
                    'missed_reports': 'Missed',
                    'report_score_pct_display': 'Report score %',
                }
            ),
            width='stretch',
            hide_index=True,
        )

    st.divider()
    st.subheader("📈 Reporting Compliance")
    try:
        cdf = _report_scoreboard(today=today)
        if cdf.empty:
            st.info('No reporting compliance data yet.')
        else:
            cdf['report_score_pct_display'] = cdf['report_score_pct'].map(lambda v: '' if pd.isna(v) else f"{float(v):.1f}%")
            cdf['compliance_rate_%'] = ((cdf['approved_reports'] / cdf['expected_reports'].replace(0, np.nan)) * 100).round(1)
            cdf['compliance_rate_display'] = cdf['compliance_rate_%'].map(lambda v: '' if pd.isna(v) else f"{float(v):.1f}%")
            st.dataframe(
                cdf[['name', 'rank', 'section', 'expected_reports', 'approved_reports', 'on_time_reports', 'late_reports', 'very_late_reports', 'missed_reports', 'report_points', 'report_score_pct_display', 'compliance_rate_display']].rename(
                    columns={
                        'name': 'Staff',
                        'rank': 'Rank',
                        'section': 'Section',
                        'expected_reports': 'Expected',
                        'approved_reports': 'Approved',
                        'on_time_reports': 'On-time',
                        'late_reports': 'Late',
                        'very_late_reports': 'Very late',
                        'missed_reports': 'Missed',
                        'report_points': 'Report pts',
                        'report_score_pct_display': 'Report score %',
                        'compliance_rate_display': 'Compliance %',
                    }
                ),
                width='stretch',
                hide_index=True,
            )
    except Exception as e:
        st.info(f"Reporting compliance will appear once report cycles start running cleanly. ({e})")
