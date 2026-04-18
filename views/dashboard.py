from datetime import date, timedelta

import pandas as pd
import streamlit as st

from core.auth import current_staff_id, is_admin
from core.dashboard_service import (
    APP_TITLE,
    _dashboard_my_reports_df,
    _dashboard_personal_perf_snapshot,
    _dashboard_points_leaderboard_df,
    _dashboard_recent_reports_df,
    _dashboard_recent_tasks_df,
    _dashboard_staff_of_month_df,
    attachment_rows_bulk,
    chat_visibility_counts,
    compliance_snapshot,
    current_biweekly_cycle,
    dashboard_summary_snapshot,
    ensure_biweekly_pdf,
    historical_compliance_snapshot,
    latest_login_activity,
    my_current_cycle_compliance_snapshot,
    obligations_snapshot,
    project_open_biweekly_cycle,
    render_pdf_preview_and_download,
    report_scoreboard,
    send_sms,
    today_absentees_snapshot,
    unified_biweekly_snapshot,
)
from core.dates import _month_start, _monthly_points_window, safe_parse_date
from core.db import execute, fetch_df
from core.labels import _timing_status_label
from core.permissions import can_manage_projects

def page_dashboard():
    st.markdown(f"<div class='worknest-header'><h2>🏠 {APP_TITLE} — Dashboard</h2></div>", unsafe_allow_html=True)
    sid = current_staff_id()
    admin = is_admin()
    today = date.today()
    selected = None

    summary = dashboard_summary_snapshot(today.isoformat())
    snap = obligations_snapshot(sid, today) if sid is not None else {"tasks": [], "reports": [], "leave": [], "last_report": None}
    comp = compliance_snapshot(today)
    hist_comp = historical_compliance_snapshot(today) if admin else None
    my_comp = my_current_cycle_compliance_snapshot(int(sid), today.isoformat()) if sid is not None else None
    absentees = today_absentees_snapshot(today.isoformat())
    ubs = unified_biweekly_snapshot(today.isoformat())

    def _day_count_text(n):
        return f"{n} day{'s' if n != 1 else ''}"

    def _due_text(days_left, capitalize=False):
        if days_left is None:
            text = "No due date"
        elif days_left == 0:
            text = "Due today"
        elif days_left < 0:
            text = f"Overdue by {_day_count_text(abs(days_left))}"
        else:
            text = f"Due in {_day_count_text(days_left)}"
        return text if capitalize else text[:1].lower() + text[1:]

    st.caption("Branch snapshot:")
    ctop1, ctop2, ctop3, ctop4 = st.columns(4)
    ctop1.metric("All Projects", summary.get("projects", 0))
    ctop2.metric("Active Projects (Branch)", summary.get("active_projects", 0))
    ctop3.metric("Dormant Projects (Branch)", summary.get("dormant_projects", 0))
    ctop4.metric("Open Tasks (Branch)", summary.get("open_tasks", 0))

    if sid is not None:
        cc = chat_visibility_counts(sid)
        unread = int(cc.get("unread", 0) or 0)
        mentions = int(cc.get("mentions", 0) or 0)
        if unread or mentions:
            alert = f"📩 You have {unread} unread chat message(s)"
            if mentions:
                alert += f" and {mentions} mention(s)"
            st.info(alert)

    st.markdown("### 🎯 Today's Obligations")
    ob1, ob2 = st.columns([2,1])
    with ob1:
        if sid is None:
            st.info("Log in to view personal obligations.")
        else:
            current_cycle = current_biweekly_cycle(today)
            current_cycle_no = int(current_cycle["cycle_no"])
            reports = [
                rep for rep in (snap.get("reports") or [])
                if str(rep.get("cycle_no") or "").strip() and int(rep.get("cycle_no")) == current_cycle_no
            ]
            def urgency_key(rep):
                d = rep.get("days_left")

                if d is None:
                    return (4, 9999)

                if d < 0:
                    return (0, d)

                if d == 0:
                    return (1, d)

                if d <= 2:
                    return (2, d)

                return (3, d)

            reports = sorted(reports, key=urgency_key)
            if reports:
                r0 = reports[0]
                due_count = len(reports)
                dl = r0.get("days_left")
                report_label = "report due" if due_count == 1 else "reports due"
                due_text = _due_text(dl, capitalize=True)
                st.warning(f"{due_count} {report_label} — Report {r0['cycle_no']} • {due_text}")
                st.markdown(f"**You have {due_count} report{'s' if due_count != 1 else ''} due:**")
                for rep in reports:
                    rep_days_left = rep.get("days_left")
                    if rep_days_left is None:
                        rep_badge = ":gray-badge[Unknown]"
                        rep_due_text = "No due date"
                    elif rep_days_left > 2:
                        rep_badge = ":green-badge[On track]"
                        rep_due_text = _due_text(rep_days_left, capitalize=True)
                    elif rep_days_left > 0:
                        rep_badge = ":blue-badge[Due soon]"
                        rep_due_text = _due_text(rep_days_left, capitalize=True)
                    elif rep_days_left == 0:
                        rep_badge = ":blue-badge[Due today]"
                        rep_due_text = _due_text(rep_days_left, capitalize=True)
                    elif rep_days_left >= -7:
                        rep_badge = ":yellow-badge[Late]"
                        rep_due_text = _due_text(rep_days_left, capitalize=True)
                    else:
                        rep_badge = ":red-badge[Very late]"
                        rep_due_text = _due_text(rep_days_left, capitalize=True)
                    line = f"- **{rep['project']}** — {rep_badge} • {rep_due_text}"
                    if rep_days_left is not None and rep_days_left < 0:
                        st.markdown(f"**{line}**")
                    elif rep_days_left == 0:
                        st.markdown(f"⚠️ {line}")
                    else:
                        st.markdown(line)
            else:
                outstanding_count = int((my_comp or {}).get("outstanding", 0))
                expected_count = int((my_comp or {}).get("expected", 0))
                dl = (current_cycle["due_date"] - today).days if current_cycle.get("due_date") else None
                if dl is not None and dl >= 0:
                    due_text = _due_text(dl, capitalize=True)
                    if expected_count > 0 and outstanding_count == 0:
                        st.success(f"All reports submitted — Report {int(current_cycle['cycle_no'])} • {due_text}")
                    else:
                        st.warning(f"{max(1, outstanding_count)} report{'s' if max(1, outstanding_count) != 1 else ''} due — Report {int(current_cycle['cycle_no'])} • {due_text}")
                    st.caption(f"Current reporting window: {current_cycle['window_start'].isoformat()} — {current_cycle['window_end'].isoformat()} | deadline {current_cycle['due_date'].isoformat()}")
                else:
                    st.success("No reports due right now.")

            tasks = sorted(snap.get("tasks") or [], key=lambda x: (x.get("days_left") is None, x.get("days_left", 9999)))
            if tasks:
                t0 = tasks[0]
                dd = t0.get("days_left")
                due_text = _due_text(dd)
                st.info(f"Task pending: {t0['label']} — {due_text}")
            else:
                st.caption("No pending tasks assigned to you.")

            leave_items = sorted(snap.get("leave") or [], key=lambda x: (x.get("start_date") or date.max))
            if leave_items:
                l0 = leave_items[0]
                start_txt = l0['start_date'].isoformat() if l0.get('start_date') else 'n/a'
                who = l0.get('covering_name') or 'another officer'
                st.info(f"Relief duty ahead: covering {who} from {start_txt}.")
            else:
                st.caption("No upcoming relief assignments.")

            last_report = snap.get("last_report")
            if last_report:
                when = last_report.get("submitted_on")
                when_txt = when.isoformat() if when else "date unavailable"
                pfx = str(last_report.get("project_code") or "").strip()
                proj = f"{pfx}  {last_report.get('project_name') or ''}".strip(" ")
                cyc = last_report.get("cycle_no")
                cyc_txt = f"Report {int(cyc)}" if pd.notna(cyc) else "Latest report"
                st.success(f"{cyc_txt} submitted on {when_txt}" + (f" | {proj}" if proj else ""))
            else:
                st.caption("No reports submitted yet.")

            my_reports_df = _dashboard_my_reports_df(int(sid))
            st.markdown("#### My Recent Report Submissions")
            if my_reports_df.empty:
                st.caption("No reports submitted yet.")
            else:
                report_ids = tuple(int(x) for x in my_reports_df["id"].tolist())
                bulk_atts = attachment_rows_bulk('biweekly', report_ids)
                atts_by_parent = {}
                if not bulk_atts.empty:
                    for parent_id, grp in bulk_atts.groupby("parent_id", sort=False):
                        atts_by_parent[int(parent_id)] = grp.drop(columns=["parent_id"], errors="ignore").reset_index(drop=True)
                for _, mr in my_reports_df.iterrows():
                    submitted_txt = mr.get('submitted_on') or mr.get('uploaded_at') or ''
                    proj_label = f"{mr.get('code') or ''}  {mr.get('project_name') or ''}".strip(' ')
                    cyc_txt = f"Report {int(mr['cycle_no'])}" if pd.notna(mr.get('cycle_no')) else 'Report'
                    st.markdown(f"**{cyc_txt}** | {proj_label}  \n**Window:** {mr.get('window_start') or ''} — {mr.get('window_end') or ''}  \n**Submitted:** {submitted_txt} | **Status:** {mr.get('status') or ''} | **Timing:** {_timing_status_label(mr.get('timing_status'))}")
                    dash_atts = atts_by_parent.get(int(mr['id']), pd.DataFrame(columns=['id','file_path','caption','uploaded_at']))
                    pdf_path = ensure_biweekly_pdf(mr.to_dict(), project_name=proj_label, attachments_df=dash_atts)
                    if pdf_path:
                        render_pdf_preview_and_download(f"dash_my_bw_{int(mr['id'])}", pdf_path)
                    st.markdown('---')

    st.markdown("### 🚫 Today's Absentees")
    if not absentees:
        st.caption("All staff are available today.")
    else:
        for row in absentees:
            staff_name = row.get("staff_name") or "Unknown"
            leave_type = row.get("leave_type") or "Leave"
            end_date = row.get("end_date")
            end_txt = end_date.isoformat() if end_date else "unknown date"
            st.markdown(f"- 👤 {staff_name} — {leave_type} (returns {end_txt})")

    current_cycle = current_biweekly_cycle(today)
    monthly_start, monthly_end = _monthly_points_window(today)
    month_perf = report_scoreboard(today=today, month_start=monthly_start, month_end=monthly_end)
    global_perf = report_scoreboard(today=today)
    with ob2:
        if admin:
            st.markdown("**Current Cycle Compliance**")
            ratio = comp.get("ratio")
            st.progress(float(ratio) if ratio is not None else 0.0)
            st.metric("Current Cycle Compliance", f"{comp.get('compliant', 0)}/{comp.get('total_staff', 0)}")
            if comp.get("no_obligation"):
                st.caption(f"No current cycle project obligation yet | Current cycle: Report {int(current_cycle['cycle_no'])} | Next submission due {current_cycle['due_date'].isoformat()}")
            else:
                st.caption(f"Current cycle only. Valid submissions count immediately. Missing active-project reports: {comp.get('outstanding', 0)} | Current cycle: Report {int(current_cycle['cycle_no'])}")
        else:
            submitted_count = int((my_comp or {}).get("submitted", 0))
            expected_count = int((my_comp or {}).get("expected", 0))
            ratio = (submitted_count / expected_count) if expected_count else 0.0
            st.markdown("**My Compliance**")
            st.caption("Your submission progress:")
            st.progress(float(ratio))
            st.markdown(
                f"<span style='font-size:18px; font-weight:600;'>{submitted_count} / {expected_count} Reports Submitted</span>",
                unsafe_allow_html=True,
            )
            if expected_count == 0:
                st.caption(f"No reports due for this cycle. | Current cycle: Report {int(current_cycle['cycle_no'])}")
            else:
                st.caption(f"Based on your assigned projects for this cycle. Remaining reports: {max(0, expected_count - submitted_count)} | Current cycle: Report {int(current_cycle['cycle_no'])}")

    if admin:
        st.markdown("### 📊 Branch Situational Awareness")
        b1, b2, b3, b4 = st.columns(4)
        b1.metric("Current Cycle Compliance", "" if comp.get('no_obligation') else f"{int(round(((comp.get('ratio') or 0.0))*100))}%")
        b2.metric("Current Cycle Missing", comp.get("outstanding", 0))
        my_pending_tasks = len(snap.get("tasks") or []) if sid is not None else 0
        b3.metric("My pending tasks", my_pending_tasks)
        b4.metric("Current report no.", int(current_cycle['cycle_no']))

    if admin and hist_comp is not None:
        st.markdown("**Full Compliance (Admin Only)**")
        h1, h2 = st.columns(2)
        h1.metric("Full Compliance (Admin Only)", f"{hist_comp.get('compliant', 0)}/{hist_comp.get('total_staff', 0)}")
        h2.metric("Historical Outstanding", hist_comp.get("outstanding", 0))
        if hist_comp.get("no_obligation"):
            st.caption("No historical report obligation is currently due.")
        else:
            st.caption(f"Full backlog across all past cycles. Outstanding: {hist_comp.get('outstanding', 0)}")
        st.markdown("**Staff Performance Leaderboard**")
        leaderboard_df = _dashboard_points_leaderboard_df()
        if leaderboard_df.empty:
            st.caption("No points have been recorded yet.")
        else:
            def _leaderboard_row_style(row):
                rank = int(row.get("Rank") or 0)
                if rank == 1:
                    color = "#eaf7ea"
                elif rank == 2:
                    color = "#fff8db"
                elif rank == 3:
                    color = "#fdeaea"
                else:
                    color = ""
                return [f"background-color: {color}" if color else "" for _ in row]

            st.dataframe(
                leaderboard_df.style.apply(_leaderboard_row_style, axis=1),
                hide_index=True,
                width='stretch',
            )
        st.markdown("**🏆 Staff of the Month**")
        st.caption(f"{today.strftime('%B %Y')}")
        staff_month_df = _dashboard_staff_of_month_df(_month_start(today).isoformat())
        if staff_month_df.empty:
            st.caption("No performance data yet.")
        else:
            medals = ["🥇", "🥈", "🥉"]
            for idx, (_, row) in enumerate(staff_month_df.iterrows()):
                medal = medals[idx] if idx < len(medals) else f"{idx + 1}."
                points = int(row.get("monthly_points") or 0)
                line = f"{medal} {row.get('staff_name') or 'Unknown'} — {points} points"
                if idx == 0:
                    st.success(line)
                else:
                    st.write(line)

    if admin:
        with st.expander("📱 SMS test (Admin only)", expanded=False):
            st.caption("Use this once to confirm that WorkNest can send SMS before wiring it into live reminders.")
            sms_test_number = st.text_input("Test phone number", value="", placeholder="2348012345678", key="dashboard_sms_test_number")
            sms_test_message = st.text_area("Test message", value="WorkNest: Test SMS successful.", key="dashboard_sms_test_message")
            if st.button("Send Test SMS", key="dashboard_send_test_sms"):
                result = send_sms([sms_test_number], sms_test_message)
                if result.get("ok"):
                    st.success("SMS request sent. Check the phone and the Render logs for delivery details.")
                else:
                    st.error(f"SMS test failed: {result}")
                st.write(result)

    st.markdown("### 🕒 Recent Activity")
    a1, a2 = st.columns(2)
    with a1:
        recent_reports = _dashboard_recent_reports_df()
        if recent_reports.empty:
            st.caption("No recent report submissions yet.")
        else:
            for _, rr in recent_reports.iterrows():
                cyc = rr.get("cycle_no")
                cyc_txt = f"Report No. {int(cyc)}" if pd.notna(cyc) else "Biweekly report"
                proj = "  ".join([x for x in [str(rr.get("project_code") or "").strip(), str(rr.get("project_name") or "").strip()] if x])
                st.write(f" {rr['officer']} submitted {cyc_txt}" + (f" for {proj}" if proj else ""))
                st.caption(str(rr.get("stamp") or ""))
    with a2:
        recent_tasks = _dashboard_recent_tasks_df()
        if recent_tasks.empty:
            st.caption("No recently completed tasks yet.")
        else:
            for _, tr in recent_tasks.iterrows():
                label = str(tr.get('title') or 'Task')
                pcode = str(tr.get('project_code') or '').strip()
                if pcode:
                    label = f"{pcode}  {label}"
                st.write(f" {tr['staff_name']} completed {label}")
                st.caption(str(tr.get('completed_date') or ''))

    st.markdown("### 👤 My Performance")
    if sid is not None:
        perf_snap = _dashboard_personal_perf_snapshot(int(sid))
        pts = perf_snap["points"]
        rp = perf_snap["reports"]
        tp = perf_snap["tasks"]
        p1, p2, p3 = st.columns(3)
        p1.metric("Biweekly reports", int(rp["n"].iloc[0]) if not rp.empty else 0)
        p2.metric("Tasks completed", int(tp["n"].iloc[0]) if not tp.empty else 0)
        p3.metric("Total points", int(pts["total_points"].iloc[0]) if not pts.empty else 0)
    else:
        st.caption("No personal performance record available.")

    if admin:
        st.markdown("### 🎯 Precision Follow-up")
        out_df = hist_comp.get("rows") if isinstance(hist_comp, dict) else None
        if isinstance(out_df, pd.DataFrame) and not out_df.empty:
            st.markdown("**Staff with Historical Outstanding Reports**")
            st.dataframe(out_df, hide_index=True, width='stretch')
        else:
            st.success("No staff currently sitting on a historical biweekly backlog.")

        st.markdown("**Login Activity**")
        latest = latest_login_activity()
        staff_users = fetch_df(
            """
            SELECT u.id AS user_id, s.id AS staff_id, s.name, COALESCE(u.username, s.email, s.name) AS username
            FROM users u
            LEFT JOIN staff s ON s.id=u.staff_id
            WHERE COALESCE(u.is_active,1)=1
            ORDER BY s.name, u.username
            """
        )
        if staff_users.empty:
            st.caption("No login accounts found.")
        else:
            merged = staff_users.merge(latest, on="user_id", how="left", suffixes=("", "_last"))
            merged["last_login"] = merged["login_at"].fillna("Never")
            st.dataframe(
                merged[["name", "username", "last_login", "login_method"]].rename(columns={
                    "name": "Staff",
                    "username": "Login",
                    "last_login": "Last login",
                    "login_method": "Method",
                }),
                hide_index=True,
                width='stretch'
            )
            never = merged[merged["login_at"].isna()]
            if not never.empty:
                st.warning("No login recorded yet: " + ", ".join([str(x) for x in never["name"].fillna(never["username"]).tolist()]))

    def project_next_due(pid, start_date, next_due_date=None):
        cyc, reason = project_open_biweekly_cycle(int(pid), date.today())
        if cyc is None:
            return (False, None, None, reason)
        overdue = date.today() > cyc["due_date"]
        return (overdue, cyc["window_end"], cyc["due_date"], reason)

    st.markdown("### ✅ Action Items (Due / Overdue)")
    items=[]
    horizon=today+timedelta(days=7)

    def due_status_label(days_left):
        if days_left is None:
            return "Unknown"
        if days_left >= 3:
            return "On track"
        if days_left in (1, 2):
            return "Due soon"
        if days_left == 0:
            return "Due today"
        if days_left >= -7:
            return "Late"
        return "Very late"

    if admin:
        proj_due=fetch_df("SELECT id,code,name,start_date,next_due_date,COALESCE(status,'ACTIVE') AS status FROM projects WHERE COALESCE(status,'ACTIVE')!='DORMANT' ORDER BY code")
    else:
        proj_due=fetch_df("SELECT P.id,P.code,P.name,P.start_date,P.next_due_date,COALESCE(P.status,'ACTIVE') AS status FROM projects P JOIN project_staff PS ON PS.project_id=P.id WHERE PS.staff_id=? AND COALESCE(P.status,'ACTIVE')!='DORMANT' ORDER BY P.code", (sid,))

    for _,p in proj_due.iterrows():
        pid=int(p["id"])
        overdue,last_d,exp,reason = project_next_due(pid, p.get("start_date"), p.get("next_due_date"))
        if exp is None: continue
        days_left = (exp - today).days
        if overdue or (0 <= days_left <= 7):
            status = due_status_label(days_left)
            items.append({
                "type":"Report due",
                "item":f"{p.get('code','')} — {p.get('name','')}",
                "due":exp.isoformat(),
                "status":status,
                "details":reason
            })

    if sid is not None:
        tdf=fetch_df("""
            SELECT T.id, T.title, T.due_date, TA.status, P.code AS project_code, P.name AS project_name
            FROM task_assignments TA
            JOIN tasks T ON T.id = TA.task_id
            LEFT JOIN projects P ON P.id = T.project_id
            WHERE TA.staff_id=? AND COALESCE(TA.status,'')!='Completed'
        """, (sid,))
        for _,t in tdf.iterrows():
            due=safe_parse_date(t.get("due_date"))
            if due is None: continue
            if due <= horizon:
                days_left = (due - today).days
                status = due_status_label(days_left)
                pfx = (t.get("project_code") or "").strip()
                items.append({
                    "type":"Task",
                    "item":f"{pfx}  {t.get('title','')}" if pfx else str(t.get('title') or ""),
                    "due":due.isoformat(),
                    "status":status,
                    "details":f"Task ID {t.get('id')}"
                })

    if items:
        df_items=pd.DataFrame(items)
        df_items["__due_sort"]=pd.to_datetime(df_items["due"], errors="coerce")
        df_items["__status_sort"]=df_items["status"].map(lambda s: 0 if s=="Overdue" else 1)
        df_items=df_items.sort_values(["__status_sort","__due_sort","type","item"]).drop(columns=["__due_sort","__status_sort"])
        st.dataframe(df_items, hide_index=True, width='stretch')
    else:
        st.success("No due/overdue items in the next 7 days.")


def _queue_rows(for_staff_id=None):
    base = ubs.get("branch_queue")
    if not isinstance(base, pd.DataFrame):
        return pd.DataFrame(columns=['project','report_no','window','due','status'])
    if for_staff_id is None:
        return base
    try:
        my_projects = fetch_df("""
            SELECT p.code, p.name
            FROM projects p
            JOIN project_staff ps ON ps.project_id=p.id
            WHERE ps.staff_id=? AND COALESCE(p.status,'ACTIVE')!='DORMANT'
        """, (int(for_staff_id),))
        allowed = set((f"{str(r.get('code') or '').strip()}  {str(r.get('name') or '').strip()}").strip(" ") for _, r in my_projects.iterrows())
        return base[base["project"].isin(allowed)].reset_index(drop=True)
    except Exception:
        return pd.DataFrame(columns=['project','report_no','window','due','status'])

    st.markdown("### 🗂️ Report Queue")
    if sid is not None:
        myq = _queue_rows(sid)
        st.markdown("**My Pending Reports**")
        if myq.empty:
            st.caption("No pending report obligation for you right now.")
        else:
            st.dataframe(myq, hide_index=True, width='stretch')
    if admin:
        st.markdown("**Branch Report Queue**")
        bq = _queue_rows(None)
        if bq.empty:
            st.success("No open branch report obligations right now.")
        else:
            st.dataframe(bq, hide_index=True, width='stretch')

    st.markdown("### 📊 Reporting Compliance")
    cdf = ubs.get("staff_compliance")
    if isinstance(cdf, pd.DataFrame) and not cdf.empty:
        st.dataframe(cdf, hide_index=True, width='stretch')

# --- Project Quick Edit (Admin only) ---

    if can_manage_projects():
        st.markdown("### Project Quick Edit")
        pdf = fetch_df("SELECT id,code,name,client,location,start_date,end_date,supervisor_staff_id FROM projects ORDER BY code")
        staff = fetch_df("SELECT id,name FROM staff ORDER BY name")
        if not pdf.empty:
            options = ["— Select project —"] + [f"{r['code']} — {r['name']}" for _, r in pdf.iterrows()]
            pick = st.selectbox("Project", options, key="dash_proj_pick")
            if pick != "— Select project —":
                sel_code = pick.split(" — ")[0].strip()
                row = pdf[pdf["code"] == sel_code]
                if not row.empty:
                    selected = row.iloc[0].to_dict()
        else:
            st.info("No projects found yet. Import projects via Import CSVs (admin).")

        # Basic edit form (admin only)
        sup_name_by_id = {int(r["id"]): r["name"] for _, r in staff.iterrows() if str(r.get("id", "")).isdigit() and r.get("name")}
        sup_id_by_name = {r["name"]: int(r["id"]) for _, r in staff.iterrows() if str(r.get("id", "")).isdigit() and r.get("name")}
        sup_options = [""] + sorted([n for n in staff["name"].dropna().tolist() if str(n).strip()]) if not staff.empty and "name" in staff.columns else [""]

        default_sup = ""
        if selected is not None and selected.get("supervisor_staff_id"):
            try:
                default_sup = sup_name_by_id.get(int(selected["supervisor_staff_id"]), "") or ""
            except Exception:
                default_sup = ""

        with st.expander("Create / Edit Project", expanded=False):
            code = st.text_input("Code", value=(selected["code"] if selected is not None else ""), key="proj_code")
            name = st.text_input("Name", value=(selected["name"] if selected is not None else ""), key="proj_name")
            client = st.text_input("Client", value=(selected.get("client") if selected is not None else "") or "", key="proj_client")
            location = st.text_input("Location", value=(selected.get("location") if selected is not None else "") or "", key="proj_location")
            start_date = st.text_input("Start date (YYYY-MM-DD)", value=(selected.get("start_date") if selected is not None else "") or "", key="proj_start")
            end_date = st.text_input("End date (YYYY-MM-DD)", value=(selected.get("end_date") if selected is not None else "") or "", key="proj_end")

            sup_idx = 0
            if default_sup and default_sup in sup_options:
                sup_idx = sup_options.index(default_sup)
            supervisor_name = st.selectbox("Supervisor", sup_options, index=sup_idx, key="proj_supervisor")
            supervisor_id = sup_id_by_name.get(supervisor_name) if supervisor_name else None

            colA, colB = st.columns([1, 1])
            with colA:
                if st.button("Save project", use_container_width=True):
                    if not code.strip() or not name.strip():
                        st.error("Project Code and Name are required.")
                    else:
                        if selected is None:
                            execute(
                                "INSERT INTO projects(code,name,client,location,supervisor_staff_id,start_date,end_date) VALUES (?,?,?,?,?,?,?)",
                                (code.strip(), name.strip(), client.strip() or None, location.strip() or None, supervisor_id, start_date.strip() or None, end_date.strip() or None),
                            )
                            st.success("Project created.")
                            st.rerun()
                        else:
                            execute(
                                "UPDATE projects SET code=?,name=?,client=?,location=?,supervisor_staff_id=?,start_date=?,end_date=? WHERE id=?",
                                (code.strip(), name.strip(), client.strip() or None, location.strip() or None, supervisor_id, start_date.strip() or None, end_date.strip() or None, int(selected["id"])),
                            )
                            st.success("Project updated.")
                            st.rerun()
            with colB:
                if selected is not None:
                    if st.button("Delete project", use_container_width=True):
                        execute("DELETE FROM projects WHERE id=?", (int(selected["id"]),))
                        st.warning("Project deleted.")
                        st.rerun()
    if selected is not None:
        st.caption("Detailed project management is available on the Projects page.")

# ---------- Staff ----------


