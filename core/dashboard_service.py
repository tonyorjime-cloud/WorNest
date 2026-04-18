import datetime as dt
from datetime import date, timedelta

import numpy as np
import pandas as pd
import streamlit as st
from dateutil.relativedelta import relativedelta

from core.dates import _to_date_or_none, _today, _utcnow_iso
from core.db import DB_IS_POSTGRES, fetch_df
from core.projects_service import _attachment_rows_bulk as attachment_rows_bulk, _biweekly_backfill_cutoff_cycle, _biweekly_cycle_from_index, _current_biweekly_cycle as current_biweekly_cycle, _ensure_biweekly_pdf as ensure_biweekly_pdf, _project_current_due_cycle, _project_first_cycle_index, _project_is_dormant, _project_nonrejected_cycle_map, _project_open_biweekly_cycle as project_open_biweekly_cycle, render_pdf_preview_and_download, send_sms
from core.labels import _timing_status_points



APP_TITLE = "WorkNest Mini v3.2.4"


def _get_chat_last_seen(staff_id: int | None):
    if staff_id is None:
        return None
    df = fetch_df("SELECT last_seen_at FROM chat_reads WHERE staff_id=? LIMIT 1", (int(staff_id),))
    if df.empty:
        return None
    return str(df.iloc[0].get("last_seen_at") or "") or None


def chat_visibility_counts(staff_id:int|None):
    if staff_id is None:
        return {"unread":0, "mentions":0}
    last_seen=_get_chat_last_seen(staff_id)
    params=[]
    where=[]
    if last_seen:
        where.append("C.created_at > ?")
        params.append(last_seen)
    where_sql=("WHERE " + " AND ".join(where)) if where else ""
    unread_df=fetch_df(f"SELECT COUNT(1) AS n FROM chat_messages C {where_sql}", tuple(params))
    unread=int(unread_df.iloc[0]['n']) if not unread_df.empty else 0
    mention_params=[int(staff_id)] + params
    mention_where=["M.mentioned_staff_id = ?"] + where
    mention_sql="WHERE " + " AND ".join(mention_where)
    mention_df=fetch_df(f"SELECT COUNT(DISTINCT M.message_id) AS n FROM chat_mentions M JOIN chat_messages C ON C.id=M.message_id {mention_sql}", tuple(mention_params))
    mentions=int(mention_df.iloc[0]['n']) if not mention_df.empty else 0
    return {"unread": unread, "mentions": mentions}


def _expected_report_rows(today: dt.date | None = None, month_start: dt.date | None = None, month_end: dt.date | None = None) -> pd.DataFrame:
    today = today or _today()
    active_projects = fetch_df("""
        SELECT id, code, name, start_date
        FROM projects
        WHERE COALESCE(status,'ACTIVE')!='DORMANT'
        ORDER BY code, name
    """)
    rows = []
    cutoff = _biweekly_backfill_cutoff_cycle()
    if active_projects.empty:
        return pd.DataFrame(columns=["project_id","project_code","project_name","cycle_no","due_date","staff_id"])
    posted = fetch_df("SELECT project_id, staff_id FROM project_staff")
    posted_map = {}
    if not posted.empty:
        for _, rr in posted.iterrows():
            try:
                posted_map.setdefault(int(rr['project_id']), []).append(int(rr['staff_id']))
            except Exception:
                continue
    for _, pr in active_projects.iterrows():
        pid = int(pr['id'])
        staff_ids = posted_map.get(pid, [])
        if not staff_ids:
            continue
        first_idx = _project_first_cycle_index(pid)
        for idx in range(first_idx, 500):
            cyc = _biweekly_cycle_from_index(idx)
            if cyc['cycle_no'] < cutoff:
                continue
            due = cyc['due_date']
            if due > today:
                break
            if month_start and due < month_start:
                continue
            if month_end and due > month_end:
                continue
            for sid in staff_ids:
                rows.append({
                    'project_id': pid,
                    'project_code': str(pr.get('code') or '').strip(),
                    'project_name': str(pr.get('name') or '').strip(),
                    'cycle_no': int(cyc['cycle_no']),
                    'due_date': due,
                    'staff_id': int(sid),
                })
    return pd.DataFrame(rows)

def _report_scoreboard(today: dt.date | None = None, month_start: dt.date | None = None, month_end: dt.date | None = None) -> pd.DataFrame:
    today = today or _today()
    staff_df = fetch_df("SELECT id, name, rank, section FROM staff ORDER BY name")
    if staff_df.empty:
        return pd.DataFrame(columns=['staff_id','name','rank','section','expected_reports','approved_reports','on_time_reports','late_reports','very_late_reports','missed_reports','report_points','report_score_pct'])

    expected_df = _expected_report_rows(today=today, month_start=month_start, month_end=month_end)
    approved_df = fetch_df(
        """SELECT project_id, cycle_no, COALESCE(timing_status,'ON_TIME') AS timing_status, COALESCE(status,'PENDING') AS status
           FROM biweekly_reports
           WHERE COALESCE(status,'PENDING')='APPROVED'"""
    )
    approved_map = {}
    if not approved_df.empty:
        for _, rr in approved_df.iterrows():
            try:
                approved_map[(int(rr['project_id']), int(rr['cycle_no']))] = str(rr.get('timing_status') or 'ON_TIME').upper()
            except Exception:
                continue

    rows = []
    for _, sr in staff_df.iterrows():
        sid = int(sr['id'])
        sdf = expected_df[expected_df['staff_id'] == sid] if not expected_df.empty else pd.DataFrame()
        expected = int(len(sdf))
        on_time = late = very_late = missed = approved = report_points = 0
        if not sdf.empty:
            for _, er in sdf.iterrows():
                timing = approved_map.get((int(er['project_id']), int(er['cycle_no'])))
                if timing is None:
                    missed += 1
                    continue
                approved += 1
                if timing == 'VERY_LATE':
                    very_late += 1
                elif timing == 'LATE':
                    late += 1
                else:
                    on_time += 1
                report_points += _timing_status_points(timing)
        score_pct = round((report_points / (expected * 3) * 100.0), 1) if expected else np.nan
        rows.append({
            'staff_id': sid,
            'name': sr.get('name') or '',
            'rank': sr.get('rank') or '',
            'section': sr.get('section') or '',
            'expected_reports': expected,
            'approved_reports': approved,
            'on_time_reports': on_time,
            'late_reports': late,
            'very_late_reports': very_late,
            'missed_reports': missed,
            'report_points': report_points,
            'report_score_pct': score_pct,
        })
    df = pd.DataFrame(rows)
    df['_score_sort'] = df['report_score_pct'].fillna(-1.0)
    return df.sort_values(['_score_sort','report_points','name'], ascending=[False,False,True]).drop(columns=['_score_sort'])

def report_scoreboard(today=None, month_start=None, month_end=None):
    return _report_scoreboard(today=today, month_start=month_start, month_end=month_end)


def latest_login_activity() -> pd.DataFrame:
    cols = ["user_id", "login_at", "login_method"]
    try:
        df = fetch_df(
            "SELECT user_id, login_at, login_method FROM login_activity WHERE user_id IS NOT NULL ORDER BY login_at DESC, id DESC"
        )
        if df.empty:
            return pd.DataFrame(columns=cols)
        df = df.dropna(subset=["user_id"]).copy()
        try:
            df["user_id"] = df["user_id"].astype(int)
        except Exception:
            pass
        latest = df.drop_duplicates(subset=["user_id"], keep="first")
        return latest[cols]
    except Exception:
        return pd.DataFrame(columns=cols)


@st.cache_data(ttl=45, show_spinner=False)
def obligations_snapshot(staff_id, today=None):
    """Return lightweight dashboard obligations for a staff member."""
    today = today or date.today()
    today = _to_date_or_none(today) or date.today()
    snap = {"tasks": [], "reports": [], "leave": [], "last_report": None}
    if staff_id is None:
        return snap
    try:
        sid = int(staff_id)
    except Exception:
        return snap

    try:
        tdf = fetch_df(
            """
            SELECT ta.id AS assignment_id, t.id AS task_id, t.title, t.due_date,
                   p.code AS project_code, p.name AS project_name
            FROM task_assignments ta
            JOIN tasks t ON t.id=ta.task_id
            LEFT JOIN projects p ON p.id=t.project_id
            WHERE ta.staff_id=? AND COALESCE(ta.status,'In progress')!='Completed'
            ORDER BY CASE WHEN t.due_date IS NULL OR t.due_date='' THEN 1 ELSE 0 END,
                     date(t.due_date) ASC, ta.id DESC
            LIMIT 10
            """,
            (sid,),
        )
        for _, row in tdf.iterrows():
            due = _to_date_or_none(row.get("due_date"))
            proj_bits = [str(row.get("project_code") or "").strip(), str(row.get("project_name") or "").strip()]
            proj = "  ".join([x for x in proj_bits if x])
            label = str(row.get("title") or "Task")
            if proj:
                label = f"{proj}  {label}"
            snap["tasks"].append(
                {
                    "assignment_id": row.get("assignment_id"),
                    "task_id": row.get("task_id"),
                    "label": label,
                    "due_date": due,
                    "days_left": ((due - today).days if due else None),
                }
            )
    except Exception:
        pass

    try:
        current_cycle = current_biweekly_cycle(today)
        current_cycle_no = int(current_cycle["cycle_no"])
        projects_df = fetch_df(
            """
            SELECT DISTINCT p.id, p.code, p.name
            FROM project_staff ps
            JOIN projects p ON p.id=ps.project_id
            WHERE ps.staff_id=?
            ORDER BY p.code, p.name
            """,
            (sid,),
        )
        seen_project_ids = set()
        for _, row in projects_df.iterrows():
            pid = row.get("id")
            if pd.isna(pid):
                continue
            try:
                pid = int(pid)
            except Exception:
                continue
            if pid in seen_project_ids or _project_is_dormant(pid):
                continue
            seen_project_ids.add(pid)
            first_cycle = _biweekly_cycle_from_index(_project_first_cycle_index(pid))
            try:
                first_cycle_no = int(first_cycle["cycle_no"])
            except Exception:
                first_cycle_no = current_cycle_no
            if current_cycle_no < first_cycle_no:
                continue
            existing = _project_nonrejected_cycle_map(pid)
            if current_cycle_no in existing:
                continue
            snap["reports"].append(
                {
                    "project_id": pid,
                    "project": " — ".join([x for x in [str(row.get("code") or "").strip(), str(row.get("name") or "").strip()] if x]),
                    "project_code": str(row.get("code") or "").strip(),
                    "project_name": str(row.get("name") or "").strip(),
                    "cycle_no": current_cycle_no,
                    "window_start": current_cycle.get("window_start"),
                    "window_end": current_cycle.get("window_end"),
                    "due_date": current_cycle.get("due_date"),
                    "days_left": ((current_cycle.get("due_date") - today).days if current_cycle.get("due_date") else None),
                    "reason": "Current cycle outstanding.",
                }
            )
    except Exception:
        pass

    try:
        ldf = fetch_df(
            """
            SELECT l.start_date, l.end_date, s.name AS covering_name
            FROM leaves l
            LEFT JOIN staff s ON s.id=l.staff_id
            WHERE l.relieving_staff_id=?
              AND date(l.end_date) >= date(?)
              AND COALESCE(l.status,'Pending') IN ('Pending','Approved')
            ORDER BY date(l.start_date) ASC
            LIMIT 5
            """,
            (sid, today.isoformat()),
        )
        for _, row in ldf.iterrows():
            snap["leave"].append(
                {
                    "start_date": _to_date_or_none(row.get("start_date")),
                    "end_date": _to_date_or_none(row.get("end_date")),
                    "covering_name": row.get("covering_name"),
                }
            )
    except Exception:
        pass

    try:
        rdf = fetch_df(
            """
            SELECT r.cycle_no, COALESCE(r.submitted_on, r.uploaded_at, r.report_date) AS submitted_on,
                   p.code AS project_code, p.name AS project_name
            FROM biweekly_reports r
            LEFT JOIN projects p ON p.id=r.project_id
            WHERE r.uploader_staff_id=?
              AND COALESCE(r.status,'PENDING') IN ('PENDING','SUBMITTED','NEEDS_REVISION','APPROVED')
            ORDER BY COALESCE(r.submitted_on, r.uploaded_at, r.report_date) DESC, r.id DESC
            LIMIT 1
            """,
            (sid,),
        )
        if not rdf.empty:
            rr = rdf.iloc[0].to_dict()
            rr["submitted_on"] = _to_date_or_none(rr.get("submitted_on"))
            snap["last_report"] = rr
    except Exception:
        pass

    return snap


@st.cache_data(ttl=60, show_spinner=False)
def today_absentees_snapshot(today_iso: str):
    today_dt = _to_date_or_none(today_iso) or date.today()
    rows = []
    try:
        adf = fetch_df(
            """
            SELECT s.name AS staff_name, l.leave_type, l.end_date
            FROM leaves l
            JOIN staff s ON s.id=l.staff_id
            WHERE UPPER(COALESCE(l.status,'')) IN ('APPROVED','RECORDED')
              AND date(?) >= date(l.start_date)
              AND date(?) <= date(l.end_date)
            ORDER BY date(l.end_date) ASC, s.name ASC
            """,
            (today_dt.isoformat(), today_dt.isoformat()),
        )
        for _, row in adf.iterrows():
            rows.append(
                {
                    "staff_name": str(row.get("staff_name") or "").strip(),
                    "leave_type": str(row.get("leave_type") or "").strip(),
                    "end_date": _to_date_or_none(row.get("end_date")),
                }
            )
    except Exception:
        return []
    return rows


@st.cache_data(ttl=45, show_spinner=False)
def _submitted_biweekly_reports_df():
    try:
        return fetch_df(
            """
            SELECT project_id, cycle_no, status, timing_status, report_date, submitted_on, uploaded_at, uploader_staff_id
            FROM (
                SELECT br.project_id, br.cycle_no,
                       COALESCE(br.status,'PENDING') AS status,
                       COALESCE(br.timing_status,'ON_TIME') AS timing_status,
                       br.report_date, br.submitted_on, br.uploaded_at, br.uploader_staff_id,
                       ROW_NUMBER() OVER (
                           PARTITION BY br.project_id, br.cycle_no
                           ORDER BY COALESCE(br.updated_at, br.submitted_on, br.uploaded_at, br.report_date) DESC, br.id DESC
                       ) AS rn
                FROM biweekly_reports br
                WHERE COALESCE(br.status,'PENDING') IN ('SUBMITTED','APPROVED','NEEDS_REVISION','PENDING')
                  AND br.cycle_no IS NOT NULL
            ) t
            WHERE rn = 1
            """
        )
    except Exception:
        return fetch_df(
            """
            SELECT b1.project_id, b1.cycle_no,
                   COALESCE(b1.status,'PENDING') AS status,
                   COALESCE(b1.timing_status,'ON_TIME') AS timing_status,
                   b1.report_date, b1.submitted_on, b1.uploaded_at, b1.uploader_staff_id
            FROM biweekly_reports b1
            JOIN (
                SELECT project_id, cycle_no, MAX(id) AS max_id
                FROM biweekly_reports
                WHERE COALESCE(status,'PENDING') IN ('SUBMITTED','APPROVED','NEEDS_REVISION','PENDING')
                  AND cycle_no IS NOT NULL
                GROUP BY project_id, cycle_no
            ) latest
              ON latest.project_id=b1.project_id AND latest.cycle_no=b1.cycle_no AND latest.max_id=b1.id
            """
        )


@st.cache_data(ttl=30, show_spinner=False)
def _dashboard_my_reports_df(sid: int):
    report_order_expr = (
        "COALESCE(br.submitted_on::timestamp, br.uploaded_at::timestamp, br.report_date::timestamp)"
        if DB_IS_POSTGRES
        else "datetime(COALESCE(br.submitted_on, br.uploaded_at, br.report_date))"
    )
    return fetch_df(
        f"""
        SELECT p.code, p.name AS project_name, br.id, br.cycle_no, br.window_start, br.window_end, br.submitted_on, br.uploaded_at,
               COALESCE(br.status,'PENDING') AS status, br.timing_status, br.report_pdf_path
        FROM biweekly_reports br
        JOIN projects p ON p.id = br.project_id
        WHERE br.uploader_staff_id=?
          AND COALESCE(br.status,'PENDING') IN ('PENDING','SUBMITTED','NEEDS_REVISION','APPROVED')
        ORDER BY {report_order_expr} DESC, br.id DESC
        LIMIT 12
    """,
        (int(sid),),
    )


@st.cache_data(ttl=30, show_spinner=False)
def _dashboard_recent_reports_df():
    return fetch_df(
        """
        SELECT COALESCE(s.name, u.username, 'Unknown') AS officer, p.code AS project_code, p.name AS project_name,
               r.cycle_no, COALESCE(r.submitted_on, r.uploaded_at, r.report_date) AS stamp
        FROM biweekly_reports r
        LEFT JOIN staff s ON s.id=r.uploader_staff_id
        LEFT JOIN users u ON u.staff_id=r.uploader_staff_id
        LEFT JOIN projects p ON p.id=r.project_id
        WHERE COALESCE(r.status,'PENDING') IN ('APPROVED','PENDING')
        ORDER BY COALESCE(r.submitted_on, r.uploaded_at, r.report_date) DESC, r.id DESC
        LIMIT 6
        """
    )


@st.cache_data(ttl=30, show_spinner=False)
def _dashboard_recent_tasks_df():
    return fetch_df(
        """
        SELECT s.name AS staff_name, COALESCE(t.title, t.description, 'Task') AS title, ta.completed_date, p.code AS project_code
        FROM task_assignments ta
        JOIN tasks t ON t.id=ta.task_id
        JOIN staff s ON s.id=ta.staff_id
        LEFT JOIN projects p ON p.id=t.project_id
        WHERE ta.status='Completed' AND ta.completed_date IS NOT NULL
        ORDER BY ta.completed_date DESC, ta.id DESC
        LIMIT 6
        """
    )


@st.cache_data(ttl=30, show_spinner=False)
def _dashboard_staff_of_month_df(month_start_iso: str):
    month_start = str(month_start_iso)
    month_end = (date.fromisoformat(month_start_iso) + relativedelta(months=1)).isoformat()
    awarded_date_filter = (
        "p.awarded_at IS NOT NULL "
        "AND p.awarded_at::date >= date(?) "
        "AND p.awarded_at::date < date(?)"
        if DB_IS_POSTGRES
        else "p.awarded_at IS NOT NULL "
        "AND date(p.awarded_at) >= date(?) "
        "AND date(p.awarded_at) < date(?)"
    )
    return fetch_df(
        f"""
        SELECT
            s.id AS staff_id,
            s.name AS staff_name,
            COALESCE(SUM(p.points), 0) AS monthly_points,
            MAX(p.awarded_at) AS last_awarded_at
        FROM staff s
        JOIN points p
          ON p.staff_id = s.id
         AND {awarded_date_filter}
        GROUP BY s.id, s.name
        ORDER BY monthly_points DESC, last_awarded_at DESC, s.name ASC
        LIMIT 3
        """,
        (month_start, month_end),
    )


@st.cache_data(ttl=30, show_spinner=False)
def _dashboard_personal_perf_snapshot(sid: int):
    pts = fetch_df("SELECT COALESCE(SUM(points),0) AS total_points, COUNT(*) AS entries FROM points WHERE staff_id=?", (int(sid),))
    rp = fetch_df(
        "SELECT COUNT(*) AS n FROM biweekly_reports WHERE uploader_staff_id=? AND COALESCE(status,'PENDING') IN ('APPROVED','PENDING')",
        (int(sid),),
    )
    tp = fetch_df(
        """
        SELECT COUNT(*) AS n
        FROM task_assignments ta
        WHERE ta.staff_id=? AND ta.status='Completed'
        """,
        (int(sid),),
    )
    return {"points": pts, "reports": rp, "tasks": tp}


@st.cache_data(ttl=30, show_spinner=False)
def _dashboard_points_leaderboard_df():
    df = fetch_df(
        """
        SELECT
            s.id AS staff_id,
            s.name AS staff_name,
            COALESCE(p.total_points, 0) AS performance_points,
            COALESCE(br.approved_reports, 0) AS approved_reports
        FROM staff s
        LEFT JOIN (
            SELECT staff_id, SUM(points) AS total_points
            FROM points
            GROUP BY staff_id
        ) p ON p.staff_id = s.id
        LEFT JOIN (
            SELECT uploader_staff_id AS staff_id, COUNT(CASE WHEN COALESCE(status,'PENDING')='APPROVED' THEN 1 END) AS approved_reports
            FROM biweekly_reports
            GROUP BY uploader_staff_id
        ) br ON br.staff_id = s.id
        ORDER BY performance_points DESC, approved_reports DESC, s.name ASC
        """
    )
    if df.empty:
        return pd.DataFrame(columns=["Rank", "Staff Name", "Approved Reports", "Performance Points"])
    out = df[["staff_name", "approved_reports", "performance_points"]].copy()
    out.insert(0, "Rank", range(1, len(out) + 1))
    out = out.rename(columns={"staff_name": "Staff Name", "approved_reports": "Approved Reports", "performance_points": "Performance Points"})
    return out


@st.cache_data(ttl=45, show_spinner=False)
def unified_biweekly_snapshot(today_iso: str):
    today = date.fromisoformat(today_iso)
    cutoff = _biweekly_backfill_cutoff_cycle()
    active_projects = fetch_df(
        """
        SELECT id, code, name
        FROM projects
        WHERE COALESCE(status,'ACTIVE')!='DORMANT'
        ORDER BY code
        """
    )
    valid = _submitted_biweekly_reports_df()
    valid_map = {}
    if not valid.empty:
        for _, rr in valid.iterrows():
            try:
                valid_map[(int(rr["project_id"]), int(rr["cycle_no"]))] = {
                    "timing_status": str(rr.get("timing_status") or "ON_TIME").upper(),
                    "status": str(rr.get("status") or "PENDING").upper(),
                }
            except Exception:
                continue

    expected_rows = []
    branch_queue = []
    for _, pr in active_projects.iterrows():
        pid = int(pr["id"])
        code = str(pr.get("code") or "").strip()
        name = str(pr.get("name") or "").strip()
        first_idx = _project_first_cycle_index(pid)
        for idx in range(first_idx, 500):
            cyc = _biweekly_cycle_from_index(idx)
            if cyc["cycle_no"] < cutoff:
                continue
            if cyc["due_date"] > today:
                break
            expected_rows.append(
                {
                    "project_id": pid,
                    "project_code": code,
                    "project_name": name,
                    "cycle_no": int(cyc["cycle_no"]),
                    "due_date": cyc["due_date"],
                }
            )
        live_cyc, _reason = _project_current_due_cycle(pid, today)
        if live_cyc is not None:
            branch_queue.append(
                {
                    "project": f"{code}  {name}".strip(" "),
                    "report_no": int(live_cyc["cycle_no"]),
                    "window": f"{live_cyc['window_start'].isoformat()}  {live_cyc['window_end'].isoformat()}",
                    "due": live_cyc["due_date"].isoformat(),
                    "status": "Overdue" if today > live_cyc["due_date"] else ("Due soon" if (live_cyc["due_date"] - today).days <= 7 else "Open"),
                }
            )

    expected_df = pd.DataFrame(expected_rows)
    if expected_df.empty:
        empty_rows = pd.DataFrame(columns=["Staff", "Login", "Outstanding reports", "Next due"])
        posted_staff = fetch_df(
            """
        SELECT DISTINCT ps.staff_id
        FROM project_staff ps
        JOIN projects p ON p.id=ps.project_id
        WHERE COALESCE(p.status,'ACTIVE')!='DORMANT'
    """
        )
        return {
            "ratio": None,
            "no_obligation": True,
            "compliant": 0,
            "outstanding": 0,
            "total_staff": len(posted_staff),
            "rows": empty_rows,
            "branch_queue": pd.DataFrame(branch_queue),
            "staff_compliance": pd.DataFrame(columns=["Engineer", "On-time", "Late", "Very late", "Missed", "Points"]),
        }

    expected_df["key"] = expected_df.apply(lambda r: (int(r["project_id"]), int(r["cycle_no"])), axis=1)
    expected_df["has_valid"] = expected_df["key"].map(lambda k: k in valid_map)
    expected_df["timing_status"] = expected_df["key"].map(lambda k: valid_map.get(k, {}).get("timing_status"))
    submitted_reports = int(expected_df["has_valid"].sum())
    outstanding_reports = int((~expected_df["has_valid"]).sum())
    project_stats = {}
    for pid, grp in expected_df.groupby("project_id", sort=False):
        missing_df = grp[~grp["has_valid"]]
        first_missing = None
        if not missing_df.empty:
            try:
                first_missing = int(missing_df["cycle_no"].min())
            except Exception:
                first_missing = None
        timing_series = grp.loc[grp["has_valid"], "timing_status"].fillna("").astype(str).str.upper()
        project_stats[int(pid)] = {
            "missing_count": int((~grp["has_valid"]).sum()),
            "first_missing_cycle": first_missing,
            "ontime": int((timing_series == "ON_TIME").sum()),
            "late": int((timing_series == "LATE").sum()),
            "very_late": int((timing_series == "VERY_LATE").sum()),
        }

    staff_users = fetch_df(
        """
        SELECT u.id AS user_id, u.username, s.id AS staff_id, s.name
        FROM users u
        LEFT JOIN staff s ON s.id=u.staff_id
        WHERE COALESCE(u.is_active,1)=1
        ORDER BY s.name, u.username
    """
    )
    posted = fetch_df(
        """
        SELECT ps.staff_id, ps.project_id, p.code AS project_code
        FROM project_staff ps
        JOIN projects p ON p.id=ps.project_id
        WHERE COALESCE(p.status,'ACTIVE')!='DORMANT'
    """
    )
    proj_by_staff = {}
    if not posted.empty:
        for _, rr in posted.iterrows():
            try:
                proj_by_staff.setdefault(int(rr["staff_id"]), []).append((int(rr["project_id"]), str(rr.get("project_code") or "")))
            except Exception:
                pass
    posted_staff_ids = set(proj_by_staff.keys())
    if posted_staff_ids and not staff_users.empty:
        staff_users = staff_users[
            staff_users["staff_id"].notna() & staff_users["staff_id"].apply(lambda x: int(x) in posted_staff_ids if pd.notna(x) else False)
        ].copy()

    out_rows = []
    compliant = 0
    total_staff = len(staff_users)
    perf_rows = []
    for _, row in staff_users.iterrows():
        sid = row.get("staff_id")
        staff_name = row.get("name") or row.get("username") or "Unknown"
        if pd.isna(sid):
            compliant += 1
            continue
        sid = int(sid)
        outstanding = []
        ontime = late = very_late = missed = points = 0
        for pid, pcode in proj_by_staff.get(sid, []):
            stat = project_stats.get(int(pid), {"missing_count": 0, "first_missing_cycle": None, "ontime": 0, "late": 0, "very_late": 0})
            missed += int(stat["missing_count"])
            if stat["missing_count"] and stat["first_missing_cycle"] is not None:
                outstanding.append((int(stat["first_missing_cycle"]), pcode))
            ontime += int(stat["ontime"])
            late += int(stat["late"])
            very_late += int(stat["very_late"])
            points += int(stat["ontime"]) * 3 + int(stat["late"]) * 2 + int(stat["very_late"]) * 1
        if outstanding:
            first = sorted(outstanding, key=lambda x: x[0])[0]
            out_rows.append({"Staff": staff_name, "Login": row.get("username") or "", "Outstanding reports": len(outstanding), "Next due": f"Report {first[0]} ({first[1]})"})
        else:
            compliant += 1
        perf_rows.append({"Engineer": staff_name, "On-time": ontime, "Late": late, "Very late": very_late, "Missed": missed, "Points": points})

    ratio = (submitted_reports / len(expected_df)) if len(expected_df) else 1.0
    return {
        "ratio": ratio,
        "no_obligation": False,
        "compliant": compliant,
        "outstanding": outstanding_reports,
        "total_staff": total_staff,
        "rows": pd.DataFrame(out_rows) if out_rows else pd.DataFrame(columns=["Staff", "Login", "Outstanding reports", "Next due"]),
        "branch_queue": pd.DataFrame(branch_queue),
        "staff_compliance": pd.DataFrame(perf_rows).sort_values(["Points", "On-time", "Engineer"], ascending=[False, False, True])
        if perf_rows
        else pd.DataFrame(columns=["Engineer", "On-time", "Late", "Very late", "Missed", "Points"]),
    }


def historical_compliance_snapshot(today=None):
    """Full backlog compliance across all past due cycles."""
    today = _to_date_or_none(today) or date.today()
    try:
        snap = unified_biweekly_snapshot(today.isoformat())
        return {
            "ratio": snap.get("ratio"),
            "no_obligation": bool(snap.get("no_obligation") or False),
            "compliant": int(snap.get("compliant") or 0),
            "outstanding": int(snap.get("outstanding") or 0),
            "total_staff": int(snap.get("total_staff") or 0),
            "rows": snap.get("rows")
            if isinstance(snap.get("rows"), pd.DataFrame)
            else pd.DataFrame(columns=["Staff", "Login", "Outstanding reports", "Next due"]),
        }
    except Exception:
        empty = pd.DataFrame(columns=["Staff", "Login", "Outstanding reports", "Next due"])
        return {"ratio": None, "no_obligation": True, "compliant": 0, "outstanding": 0, "total_staff": 0, "rows": empty}


@st.cache_data(ttl=45, show_spinner=False)
def current_cycle_compliance_snapshot(today_iso: str):
    today = date.fromisoformat(today_iso)
    current_cycle = current_biweekly_cycle(today)
    current_cycle_no = int(current_cycle["cycle_no"])
    posted = fetch_df(
        """
        SELECT DISTINCT ps.staff_id, ps.project_id, p.code AS project_code, p.name AS project_name
        FROM project_staff ps
        JOIN projects p ON p.id=ps.project_id
        WHERE COALESCE(p.status,'ACTIVE')!='DORMANT'
    """
    )
    if posted.empty:
        empty_rows = pd.DataFrame(columns=["Staff", "Login", "Outstanding projects", "Missing project"])
        return {
            "ratio": None,
            "no_obligation": True,
            "compliant": 0,
            "outstanding": 0,
            "total_staff": 0,
            "rows": empty_rows,
            "current_cycle_no": current_cycle_no,
            "due_date": current_cycle.get("due_date"),
        }

    valid = _submitted_biweekly_reports_df()
    valid_project_ids = set()
    if not valid.empty:
        for _, rr in valid.iterrows():
            try:
                if int(rr.get("cycle_no")) == current_cycle_no:
                    valid_project_ids.add(int(rr["project_id"]))
            except Exception:
                continue

    expected_project_ids = set()
    proj_by_staff = {}
    proj_label_by_id = {}
    for _, rr in posted.iterrows():
        try:
            sid = int(rr["staff_id"])
            pid = int(rr["project_id"])
        except Exception:
            continue
        expected_project_ids.add(pid)
        proj_by_staff.setdefault(sid, []).append(pid)
        proj_label_by_id[pid] = "  ".join([x for x in [str(rr.get("project_code") or "").strip(), str(rr.get("project_name") or "").strip()] if x])

    staff_users = fetch_df(
        """
        SELECT u.id AS user_id, u.username, s.id AS staff_id, s.name
        FROM users u
        LEFT JOIN staff s ON s.id=u.staff_id
        WHERE COALESCE(u.is_active,1)=1
        ORDER BY s.name, u.username
    """
    )
    if not staff_users.empty:
        staff_users = staff_users[
            staff_users["staff_id"].notna() & staff_users["staff_id"].apply(lambda x: int(x) in proj_by_staff if pd.notna(x) else False)
        ].copy()

    compliant = 0
    out_rows = []
    for _, row in staff_users.iterrows():
        sid = row.get("staff_id")
        if pd.isna(sid):
            continue
        sid = int(sid)
        assigned = sorted(set(proj_by_staff.get(sid, [])))
        missing = [pid for pid in assigned if pid not in valid_project_ids]
        if missing:
            first_pid = missing[0]
            out_rows.append(
                {
                    "Staff": row.get("name") or row.get("username") or "Unknown",
                    "Login": row.get("username") or "",
                    "Outstanding projects": len(missing),
                    "Missing project": proj_label_by_id.get(first_pid) or str(first_pid),
                }
            )
        else:
            compliant += 1

    ratio = (len(expected_project_ids & valid_project_ids) / len(expected_project_ids)) if expected_project_ids else None
    return {
        "ratio": ratio,
        "no_obligation": len(expected_project_ids) == 0,
        "compliant": compliant,
        "outstanding": int(len(expected_project_ids - valid_project_ids)),
        "total_staff": int(len(staff_users)),
        "rows": pd.DataFrame(out_rows) if out_rows else pd.DataFrame(columns=["Staff", "Login", "Outstanding projects", "Missing project"]),
        "current_cycle_no": current_cycle_no,
        "due_date": current_cycle.get("due_date"),
    }


@st.cache_data(ttl=45, show_spinner=False)
def my_current_cycle_compliance_snapshot(staff_id: int, today_iso: str):
    today = date.fromisoformat(today_iso)
    current_cycle = current_biweekly_cycle(today)
    current_cycle_no = int(current_cycle["cycle_no"])
    posted = fetch_df(
        """
        SELECT DISTINCT ps.project_id, p.code AS project_code, p.name AS project_name
        FROM project_staff ps
        JOIN projects p ON p.id=ps.project_id
        WHERE ps.staff_id=?
          AND COALESCE(p.status,'ACTIVE')!='DORMANT'
        ORDER BY p.code, p.name
        """,
        (int(staff_id),),
    )
    assigned_projects = []
    for _, rr in posted.iterrows():
        try:
            pid = int(rr["project_id"])
        except Exception:
            continue
        assigned_projects.append(
            {
                "project_id": pid,
                "project_label": " - ".join([x for x in [str(rr.get("project_code") or "").strip(), str(rr.get("project_name") or "").strip()] if x]),
            }
        )

    submitted_ids = set()
    if assigned_projects:
        valid_df = fetch_df(
            """
            SELECT DISTINCT project_id
            FROM biweekly_reports
            WHERE uploader_staff_id=?
              AND cycle_no=?
              AND COALESCE(status,'PENDING') IN ('SUBMITTED','APPROVED','NEEDS_REVISION','PENDING')
            """,
            (int(staff_id), int(current_cycle_no)),
        )
        for _, rr in valid_df.iterrows():
            try:
                submitted_ids.add(int(rr["project_id"]))
            except Exception:
                continue

    expected_ids = {row["project_id"] for row in assigned_projects}
    submitted_expected = expected_ids & submitted_ids
    return {
        "submitted": int(len(submitted_expected)),
        "expected": int(len(expected_ids)),
        "outstanding": int(len(expected_ids - submitted_expected)),
        "current_cycle_no": current_cycle_no,
        "due_date": current_cycle.get("due_date"),
        "projects": assigned_projects,
    }


def compliance_snapshot(today=None):
    """Current-cycle compliance for assigned active projects."""
    today = _to_date_or_none(today) or date.today()
    try:
        return current_cycle_compliance_snapshot(today.isoformat())
    except Exception:
        empty = pd.DataFrame(columns=["Staff", "Login", "Outstanding projects", "Missing project"])
        current_cycle = current_biweekly_cycle(today)
        return {
            "ratio": None,
            "no_obligation": True,
            "compliant": 0,
            "outstanding": 0,
            "total_staff": 0,
            "rows": empty,
            "current_cycle_no": int(current_cycle["cycle_no"]),
            "due_date": current_cycle.get("due_date"),
        }


@st.cache_data(ttl=30, show_spinner=False)
def dashboard_summary_snapshot(today_iso: str):
    out = {"projects": 0, "active_projects": 0, "dormant_projects": 0, "open_tasks": 0, "tasks_completed_7d": 0}
    try:
        pdf = fetch_df(
            """
            SELECT
                COUNT(*) AS projects,
                SUM(CASE WHEN UPPER(COALESCE(status,'ACTIVE'))='ACTIVE' THEN 1 ELSE 0 END) AS active_projects,
                SUM(CASE WHEN UPPER(COALESCE(status,'ACTIVE'))='DORMANT' THEN 1 ELSE 0 END) AS dormant_projects
            FROM projects
        """
        )
        if not pdf.empty:
            row = pdf.iloc[0]
            out["projects"] = int(row.get("projects") or 0)
            out["active_projects"] = int(row.get("active_projects") or 0)
            out["dormant_projects"] = int(row.get("dormant_projects") or 0)
    except Exception:
        pass
    try:
        tdf = fetch_df("SELECT COUNT(*) AS n FROM task_assignments WHERE COALESCE(status,'')!='Completed'")
        if not tdf.empty:
            out["open_tasks"] = int(tdf["n"].iloc[0] or 0)
    except Exception:
        pass
    try:
        recent = fetch_df(
            "SELECT COUNT(*) AS n FROM task_assignments WHERE status='Completed' AND completed_date IS NOT NULL AND date(completed_date) >= date(?)",
            ((date.fromisoformat(today_iso) - timedelta(days=7)).isoformat(),),
        )
        if not recent.empty:
            out["tasks_completed_7d"] = int(recent["n"].iloc[0] or 0)
    except Exception:
        pass
    return out
