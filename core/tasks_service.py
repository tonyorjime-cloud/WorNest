import os
import smtplib
import ssl
import uuid
from datetime import date, datetime
from email.message import EmailMessage

import numpy as np
import requests
import streamlit as st
from dateutil import parser as dtparser

from core.auth import current_staff_id, is_admin
from core.dates import _parse_date_safe
from core.db import UPLOAD_DIR, execute, fetch_df
from core.permissions import has_perm, is_section_head


def _onesignal_cfg():
    return {
        "app_id": (os.getenv("ONESIGNAL_APP_ID") or "").strip(),
        "api_key": (os.getenv("ONESIGNAL_REST_API_KEY") or "").strip(),
    }


def send_push(external_user_ids, title: str, message: str):
    """Send a push notification to OneSignal users identified by external_user_ids (emails)."""
    cfg = _onesignal_cfg()
    app_id = cfg.get("app_id")
    api_key = cfg.get("api_key")
    if not app_id or not api_key:
        return False
    if not external_user_ids:
        return False
    ids = [str(x).strip() for x in external_user_ids if str(x).strip()]
    ids = list(dict.fromkeys(ids))
    if not ids:
        return False
    try:
        r = requests.post(
            "https://onesignal.com/api/v1/notifications",
            headers={
                "Authorization": f"Basic {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "app_id": app_id,
                "include_external_user_ids": ids,
                "headings": {"en": title[:60]},
                "contents": {"en": message[:240]},
            },
            timeout=10,
        )
        return bool(r.ok)
    except Exception:
        return False


def _task_points(date_assigned, days_allotted: int, completed_date) -> int:
    da = _parse_date_safe(date_assigned)
    cd = _parse_date_safe(completed_date)
    if not da or not cd:
        return 0
    if not days_allotted or days_allotted <= 0:
        return 3
    days = (cd - da).days + 1
    if days <= days_allotted:
        return 3
    if days <= int(np.ceil(1.5 * days_allotted)):
        return 2
    return 1


def smtp_configured() -> bool:
    return bool(os.getenv("SMTP_HOST") and os.getenv("SMTP_USER") and os.getenv("SMTP_PASSWORD"))


def send_email(to_email: str, subject: str, body: str) -> tuple[bool, str]:
    """Send plain-text email via SMTP."""
    if not to_email:
        return (False, "missing recipient")
    host = os.getenv("SMTP_HOST", "").strip()
    port = int(os.getenv("SMTP_PORT", "587").strip() or "587")
    user = os.getenv("SMTP_USER", "").strip()
    pwd = os.getenv("SMTP_PASSWORD", "").strip()
    use_tls = os.getenv("SMTP_TLS", "1").strip() not in ["0", "false", "False"]
    sender = os.getenv("SMTP_FROM", user).strip() or user
    if not (host and user and pwd):
        return (False, "SMTP not configured")

    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)

    ctx = ssl.create_default_context()
    try:
        if use_tls:
            with smtplib.SMTP(host, port, timeout=20) as s:
                s.ehlo()
                s.starttls(context=ctx)
                s.login(user, pwd)
                s.send_message(msg)
        else:
            with smtplib.SMTP(host, port, timeout=20) as s:
                s.login(user, pwd)
                s.send_message(msg)
        return (True, "sent")
    except Exception as e:
        return (False, f"{type(e).__name__}: {e}")


def run_task_reminders(today: date | None = None, horizon_days: int = 2) -> dict:
    """Checks due-soon and overdue tasks and sends email reminders once per day per assignment + type."""
    if today is None:
        today = date.today()
    today_s = str(today)

    df = fetch_df(
        """
        SELECT
            ta.id AS assignment_id,
            ta.status,
            t.title,
            t.due_date,
            t.date_assigned,
            s.name AS staff_name,
            s.email AS staff_email,
            p.code AS project_code,
            p.name AS project_name
        FROM task_assignments ta
        JOIN tasks t ON t.id=ta.task_id
        JOIN staff s ON s.id=ta.staff_id
        LEFT JOIN projects p ON p.id=t.project_id
        WHERE ta.status!='Completed'
    """
    )
    if df.empty:
        return {"checked": 0, "sent": 0, "skipped": 0, "errors": 0}

    sent = skipped = errors = 0
    for _, r in df.iterrows():
        try:
            due = dtparser.parse(r["due_date"]).date()
        except Exception:
            continue
        days_to_due = (due - today).days
        if days_to_due < 0:
            rtype = "overdue"
        elif days_to_due <= horizon_days:
            rtype = "due_soon"
        else:
            continue

        already = fetch_df(
            "SELECT 1 FROM reminders_sent WHERE assignment_id=? AND reminder_type=? AND sent_on=?",
            (int(r["assignment_id"]), rtype, today_s),
        )
        if not already.empty:
            continue

        proj = ""
        if r.get("project_code") is not None and r.get("project_name") is not None:
            proj = f"{r['project_code']} — {r['project_name']}"
        elif r.get("project_code") is not None:
            proj = str(r["project_code"])
        subj = f"WorkNest: Task reminder ({'OVERDUE' if rtype == 'overdue' else 'Due soon'}) — {r['title']}"
        body_lines = [
            f"Hello {r['staff_name']},",
            "",
            "This is an automated reminder from WorkNest.",
            "",
            f"Task: {r['title']}",
            f"Due date: {due.isoformat()}",
        ]
        if proj:
            body_lines.append(f"Project: {proj}")
        if rtype == "overdue":
            body_lines.append(f"Status: OVERDUE by {abs(days_to_due)} day(s)")
        else:
            body_lines.append(f"Status: Due in {days_to_due} day(s)")
        body_lines += [
            "",
            "Please log into WorkNest to review the task details and attachments.",
            "",
            " WorkNest",
        ]
        ok, msg = send_email(str(r.get("staff_email") or "").strip(), subj, "\n".join(body_lines))
        if ok:
            sent += 1
            execute(
                "INSERT OR IGNORE INTO reminders_sent (assignment_id, reminder_type, sent_on) VALUES (?,?,?)",
                (int(r["assignment_id"]), rtype, today_s),
            )
        else:
            if msg == "missing recipient" or msg == "SMTP not configured":
                skipped += 1
            else:
                errors += 1
    return {"checked": int(len(df)), "sent": sent, "skipped": skipped, "errors": errors}


def current_staff_section() -> str | None:
    sid = current_staff_id()
    if sid is None:
        return None
    df = fetch_df("SELECT section FROM staff WHERE id=?", (int(sid),))
    if df.empty:
        return None
    sec = df["section"].iloc[0]
    return str(sec).strip() if sec is not None else None


def is_assigned_to_task(task_id: int, staff_id: int | None = None) -> bool:
    sid = staff_id if staff_id is not None else current_staff_id()
    if sid is None:
        return False
    df = fetch_df("SELECT 1 FROM task_assignments WHERE task_id=? AND staff_id=?", (int(task_id), int(sid)))
    return not df.empty


def can_upload_task_files(task_row: dict) -> bool:
    if is_admin():
        return True
    sid = current_staff_id()
    if sid is None:
        return False
    try:
        tid = int(task_row.get("id"))
    except Exception:
        return False
    try:
        if int(task_row.get("created_by_staff_id") or -1) == sid:
            return True
    except Exception:
        pass
    return is_assigned_to_task(tid, sid)


def can_download_task_files(task_row: dict) -> bool:
    return can_upload_task_files(task_row) or is_section_head()


def save_uploaded_file(uploaded_file, subfolder=""):
    if uploaded_file is None:
        return None
    original_name = getattr(uploaded_file, "name", "") or "upload.bin"
    _, ext = os.path.splitext(original_name)
    fname = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex}{ext or '.bin'}"
    folder = os.path.join(UPLOAD_DIR, subfolder) if subfolder else UPLOAD_DIR
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, fname)
    with open(path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return path


def file_download_button(label, file_path, key):
    try:
        with open(file_path, "rb") as f:
            data = f.read()
        st.download_button(label, data=data, file_name=os.path.basename(file_path), key=key)
    except Exception:
        st.error(f"Missing file: {file_path}")




def _report_scoreboard(today=None, month_start=None, month_end=None):
    from core.dashboard_service import report_scoreboard

    return report_scoreboard(today=today, month_start=month_start, month_end=month_end)


def compliance_snapshot(today=None):
    from core.dashboard_service import compliance_snapshot as _compliance_snapshot

    return _compliance_snapshot(today)
