import os, sqlite3, smtplib, ssl
from datetime import date
from email.message import EmailMessage
from dateutil import parser as dtparser
import pandas as pd

DB_PATH=os.getenv("WORKNEST_DB_PATH","worknest.db")

def get_conn():
    c=sqlite3.connect(DB_PATH, check_same_thread=False)
    c.execute("PRAGMA foreign_keys=ON")
    return c

def fetch_df(q, p=()):
    c=get_conn()
    df=pd.read_sql_query(q, c, params=p)
    c.close()
    return df

def execute(q, p=()):
    c=get_conn(); cur=c.cursor()
    cur.execute(q, p); c.commit()
    lid=cur.lastrowid
    c.close()
    return lid

def smtp_configured()->bool:
    return bool(os.getenv("SMTP_HOST") and os.getenv("SMTP_USER") and os.getenv("SMTP_PASSWORD"))

def send_email(to_email:str, subject:str, body:str)->tuple[bool,str]:
    if not to_email:
        return (False, "missing recipient")
    host=os.getenv("SMTP_HOST","").strip()
    port=int(os.getenv("SMTP_PORT","587").strip() or "587")
    user=os.getenv("SMTP_USER","").strip()
    pwd=os.getenv("SMTP_PASSWORD","").strip()
    use_tls=(os.getenv("SMTP_TLS","1").strip() not in ["0","false","False"])
    sender=os.getenv("SMTP_FROM", user).strip() or user
    if not (host and user and pwd):
        return (False, "SMTP not configured")

    msg=EmailMessage()
    msg["From"]=sender
    msg["To"]=to_email
    msg["Subject"]=subject
    msg.set_content(body)

    ctx=ssl.create_default_context()
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

def run_task_reminders(today:date|None=None, horizon_days:int=2)->dict:
    if today is None: today=date.today()
    today_s=str(today)

    df=fetch_df("""
        SELECT
            ta.id AS assignment_id,
            ta.status,
            t.title,
            t.due_date,
            s.name AS staff_name,
            s.email AS staff_email,
            p.code AS project_code,
            p.name AS project_name
        FROM task_assignments ta
        JOIN tasks t ON t.id=ta.task_id
        JOIN staff s ON s.id=ta.staff_id
        LEFT JOIN projects p ON p.id=t.project_id
        WHERE ta.status!='Completed'
    """)
    if df.empty:
        return {"checked":0,"sent":0,"skipped":0,"errors":0}

    sent=skipped=errors=0
    for _,r in df.iterrows():
        try:
            due=dtparser.parse(r["due_date"]).date()
        except Exception:
            continue
        days_to_due=(due - today).days
        if days_to_due < 0:
            rtype="overdue"
        elif days_to_due <= horizon_days:
            rtype="due_soon"
        else:
            continue

        already=fetch_df("SELECT 1 FROM reminders_sent WHERE assignment_id=? AND reminder_type=? AND sent_on=?",
                         (int(r["assignment_id"]), rtype, today_s))
        if not already.empty:
            continue

        proj=""
        if pd.notna(r.get("project_code")) and pd.notna(r.get("project_name")):
            proj=f"{r['project_code']} — {r['project_name']}"
        elif pd.notna(r.get("project_code")):
            proj=str(r["project_code"])

        subj=f"WorkNest: Task reminder ({'OVERDUE' if rtype=='overdue' else 'Due soon'}) — {r['title']}"
        body_lines=[
            f"Hello {r['staff_name']},",
            "",
            "This is an automated reminder from WorkNest.",
            "",
            f"Task: {r['title']}",
            f"Due date: {due.isoformat()}",
        ]
        if proj:
            body_lines.append(f"Project: {proj}")
        if rtype=="overdue":
            body_lines.append(f"Status: OVERDUE by {abs(days_to_due)} day(s)")
        else:
            body_lines.append(f"Status: Due in {days_to_due} day(s)")
        body_lines += ["", "Please log into WorkNest to review the task details and attachments.", "", "— WorkNest"]

        ok,msg=send_email(str(r.get("staff_email") or "").strip(), subj, "\n".join(body_lines))
        if ok:
            sent += 1
            execute("INSERT OR IGNORE INTO reminders_sent (assignment_id, reminder_type, sent_on) VALUES (?,?,?)",
                    (int(r["assignment_id"]), rtype, today_s))
        else:
            if msg in ["missing recipient", "SMTP not configured"]:
                skipped += 1
            else:
                errors += 1

    return {"checked":int(len(df)),"sent":sent,"skipped":skipped,"errors":errors}

if __name__=="__main__":
    stats=run_task_reminders()
    print(stats)
