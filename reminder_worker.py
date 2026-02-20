import os, smtplib, ssl
import math
from datetime import date, timedelta
from email.message import EmailMessage
from dateutil import parser as dtparser
import pandas as pd

try:
    import psycopg2
except Exception:
    psycopg2 = None  # type: ignore

import sqlite3

DB_URL = os.getenv("DATABASE_URL") or os.getenv("WORKNEST_DB_URL") or ""
DB_IS_POSTGRES = bool(DB_URL.strip().lower().startswith(("postgres://","postgresql://")))
DB_PATH = os.getenv("WORKNEST_DB_PATH","worknest.db")

def _adapt_query(q: str) -> str:
    if DB_IS_POSTGRES:
        return q.replace("?", "%s")
    return q

def get_conn():
    if DB_IS_POSTGRES:
        if not psycopg2:
            raise RuntimeError("psycopg2 not installed. Add psycopg2-binary.")
        return psycopg2.connect(DB_URL)
    c=sqlite3.connect(DB_PATH, check_same_thread=False)
    c.execute("PRAGMA foreign_keys=ON")
    return c

def fetch_df(q, p=()):
    q=_adapt_query(q)
    c=get_conn()
    try:
        df=pd.read_sql_query(q, c, params=p)
        return df
    finally:
        try: c.close()
        except Exception: pass

def execute(q, p=()):
    q=_adapt_query(q).strip()
    # Postgres compatibility: SQLite uses INSERT OR IGNORE
    if DB_IS_POSTGRES and q.lower().startswith("insert or ignore"):
        q = "INSERT" + q[len("INSERT OR IGNORE"):]
        # Best-effort: rely on unique constraints; do nothing on conflict
        if " on conflict" not in q.lower():
            q = q.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
    c=get_conn()
    try:
        if DB_IS_POSTGRES:
            with c:
                with c.cursor() as cur:
                    cur.execute(q, p)
        else:
            cur=c.cursor()
            cur.execute(q, p)
            c.commit()
    finally:
        try: c.close()
        except Exception: pass


def _get_setting(key:str, default:str="") -> str:
    try:
        df = fetch_df("SELECT value FROM app_settings WHERE key=?", (key,))
        if not df.empty:
            v = df.iloc[0].get("value")
            return "" if v is None else str(v)
    except Exception:
        pass
    return default

def _month_start(d:date) -> date:
    return date(d.year, d.month, 1)

def _month_end(d:date) -> date:
    ms=_month_start(d)
    if ms.month==12:
        nxt=date(ms.year+1,1,1)
    else:
        nxt=date(ms.year, ms.month+1, 1)
    return nxt - timedelta(days=1)

def _is_last_day_of_month(d:date)->bool:
    try:
        return d == _month_end(d)
    except Exception:
        return False

def run_staff_of_month_post(today:date|None=None)->dict:
    if today is None:
        today=date.today()
    if not _is_last_day_of_month(today):
        return {"posted":0, "reason":"not_last_day"}

    ms=_month_start(today)
    mstr=str(ms)

    # Prevent duplicates
    try:
        already=fetch_df("SELECT 1 FROM staff_of_month_posts WHERE month=?", (mstr,))
        if not already.empty:
            return {"posted":0, "reason":"already_posted"}
    except Exception:
        return {"posted":0, "reason":"missing_tables"}

    # Ensure monthly base is stored (best-effort: compute minimal base and upsert)
    try:
        staff=fetch_df("SELECT id FROM staff")
        if staff.empty:
            return {"posted":0, "reason":"no_staff"}
        # Tasks completed in month
        me=pd.Timestamp(today).to_period('M').end_time.date()
        tdf=fetch_df("""SELECT A.staff_id, T.date_assigned, T.days_allotted, A.completed_date
                        FROM task_assignments A JOIN tasks T ON T.id=A.task_id
                       WHERE A.status='Completed'
                         AND date(A.completed_date) BETWEEN date(?) AND date(?)""", (str(ms), str(me)))
        # Test results in month
        testdf=fetch_df("""SELECT uploader_staff_id AS staff_id, COUNT(1) AS n
                           FROM test_results
                          WHERE uploader_staff_id IS NOT NULL
                            AND date(uploaded_at) BETWEEN date(?) AND date(?)
                          GROUP BY uploader_staff_id""", (str(ms), str(me)))
        testmap={int(r['staff_id']): int(r['n']) for _,r in testdf.iterrows()} if not testdf.empty else {}

        def task_points(da, allotted, cd):
            try:
                da=dtparser.parse(str(da)).date()
                cd=dtparser.parse(str(cd)).date()
                allotted=int(allotted or 0)
                if allotted<=0: return 0
                days=(cd-da).days+1
                if days<=allotted: return 3
                if days<=int(math.ceil(1.5*allotted)): return 2
                return 1
            except Exception:
                return 0

        for _,sr in staff.iterrows():
            sid=int(sr['id'])
            srows=tdf[tdf['staff_id']==sid] if not tdf.empty else pd.DataFrame()
            tp=sum(task_points(r['date_assigned'], r['days_allotted'], r['completed_date']) for _,r in srows.iterrows())
            # Reports are skipped in worker for simplicity; the app UI can compute/refresh monthly.
            rp=0
            tsp=testmap.get(sid,0)*3
            # Upsert (requires unique constraint)
            if DB_IS_POSTGRES:
                execute("""INSERT INTO performance_index (staff_id, month, task_points, report_points, test_points)
                           VALUES (?,?,?,?,?)
                           ON CONFLICT (staff_id, month) DO UPDATE
                           SET task_points=EXCLUDED.task_points,
                               report_points=EXCLUDED.report_points,
                               test_points=EXCLUDED.test_points""", (sid, mstr, int(tp), int(rp), int(tsp)))
            else:
                execute("INSERT OR REPLACE INTO performance_index (staff_id, month, task_points, report_points, test_points) VALUES (?,?,?,?,?)",
                        (sid, mstr, int(tp), int(rp), int(tsp)))
    except Exception:
        # If this fails, still try to post based on existing performance_index
        pass

    # Determine include-soft toggle
    inc_soft = _get_setting("PERF_INCLUDE_SOFT", "0").strip().lower() in ("1","true","yes")

    lb=fetch_df("""SELECT PI.staff_id, S.name, S.rank, PI.task_points, PI.report_points, PI.test_points,
                        PI.reliability_score, PI.attention_to_detail_score
                   FROM performance_index PI JOIN staff S ON S.id=PI.staff_id
                  WHERE PI.month=?""", (mstr,))
    if lb.empty:
        return {"posted":0, "reason":"no_performance"}

    if inc_soft:
        lb['total'] = lb[['task_points','report_points','test_points','reliability_score','attention_to_detail_score']].fillna(0).sum(axis=1)
    else:
        lb['total'] = lb[['task_points','report_points','test_points']].fillna(0).sum(axis=1)
    lb=lb.sort_values(['total','test_points','report_points','task_points','name'], ascending=[False,False,False,False,True])
    top=lb.iloc[0]
    month_label=ms.strftime('%B %Y')
    msg=(
        f"🏆 Staff of the Month — {month_label}\n\n"
        f"🥇 {top['name']} ({top.get('rank','')})\n"
        f"Total Score: {int(top['total'])} points\n\n"
        f"Breakdown: Tasks {int(top['task_points'])} | Biweekly Reports {int(top['report_points'])} | Test Reports {int(top['test_points'])}"
    )
    if inc_soft:
        msg += f" | Reliability {int(top.get('reliability_score') or 0)} | Attention to Detail {int(top.get('attention_to_detail_score') or 0)}"
    msg += "\n\n— WorkNest (Performance Index)"

    nowiso=pd.Timestamp.utcnow().isoformat(timespec='seconds')
    # post as Admin (staff_id=1) fallback
    poster_id=1
    try:
        if DB_IS_POSTGRES:
            execute("INSERT INTO chat_messages (staff_id, message) VALUES (?,?)", (poster_id, msg))
            execute("INSERT INTO staff_of_month_posts (month, staff_id, total_score, posted_at) VALUES (?,?,?,?)",
                    (mstr, int(top['staff_id']), int(top['total']), nowiso))
        else:
            execute("INSERT INTO chat_messages (staff_id, message, posted_at) VALUES (?,?,?)", (poster_id, msg, nowiso))
            execute("INSERT OR REPLACE INTO staff_of_month_posts (month, staff_id, total_score, posted_at) VALUES (?,?,?,?)",
                    (mstr, int(top['staff_id']), int(top['total']), nowiso))
        return {"posted":1, "reason":"ok"}
    except Exception as e:
        return {"posted":0, "reason":f"error:{type(e).__name__}"}

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
    som=run_staff_of_month_post()
    print({"reminders":stats, "staff_of_month":som})
