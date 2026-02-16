import os, hashlib
import smtplib, ssl
from email.message import EmailMessage
from datetime import datetime, date, timedelta
from dateutil import parser as dtparser
import pandas as pd, numpy as np, streamlit as st

try:
    import psycopg2
    import psycopg2.extras
except Exception:
    psycopg2 = None  # type: ignore

import sqlite3

# Database backend selection
DB_URL = os.getenv('DATABASE_URL') or os.getenv('WORKNEST_DB_URL') or ''
DB_IS_POSTGRES = bool(DB_URL.strip().lower().startswith(('postgres://','postgresql://')))

st.set_page_config(page_title="WorkNest Mini v3.2.1", layout="wide")
APP_TITLE="WorkNest Mini v3.2.1"
# --- Storage paths (Render-safe) ---
def _first_writable_dir(candidates):
    for d in candidates:
        if not d:
            continue
        try:
            os.makedirs(d, exist_ok=True)
            test_path=os.path.join(d, ".worknest_write_test")
            with open(test_path, "w", encoding="utf-8") as f:
                f.write("ok")
            os.remove(test_path)
            return d
        except Exception:
            continue
    return ""

ENV_DATA_DIR=os.getenv("WORKNEST_DATA_DIR","").strip()
DEFAULT_LOCAL_DATA=os.path.join(os.getcwd(), "data")
DATA_DIR=_first_writable_dir([ENV_DATA_DIR, DEFAULT_LOCAL_DATA, os.getcwd()])

DB_PATH=os.getenv('WORKNEST_DB_PATH', os.path.join(DATA_DIR,'worknest.db'))
UPLOAD_DIR=os.getenv("WORKNEST_UPLOAD_DIR", os.path.join(DATA_DIR,"uploads"))


# Ensure persistence paths exist
os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
os.makedirs(UPLOAD_DIR, exist_ok=True)

CORE_DOC_CATEGORIES=["architectural","structural","electrical","mechanical","soil_investigation","boq","program_of_work"]
STAGES=["Substructure","Ground Floor","Typical Floor","Roof","External Works"]
TEST_TYPES_DISPLAY=[
    ("slump","Concrete Slump Test"),
    ("concube","Concrete Cube Test"),
    ("steel","Steel Test (Batch)"),
    ("reinforcement","Reinforcement Test (Batch)"),
]
RANK_ORDER=["Higher Technical Officer","Senior Technical Officer","Engineer II","Engineer I","Senior Engineer","Principal Engineer","Assistant Chief Engineer","Chief Engineer","Assistant Director"]
RANK_TO_INDEX={r:i for i,r in enumerate(RANK_ORDER)}

def normalize_rank(r):
    if not r: return None
    r=str(r).strip()
    aliases={
        "Asst. Director":"Assistant Director",
        "Assistant Dir":"Assistant Director",
        "Engr I":"Engineer I",
        "Engr II":"Engineer II",
        "Engineer 1":"Engineer I",
        "Engineer 2":"Engineer II",
    }
    return aliases.get(r, r)

def rank_index_safe(r):
    rr=normalize_rank(r)
    return RANK_TO_INDEX.get(rr, None)

def get_conn():
    if DB_IS_POSTGRES:
        if not psycopg2:
            raise RuntimeError("psycopg2 is not installed. Add psycopg2-binary to requirements.txt")
        if not DB_URL:
            raise RuntimeError("DATABASE_URL (or WORKNEST_DB_URL) is not set.")
        return psycopg2.connect(DB_URL)
    c=sqlite3.connect(DB_PATH, check_same_thread=False)
    c.execute("PRAGMA foreign_keys=ON")
    return c


def _adapt_query(q: str) -> str:
    if DB_IS_POSTGRES:
        return q.replace("?", "%s")
    return q

def _exec_script(cur, sql_script: str):
    if not sql_script:
        return
    if not DB_IS_POSTGRES:
        cur.executescript(sql_script)
        return
    stmts=[s.strip() for s in sql_script.split(";") if s.strip()]
    for stmt in stmts:
        cur.execute(stmt)

def hash_pwd(p):
    return hashlib.sha256(("worknest_salt_"+str(p)).encode("utf-8")).hexdigest()

def init_db():
    c = get_conn()
    cur = c.cursor()

    if DB_IS_POSTGRES:
        pg_schema = """CREATE TABLE IF NOT EXISTS public_holidays (
  id SERIAL PRIMARY KEY,
  date TEXT NOT NULL,
  name TEXT
);

CREATE TABLE IF NOT EXISTS staff (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  rank TEXT NOT NULL,
  email TEXT UNIQUE,
  phone TEXT,
  section TEXT,
  role TEXT,
  grade TEXT,
  join_date TEXT,
  dob TEXT
);

CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL,
  username TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  is_admin INTEGER DEFAULT 0,
  role TEXT DEFAULT 'staff',
  is_active INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS projects (
  id SERIAL PRIMARY KEY,
  code TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL,
  client TEXT,
  location TEXT,
  rebar_strength DOUBLE PRECISION,
  concrete_strength DOUBLE PRECISION,
  target_slump_min DOUBLE PRECISION,
  target_slump_max DOUBLE PRECISION,
  supervisor_staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL,
  start_date TEXT,
  end_date TEXT
);

CREATE TABLE IF NOT EXISTS project_staff (
  id SERIAL PRIMARY KEY,
  project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  staff_id INTEGER NOT NULL REFERENCES staff(id) ON DELETE CASCADE,
  role TEXT,
  UNIQUE(project_id, staff_id)
);

CREATE TABLE IF NOT EXISTS buildings (
  id SERIAL PRIMARY KEY,
  project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  floors INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS documents (
  id SERIAL PRIMARY KEY,
  project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  building_id INTEGER REFERENCES buildings(id) ON DELETE SET NULL,
  category TEXT NOT NULL,
  file_path TEXT NOT NULL,
  uploaded_at TEXT NOT NULL,
  uploader_staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS biweekly_reports (
  id SERIAL PRIMARY KEY,
  project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  report_date TEXT NOT NULL,
  file_path TEXT,
  uploader_staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS tasks (
  id SERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  description TEXT,
  date_assigned TEXT NOT NULL,
  days_allotted INTEGER NOT NULL,
  due_date TEXT NOT NULL,
  project_id INTEGER REFERENCES projects(id) ON DELETE SET NULL,
  created_by_staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS task_assignments (
  id SERIAL PRIMARY KEY,
  task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  staff_id INTEGER NOT NULL REFERENCES staff(id) ON DELETE CASCADE,
  status TEXT DEFAULT 'In progress',
  completed_date TEXT,
  days_taken INTEGER
);

CREATE TABLE IF NOT EXISTS task_documents (
  id SERIAL PRIMARY KEY,
  task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
  file_path TEXT NOT NULL,
  original_name TEXT,
  uploaded_at TEXT NOT NULL,
  uploader_staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS reminders_sent (
  id SERIAL PRIMARY KEY,
  assignment_id INTEGER NOT NULL REFERENCES task_assignments(id) ON DELETE CASCADE,
  reminder_type TEXT NOT NULL,
  sent_on TEXT NOT NULL,
  UNIQUE(assignment_id, reminder_type, sent_on)
);

CREATE TABLE IF NOT EXISTS leaves (
  id SERIAL PRIMARY KEY,
  staff_id INTEGER NOT NULL REFERENCES staff(id) ON DELETE CASCADE,
  leave_type TEXT NOT NULL,
  start_date TEXT NOT NULL,
  end_date TEXT NOT NULL,
  working_days INTEGER DEFAULT 0,
  relieving_staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL,
  status TEXT DEFAULT 'Pending',
  reason TEXT,
  request_date TEXT,
  approved_by_staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS test_results (
  id SERIAL PRIMARY KEY,
  project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
  building_id INTEGER REFERENCES buildings(id) ON DELETE SET NULL,
  stage TEXT,
  test_type TEXT NOT NULL,
  batch_id TEXT,
  file_path TEXT NOT NULL,
  uploaded_at TEXT NOT NULL,
  uploader_staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS points (
  id SERIAL PRIMARY KEY,
  staff_id INTEGER NOT NULL REFERENCES staff(id) ON DELETE CASCADE,
  source TEXT NOT NULL,
  source_id INTEGER NOT NULL,
  points INTEGER NOT NULL,
  awarded_at TEXT NOT NULL,
  UNIQUE(staff_id, source, source_id)
);
"""
        _exec_script(cur, pg_schema)

        # --- Postgres schema migrations (idempotent) ---
        def _pg_has_column(table: str, column: str) -> bool:
            cur.execute(
                """SELECT 1
                   FROM information_schema.columns
                  WHERE table_schema='public' AND table_name=%s AND column_name=%s
                  LIMIT 1""",
                (table, column),
            )
            return cur.fetchone() is not None

        def _pg_add_column(ddl: str):
            cur.execute(ddl)

        # leaves: align old schema to new fields expected by UI
        if not _pg_has_column('leaves', 'relieving_staff_id'):
            _pg_add_column("ALTER TABLE leaves ADD COLUMN relieving_staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL")
        if not _pg_has_column('leaves', 'status'):
            _pg_add_column("ALTER TABLE leaves ADD COLUMN status TEXT DEFAULT 'Pending'")
        if not _pg_has_column('leaves', 'reason'):
            _pg_add_column("ALTER TABLE leaves ADD COLUMN reason TEXT")
        if not _pg_has_column('leaves', 'request_date'):
            _pg_add_column("ALTER TABLE leaves ADD COLUMN request_date TEXT")

        # users: align fields
        if not _pg_has_column('users', 'role'):
            _pg_add_column("ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'staff'")
        if not _pg_has_column('users', 'is_active'):
            _pg_add_column("ALTER TABLE users ADD COLUMN is_active INTEGER DEFAULT 1")

        # staff: dob
        if not _pg_has_column('staff', 'dob'):
            _pg_add_column("ALTER TABLE staff ADD COLUMN dob TEXT")

        # tasks: created_by_staff_id
        if not _pg_has_column('tasks', 'created_by_staff_id'):
            _pg_add_column("ALTER TABLE tasks ADD COLUMN created_by_staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL")

    else:
        sqlite_schema = """CREATE TABLE IF NOT EXISTS public_holidays (id INTEGER PRIMARY KEY, date TEXT NOT NULL, name TEXT);
CREATE TABLE IF NOT EXISTS staff (id INTEGER PRIMARY KEY, name TEXT NOT NULL, rank TEXT NOT NULL, email TEXT UNIQUE, phone TEXT, section TEXT, role TEXT, grade TEXT, join_date TEXT, dob TEXT);
CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, staff_id INTEGER, username TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL, is_admin INTEGER DEFAULT 0, role TEXT DEFAULT 'staff', is_active INTEGER DEFAULT 1);
CREATE TABLE IF NOT EXISTS projects (id INTEGER PRIMARY KEY, code TEXT UNIQUE NOT NULL, name TEXT NOT NULL, client TEXT, location TEXT, rebar_strength REAL, concrete_strength REAL, target_slump_min REAL, target_slump_max REAL, supervisor_staff_id INTEGER, start_date TEXT, end_date TEXT);
CREATE TABLE IF NOT EXISTS project_staff (id INTEGER PRIMARY KEY, project_id INTEGER NOT NULL, staff_id INTEGER NOT NULL, role TEXT, UNIQUE(project_id,staff_id));
CREATE TABLE IF NOT EXISTS buildings (id INTEGER PRIMARY KEY, project_id INTEGER NOT NULL, name TEXT NOT NULL, floors INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS documents (id INTEGER PRIMARY KEY, project_id INTEGER NOT NULL, building_id INTEGER, category TEXT NOT NULL, file_path TEXT NOT NULL, uploaded_at TEXT NOT NULL, uploader_staff_id INTEGER);
CREATE TABLE IF NOT EXISTS biweekly_reports (id INTEGER PRIMARY KEY, project_id INTEGER NOT NULL, report_date TEXT NOT NULL, file_path TEXT, uploader_staff_id INTEGER);
CREATE TABLE IF NOT EXISTS tasks (id INTEGER PRIMARY KEY, title TEXT NOT NULL, description TEXT, date_assigned TEXT NOT NULL, days_allotted INTEGER NOT NULL, due_date TEXT NOT NULL, project_id INTEGER, created_by_staff_id INTEGER);
CREATE TABLE IF NOT EXISTS task_assignments (id INTEGER PRIMARY KEY, task_id INTEGER NOT NULL, staff_id INTEGER NOT NULL, status TEXT DEFAULT 'In progress', completed_date TEXT, days_taken INTEGER);
CREATE TABLE IF NOT EXISTS task_documents (id INTEGER PRIMARY KEY, task_id INTEGER NOT NULL, file_path TEXT NOT NULL, original_name TEXT, uploaded_at TEXT NOT NULL, uploader_staff_id INTEGER);
CREATE TABLE IF NOT EXISTS reminders_sent (id INTEGER PRIMARY KEY, assignment_id INTEGER NOT NULL, reminder_type TEXT NOT NULL, sent_on TEXT NOT NULL, UNIQUE(assignment_id, reminder_type, sent_on));
CREATE TABLE IF NOT EXISTS leaves (id INTEGER PRIMARY KEY, staff_id INTEGER NOT NULL, leave_type TEXT NOT NULL, start_date TEXT NOT NULL, end_date TEXT NOT NULL, working_days INTEGER DEFAULT 0, relieving_staff_id INTEGER, status TEXT DEFAULT 'Pending', reason TEXT, request_date TEXT, approved_by_staff_id INTEGER);
CREATE TABLE IF NOT EXISTS test_results (id INTEGER PRIMARY KEY, project_id INTEGER NOT NULL, building_id INTEGER, stage TEXT, test_type TEXT NOT NULL, batch_id TEXT, file_path TEXT NOT NULL, uploaded_at TEXT NOT NULL, uploader_staff_id INTEGER);
CREATE TABLE IF NOT EXISTS points (id INTEGER PRIMARY KEY, staff_id INTEGER NOT NULL, source TEXT NOT NULL, source_id INTEGER NOT NULL, points INTEGER NOT NULL, awarded_at TEXT NOT NULL, UNIQUE(staff_id, source, source_id));
"""
        _exec_script(cur, sqlite_schema)

        # SQLite migrations
        try:
            cols = [r[1] for r in cur.execute("PRAGMA table_info(staff)").fetchall()]
            if "dob" not in cols:
                cur.execute("ALTER TABLE staff ADD COLUMN dob TEXT")
        except Exception:
            pass

        try:
            cols = [r[1] for r in cur.execute("PRAGMA table_info(users)").fetchall()]
            if "role" not in cols:
                cur.execute("ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'staff'")
            if "is_active" not in cols:
                cur.execute("ALTER TABLE users ADD COLUMN is_active INTEGER DEFAULT 1")
        except Exception:
            pass

        try:
            tcols = [r[1] for r in cur.execute("PRAGMA table_info(tasks)").fetchall()]
            if "created_by_staff_id" not in tcols:
                cur.execute("ALTER TABLE tasks ADD COLUMN created_by_staff_id INTEGER")
        except Exception:
            pass

    c.commit()
    c.close()

    # Bootstrap admin (only if users table is empty)
    try:
        ucnt = fetch_df("SELECT COUNT(1) AS n FROM users")
        n = int(ucnt.iloc[0]["n"]) if not ucnt.empty else 0
        if n == 0:
            sid = execute(
                "INSERT INTO staff (name,rank,email,phone,section,role,grade,join_date) VALUES (?,?,?,?,?,?,?,?)",
                ("Admin", "Assistant Director", "", "", "", "admin", "", ""),
            )
            execute(
                "INSERT INTO users (staff_id,username,password_hash,is_admin,role,is_active) VALUES (?,?,?,?,?,?)",
                (sid, "admin", hash_pwd("fcda"), 1, "admin", 1),
            )
    except Exception:
        pass
def fetch_df(q, p=()):
    q=_adapt_query(q)
    c=get_conn()
    try:
        if DB_IS_POSTGRES:
            with c.cursor() as cur:
                cur.execute(q, p or ())
                cols=[d[0] for d in (cur.description or [])]
                rows=cur.fetchall() if cur.description else []
            return pd.DataFrame(rows, columns=cols)
        else:
            df=pd.read_sql_query(q, c, params=p)
            return df
    finally:
        try: c.close()
        except Exception: pass

def execute(q, p=()):
    q=_adapt_query(q).strip()
    c=get_conn()
    try:
        if DB_IS_POSTGRES:
            with c:
                with c.cursor() as cur:
                    low=q.lower()
                    if low.startswith("insert") and "returning" not in low:
                        q2=q.rstrip().rstrip(";")+" RETURNING id"
                        cur.execute(q2, p)
                        row=cur.fetchone()
                        return int(row[0]) if row else None
                    cur.execute(q, p)
                    if "returning" in low:
                        row=cur.fetchone()
                        return int(row[0]) if row else None
                    return None
        else:
            cur=c.cursor()
            cur.execute(q, p)
            c.commit()
            return cur.lastrowid
    finally:
        try: c.close()
        except Exception: pass


# ---------- Notifications (Email Reminders) ----------
def smtp_configured()->bool:
    return bool(os.getenv("SMTP_HOST") and os.getenv("SMTP_USER") and os.getenv("SMTP_PASSWORD"))

def send_email(to_email:str, subject:str, body:str)->tuple[bool,str]:
    """Send plain-text email via SMTP. Requires env vars:
    SMTP_HOST, SMTP_PORT (default 587), SMTP_USER, SMTP_PASSWORD, SMTP_FROM (optional), SMTP_TLS (default 1).
    """
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
    """Checks due-soon and overdue tasks and sends email reminders once per day per assignment + type."""
    if today is None: today=date.today()
    today_s=str(today)

    df=fetch_df("""
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

        # dedupe per day
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
        body_lines += [
            "",
            "Please log into WorkNest to review the task details and attachments.",
            "",
            "— WorkNest"
        ]
        ok,msg=send_email(str(r.get("staff_email") or "").strip(), subj, "\n".join(body_lines))
        if ok:
            sent += 1
            execute("INSERT OR IGNORE INTO reminders_sent (assignment_id, reminder_type, sent_on) VALUES (?,?,?)",
                    (int(r["assignment_id"]), rtype, today_s))
        else:
            # We only log as 'sent' if actually sent; otherwise keep it eligible.
            if msg=="missing recipient" or msg=="SMTP not configured":
                skipped += 1
            else:
                errors += 1
    return {"checked":int(len(df)),"sent":sent,"skipped":skipped,"errors":errors}


def apply_styles():
    st.markdown("""<style>
    .worknest-header{background:linear-gradient(90deg,#00B09B,#96C93D);color:#fff;padding:12px 16px;border-radius:14px}
    .pill{display:inline-block;padding:2px 10px;border-radius:999px;background:#eef}
    </style>""", unsafe_allow_html=True)

def current_user(): return st.session_state.get("user")
def user_role():
    u=current_user()
    if not u: return None
    r=(u.get('role') or '').strip()
    if r: return r
    # Backward compatibility
    return 'admin' if int(u.get('is_admin',0) or 0)==1 else 'staff'

def is_admin():
    return user_role()=='admin' or int((current_user() or {}).get('is_admin',0) or 0)==1

def is_sub_admin():
    return user_role() in ('admin','sub_admin')

def is_section_head():
    return user_role() in ('admin','sub_admin','section_head')

def can_import_csv():
    return is_admin()

def can_manage_projects():
    # create/edit/delete projects
    return is_admin()

def can_upload_core_docs():
    # "core" project documents (drawings, approvals, etc.)
    return is_sub_admin()

def can_assign_tasks():
    # create/assign tasks
    return is_section_head()

def can_approve_leave():
    return is_admin()

def current_staff_id():
    u=current_user()
    if not u: return None
    sid=u.get("staff_id")
    try: return int(sid) if sid is not None else None
    except: return None

# ---------- Auth ----------
def login_ui():
    st.markdown(f"<h2 style='text-align:center'>{APP_TITLE}</h2>", unsafe_allow_html=True)
    st.caption("Login with staff <b>email</b> (preferred) or <b>name</b>. Default password is <b>fcda</b>.", unsafe_allow_html=True)
    username=st.text_input("Username (email or name)", key="login_user")
    password=st.text_input("Password", type="password", key="login_pwd")
    if st.button("Login", key="login_btn"):
        u=fetch_df("SELECT * FROM users WHERE username=?", (username,))
        if (not u.empty) and int(u["is_active"].iloc[0] if "is_active" in u.columns else 1)==1 and u["password_hash"].iloc[0]==hash_pwd(password):
            st.session_state["user"]=dict(u.iloc[0]); st.rerun()
        else:
            s=fetch_df("SELECT id,name,email,rank FROM staff WHERE (email=? COLLATE NOCASE) OR (name=?)", (username, username))
            if s.empty:
                st.error("User not found. Ask Admin to add you in Staff, then login using your email or name.")
            else:
                sid=int(s["id"].iloc[0])
                uname=s["email"].iloc[0] if pd.notna(s["email"].iloc[0]) else s["name"].iloc[0]
                ex=fetch_df("SELECT id FROM users WHERE username=?", (uname,))
                if ex.empty:
                    adm=0  # new users are staff by default; Admin promotes via Access Control
                    execute("INSERT INTO users (staff_id, username, password_hash, is_admin, role, is_active) VALUES (?,?,?,?,?,?)", (sid, uname, hash_pwd("fcda"), adm, ("admin" if adm==1 else "staff"), 1))
                u=fetch_df("SELECT * FROM users WHERE username=?", (uname,))
                if (not u.empty) and int(u["is_active"].iloc[0] if "is_active" in u.columns else 1)==1 and u["password_hash"].iloc[0]==hash_pwd(password):
                    st.session_state["user"]=dict(u.iloc[0]); st.rerun()
                else:
                    st.error("Wrong password. Default is 'fcda' unless changed.")

def logout_button():
    if st.sidebar.button("🚪 Logout", key="logout_btn"):
        st.session_state.pop("user", None); st.rerun()


def sidebar_nav():
    u=current_user()
    st.sidebar.title("📚 Navigation")
    if u: st.sidebar.markdown(f"**User:** {u['username']}  \\n**Role:** {user_role()}")
    logout_button()

    base_pages=["🏠 Dashboard","🏗️ Projects","🗂️ Tasks & Performance","🧳 Leave"]
    admin_pages=["👥 Staff","📄 Leave Table","⬆️ Import CSVs","🔐 Access Control"]
    pages = base_pages + (admin_pages if is_admin() else [])

    return st.sidebar.radio("Go to", pages, key="nav_radio")

# ---------- Helpers ----------

def is_assigned_to_task(task_id:int, staff_id:int|None=None)->bool:
    sid = staff_id if staff_id is not None else current_staff_id()
    if sid is None: return False
    df=fetch_df("SELECT 1 FROM task_assignments WHERE task_id=? AND staff_id=?", (int(task_id), int(sid)))
    return (not df.empty)


def can_upload_project_outputs(project_id:int)->bool:
    # reports + test results: admin can upload anywhere; staff only where assigned
    if is_admin(): return True
    return is_assigned_to_project(project_id)

def can_upload_task_files(task_row:dict)->bool:
    # task attachments: admin always; task creator or assignee can upload
    if is_admin(): return True
    sid=current_staff_id()
    if sid is None: return False
    try:
        tid=int(task_row.get("id"))
    except Exception:
        return False
    # Creator
    try:
        if int(task_row.get("created_by_staff_id") or -1)==sid:
            return True
    except Exception:
        pass
    # Any assignee
    return is_assigned_to_task(tid, sid)

def can_download_task_files(task_row:dict)->bool:
    # same as upload for now
    return can_upload_task_files(task_row) or is_section_head()

def can_upload_core_to_project(project_id:int)->bool:
    # core docs: admin + sub-admin only
    return can_upload_core_docs()


def save_uploaded_file(uploaded_file, subfolder=""):
    if uploaded_file is None: return None
    fname=f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uploaded_file.name}"
    folder=os.path.join(UPLOAD_DIR, subfolder) if subfolder else UPLOAD_DIR
    os.makedirs(folder, exist_ok=True)
    path=os.path.join(folder, fname)
    with open(path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    return path

def file_download_button(label, file_path, key):
    try:
        with open(file_path, "rb") as f:
            data=f.read()
        st.download_button(label, data=data, file_name=os.path.basename(file_path), key=key)
    except Exception as e:
        st.error(f"Missing file: {file_path}")

# ---------- Dashboard ----------
def page_dashboard():
    st.markdown(f"<div class='worknest-header'><h2>🏠 {APP_TITLE} — Dashboard</h2></div>", unsafe_allow_html=True)
    col1,col2,col3=st.columns(3)
    projects=fetch_df("SELECT * FROM projects")
    staff=fetch_df("SELECT * FROM staff")
    open_tasks=fetch_df("SELECT * FROM task_assignments WHERE status!='Completed'")
    col1.metric("Projects", len(projects))
    col2.metric("Staff", len(staff))
    col3.metric("Open Tasks", len(open_tasks))

    def project_core_docs_status(pid):
        df=fetch_df("SELECT DISTINCT category FROM documents WHERE project_id=?", (pid,))
        present=set(df["category"]) if not df.empty else set()
        missing=[c for c in CORE_DOC_CATEGORIES if c not in present]
        return present, missing

    def project_next_due(pid, start_date):
        # start_date may come from pandas as NaN/None/empty; treat all as missing.
        if start_date is None:
            return (True, None, None, "Start date missing — cannot track biweekly schedule")
        try:
            if (isinstance(start_date, float) and pd.isna(start_date)):
                return (True, None, None, "Start date missing — cannot track biweekly schedule")
            sd = str(start_date).strip()
            if sd == "" or sd.lower() in ("nan", "none", "null"):
                return (True, None, None, "Start date missing — cannot track biweekly schedule")
            start = dtparser.parse(sd).date()
        except Exception:
            return (True, None, None, f"Invalid start date '{start_date}' — cannot track biweekly schedule")

        last = fetch_df("SELECT MAX(report_date) d FROM biweekly_reports WHERE project_id=?", (pid,))
        last_raw = None
        if (not last.empty) and ("d" in last.columns):
            last_raw = last["d"].iloc[0]

        last_d = None
        try:
            if last_raw is not None and not (isinstance(last_raw, float) and pd.isna(last_raw)) and str(last_raw).strip().lower() not in ("", "nan", "none", "null"):
                last_d = dtparser.parse(str(last_raw)).date()
        except Exception:
            last_d = None  # if stored value is junk, ignore and compute from start date

        exp = (start + timedelta(days=14)) if (last_d is None) else (last_d + timedelta(days=14))
        return (date.today() > exp, last_d, exp, None)


    flags=[]
    for _,p in projects.iterrows():
        pid=int(p["id"])
        present,missing=project_core_docs_status(pid)
        if missing:
            flags.append({"Project":f"{p['code']} — {p['name']}", "Issue":"Missing docs: "+", ".join(missing)})
        overdue,last_d,exp,reason = project_next_due(pid, p.get("start_date"))
        if reason:
            flags.append({"Project":f"{p['code']} — {p['name']}", "Issue":reason})
        elif overdue:
            if last_d is None:
                flags.append({"Project":f"{p['code']} — {p['name']}", "Issue":f"Biweekly overdue — no report yet (expected by {exp})"})
            else:
                flags.append({"Project":f"{p['code']} — {p['name']}", "Issue":f"Biweekly overdue — last on {last_d} (expected by {exp})"})

    st.subheader("🚩 Red Flags")
    st.dataframe(pd.DataFrame(flags) if flags else pd.DataFrame([{"Project":"—","Issue":"No red flags"}]), width='stretch')

# ---------- Projects ----------
def page_projects():
    st.markdown("<div class='worknest-header'><h2>🏗️ Projects</h2></div>", unsafe_allow_html=True)
    projects=fetch_df("""
        SELECT p.id, p.code, p.name, p.client, p.location, p.start_date, p.end_date, p.supervisor_staff_id,
               (SELECT name FROM staff s WHERE s.id=p.supervisor_staff_id) supervisor
        FROM projects p ORDER BY p.code
    """)
    left,right=st.columns([1,2])
    with left:
        st.subheader("Project List")
        if projects.empty:
            st.info("No projects yet. Use the form on the right to add one.")
            selected=None
        else:
            labels=[f"{r['code']} — {r['name']}" for _,r in projects.iterrows()]
            selected_label=st.selectbox("Select a project", labels, key="proj_select")
            selected=projects.iloc[labels.index(selected_label)] if labels else None
    with right:
        st.subheader("Create / Update Project")
        if not can_manage_projects():
            st.info("Only Admin can create/update/delete projects.")

        staff=fetch_df("SELECT id,name FROM staff ORDER BY name")
        sup_names=["—"]+[s for s in staff["name"].tolist()] if not staff.empty else ["—"]
        code=st.text_input("Code", value=(selected["code"] if selected is not None else ""), key="proj_code")
        name=st.text_input("Name", value=(selected["name"] if selected is not None else ""), key="proj_name")
        client=st.text_input("Client", value=(selected["client"] if selected is not None and pd.notna(selected["client"]) else ""), key="proj_client")
        location=st.text_input("Location", value=(selected["location"] if selected is not None and pd.notna(selected["location"]) else ""), key="proj_loc")
        start=st.date_input("Start Date", value=(dtparser.parse(selected["start_date"]).date() if selected is not None and pd.notna(selected["start_date"]) else date.today()), key="proj_start")
        end=st.date_input("End Date", value=(dtparser.parse(selected["end_date"]).date() if selected is not None and pd.notna(selected["end_date"]) else date.today()), key="proj_end")
        sup_default = selected["supervisor"] if (selected is not None and pd.notna(selected["supervisor"])) else "—"
        sup_name=st.selectbox("Supervisor", sup_names, index=sup_names.index(sup_default) if sup_default in sup_names else 0, key="proj_sup")
        colA,colB=st.columns(2)
        with colA:
            if can_manage_projects() and st.button("💾 Save / Update", key="proj_save"):
                if selected is None:
                    sup_id=None
                    if sup_name!="—": sup_id=int(staff[staff["name"]==sup_name]["id"].iloc[0])
                    execute("""INSERT INTO projects (code,name,client,location,start_date,end_date,supervisor_staff_id)
                               VALUES (?,?,?,?,?,?,?)""", (code,name,client or None,location or None,str(start),str(end),sup_id))
                    st.success("Project created.")
                else:
                    sup_id=None
                    if sup_name!="—": sup_id=int(staff[staff["name"]==sup_name]["id"].iloc[0])
                    execute("""UPDATE projects SET code=?,name=?,client=?,location=?,start_date=?,end_date=?,supervisor_staff_id=? WHERE id=?""",
                            (code,name,client or None,location or None,str(start),str(end),sup_id,int(selected["id"])))
                    st.success("Project updated.")
                st.rerun()
        with colB:
            if (selected is not None) and st.button("🗑️ Delete", key="proj_del"):
                execute("DELETE FROM projects WHERE id=?", (int(selected["id"]),))
                st.success("Project deleted."); st.rerun()

    if selected is not None:
        pid=int(selected["id"])
        st.markdown("### Posted Staff")
        df=fetch_df("""
            SELECT s.name, s.rank, ps.role
            FROM project_staff ps JOIN staff s ON s.id=ps.staff_id
            WHERE ps.project_id=? ORDER BY s.rank, s.name
        """,(pid,))
        st.dataframe(df if not df.empty else pd.DataFrame(columns=["name","rank","role"]), width='stretch')

        st.markdown("---")
        tabs = st.tabs(["🏢 Buildings","📄 Core Docs","🧪 Tests","📝 Biweekly Reports"])

        # Buildings
        with tabs[0]:
            bdf=fetch_df("SELECT id,name,floors FROM buildings WHERE project_id=? ORDER BY name",(pid,))
            st.subheader("Buildings")
            st.dataframe(bdf if not bdf.empty else pd.DataFrame(columns=["id","name","floors"]), width='stretch')
            st.markdown("**Add / Edit Building**")
            names = ["— New —"] + (bdf["name"].tolist() if not bdf.empty else [])
            pick = st.selectbox("Choose building", names, key="b_pick")
            if pick=="— New —":
                b_name = st.text_input("Building name", key="b_name_new")
                floors = st.number_input("Floors", 0, 200, 0, key="b_f_new")
                if st.button("➕ Add Building", key="b_add"):
                    execute("INSERT INTO buildings (project_id,name,floors) VALUES (?,?,?)",(pid,b_name,int(floors)))
                    st.success("Building added."); st.rerun()
            else:
                brow = bdf[bdf["name"]==pick].iloc[0]
                b_name = st.text_input("Building name", value=brow["name"], key="b_name_edit")
                floors = st.number_input("Floors", 0, 200, int(brow["floors"]), key="b_f_edit")
                colx,coly=st.columns(2)
                with colx:
                    if st.button("💾 Save Building", key="b_save"):
                        execute("UPDATE buildings SET name=?, floors=? WHERE id=?", (b_name,int(floors),int(brow["id"])))
                        st.success("Building updated."); st.rerun()
                with coly:
                    if st.button("🗑️ Delete Building", key="b_del"):
                        execute("DELETE FROM buildings WHERE id=?", (int(brow["id"]),))
                        st.success("Building deleted."); st.rerun()

        # Core Documents upload/list
        with tabs[1]:
            st.subheader("Core Documents")
            c1,c2=st.columns(2)
            with c1:
                st.caption("Required categories: " + ", ".join(CORE_DOC_CATEGORIES))
            with c2:
                allowed = can_upload_core_to_project(pid)
                st.markdown(f"Upload permission: <span class='pill'>{'Yes' if allowed else 'No'}</span>", unsafe_allow_html=True)
            if allowed:
                cat = st.selectbox("Category", CORE_DOC_CATEGORIES, key="doc_cat")
                up = st.file_uploader("Upload file (PDF/Image)", type=["pdf","png","jpg","jpeg"], key="doc_file")
                if st.button("⬆️ Upload Document", key="doc_up"):
                    path=save_uploaded_file(up, f"project_{pid}/docs")
                    if path:
                        execute("""INSERT INTO documents (project_id, building_id, category, file_path, uploaded_at, uploader_staff_id)
                                   VALUES (?,?,?,?,?,?)""",(pid, None, cat, path, datetime.now().isoformat(timespec="seconds"), current_staff_id()))
                        st.success("Document uploaded.")
                    else:
                        st.error("Select a file first.")
            ddf=fetch_df("SELECT id,category,file_path,uploaded_at FROM documents WHERE project_id=? ORDER BY uploaded_at DESC",(pid,))
            if ddf.empty:
                st.info("No documents yet.")
            else:
                for _,r in ddf.iterrows():
                    colA,colB=st.columns([3,1])
                    with colA: st.write(f"**{r['category']}** — {os.path.basename(r['file_path'])}  \n*{r['uploaded_at']}*")
                    with colB: file_download_button("⬇️ Download", r["file_path"], key=f"docdl{r['id']}")

        # Tests upload/list
        with tabs[2]:
            st.subheader("Test Results (per building & stage)")
            bdf=fetch_df("SELECT id,name FROM buildings WHERE project_id=? ORDER BY name",(pid,))
            b_opts = ["— (no specific building) —"] + (bdf["name"].tolist() if not bdf.empty else [])
            b_pick = st.selectbox("Building", b_opts, key="t_building")
            bid = None
            if b_pick!="— (no specific building) —" and (not bdf.empty):
                bid = int(bdf[bdf["name"]==b_pick]["id"].iloc[0])

            stage = st.selectbox("Stage", STAGES, key="t_stage")
            ttype_label = st.selectbox("Test Type", [x[1] for x in TEST_TYPES_DISPLAY], key="t_type")
            ttype = [k for k,v in TEST_TYPES_DISPLAY if v==ttype_label][0]
            batch_needed = (ttype in ["steel","reinforcement"])
            batch_id = st.text_input("Batch ID (required for batch tests)", key="t_batch") if batch_needed else None

            allowed = can_upload_project_outputs(pid)
            st.markdown(f"Upload permission: <span class='pill'>{'Yes' if allowed else 'No'}</span>", unsafe_allow_html=True)

            up = st.file_uploader("Upload test result file (PDF/Image)", type=["pdf","png","jpg","jpeg"], key="t_file")
            if st.button("⬆️ Upload Test", key="t_upload"):
                if not allowed:
                    st.error("You don't have permission to upload to this project.")
                else:
                    if batch_needed and (not batch_id or not batch_id.strip()):
                        st.error("Batch ID is required for steel/reinforcement tests.")
                    else:
                        path=save_uploaded_file(up, f"project_{pid}/tests")
                        if path:
                            execute("""INSERT INTO test_results (project_id,building_id,stage,test_type,batch_id,file_path,uploaded_at,uploader_staff_id)
                                       VALUES (?,?,?,?,?,?,?,?)""",(pid, bid, stage, ttype, batch_id, path, datetime.now().isoformat(timespec="seconds"), current_staff_id()))
                            st.success("Test uploaded.")
                        else:
                            st.error("Select a file first.")

            # List
            tdf=fetch_df("""
                SELECT tr.id, b.name AS building, tr.stage, tr.test_type, tr.batch_id, tr.file_path, tr.uploaded_at
                FROM test_results tr
                LEFT JOIN buildings b ON b.id=tr.building_id
                WHERE tr.project_id=?
                ORDER BY tr.uploaded_at DESC
            """,(pid,))
            if tdf.empty:
                st.info("No tests uploaded yet.")
            else:
                for _,r in tdf.iterrows():
                    colA,colB,colC=st.columns([3,1,1])
                    bname = r["building"] if pd.notna(r["building"]) else "—"
                    lab = r["test_type"].capitalize()
                    if r["test_type"] in ["steel","reinforcement"] and pd.notna(r["batch_id"]):
                        lab += f" (Batch: {r['batch_id']})"
                    with colA: st.write(f"**{lab}** — Building: {bname} — Stage: {r['stage']}  \n*{r['uploaded_at']}*")
                    with colB: file_download_button("⬇️ Download", r["file_path"], key=f"tdl{r['id']}")
                    with colC:
                        if is_admin() and st.button("🗑️", key=f"tdel{r['id']}"):
                            execute("DELETE FROM test_results WHERE id=?", (int(r["id"]),))
                            st.experimental_rerun()

        # Biweekly Reports
        with tabs[3]:
            st.subheader("Biweekly Reports")
            allowed = can_upload_project_outputs(pid)
            st.markdown(f"Upload permission: <span class='pill'>{'Yes' if allowed else 'No'}</span>", unsafe_allow_html=True)
            rdate = st.date_input("Report Date", value=date.today(), key="bw_date")
            up = st.file_uploader("Upload biweekly report (PDF/Image)", type=["pdf","png","jpg","jpeg"], key="bw_file")
            if st.button("⬆️ Upload Report", key="bw_up"):
                if not allowed:
                    st.error("You don't have permission to upload to this project.")
                else:
                    path=save_uploaded_file(up, f"project_{pid}/reports")
                    if path:
                        rid = execute("INSERT INTO biweekly_reports (project_id,report_date,file_path,uploader_staff_id) VALUES (?,?,?,?)",
                                      (pid, str(rdate), path, current_staff_id()))
                        st.success("Report uploaded.")
                        sid = current_staff_id()
                        if sid is not None:
                            try:
                                execute("INSERT OR IGNORE INTO points (staff_id, source, source_id, points, awarded_at) VALUES (?,?,?,?,?)",
                                        (int(sid), "biweekly", int(rid), 5, datetime.now().isoformat(timespec="seconds")))
                            except Exception:
                                pass
                        posted = fetch_df("SELECT staff_id FROM project_staff WHERE project_id=?", (pid,))
                        if not posted.empty:
                            for _,pr in posted.iterrows():
                                try:
                                    execute("INSERT OR IGNORE INTO points (staff_id, source, source_id, points, awarded_at) VALUES (?,?,?,?,?)",
                                            (int(pr["staff_id"]), "biweekly", int(rid), 5, datetime.now().isoformat(timespec="seconds")))
                                except Exception:
                                    pass
                    else:
                        st.error("Select a file first.")
            rdf=fetch_df("SELECT id,report_date,file_path FROM biweekly_reports WHERE project_id=? ORDER BY date(report_date) DESC",(pid,))
            if rdf.empty:
                st.info("No reports yet.")
            else:
                for _,r in rdf.iterrows():
                    colA,colB=st.columns([3,1])
                    with colA: st.write(f"**{r['report_date']}** — {os.path.basename(r['file_path'])}")
                    with colB: file_download_button("⬇️ Download", r["file_path"], key=f"bw{r['id']}")

# ---------- Staff ----------
def page_staff():
    st.markdown("<div class='worknest-header'><h2>👥 Staff</h2></div>", unsafe_allow_html=True)
    staff=fetch_df("SELECT id,name,rank,email,section FROM staff ORDER BY name")
    if staff.empty:
        st.info("No staff yet. Import from CSVs or add directly via DB.")
        return
    names=[r["name"] for _,r in staff.iterrows()]
    sel=st.selectbox("Select staff", names, key="staff_pick")
    srow=staff[staff["name"]==sel].iloc[0]
    st.markdown(f"**Name:** {srow['name']}  \n**Rank:** {srow['rank']}  \n**Email:** {srow['email'] or '—'}  \n**Section:** {srow['section'] or '—'}")
    st.markdown("**Projects posted on:**")
    df=fetch_df("""
        SELECT p.code AS project_code, p.name AS project_name, COALESCE(ps.role,'Staff') AS role
        FROM project_staff ps JOIN projects p ON p.id=ps.project_id
        WHERE ps.staff_id=? ORDER BY p.code
    """,(int(srow["id"]),))
    st.dataframe(df if not df.empty else pd.DataFrame(columns=["project_code","project_name","role"]), width='stretch')

# ---------- Leave ----------
def working_days_between(start, end, holidays):
    if end < start: return 0
    d=start; H=set(holidays); days=0
    while d<=end:
        if d.weekday()<5 and d not in H: days+=1
        d+=timedelta(days=1)
    return days

def add_working_days(start, n, holidays, cap_dec31=True):
    if n<=1: return start
    d=start; count=1; H=set(holidays)
    last=date(start.year,12,31) if cap_dec31 else None
    while count<n:
        d+=timedelta(days=1)
        if last and d>last: return last
        if d.weekday()<5 and d not in H: count+=1
    return d

def page_leave():
    st.markdown("<div class='worknest-header'><h2>🧳 Leave</h2></div>", unsafe_allow_html=True)
    staff_df=fetch_df("SELECT id,name,rank FROM staff ORDER BY name")
    hol_df=fetch_df("SELECT date FROM public_holidays")
    holidays=[dtparser.parse(x).date() for x in hol_df["date"].tolist()] if not hol_df.empty else []

    if staff_df.empty:
        st.info("Add staff first."); return

    colA,colB=st.columns([2,1])
    with colA:
        if is_admin():
            staff_opt=st.selectbox("Applicant", staff_df["name"].tolist(), key="lv_app")
            srow=staff_df[staff_df["name"]==staff_opt].iloc[0]
        else:
            sid=current_staff_id()
            if sid is None:
                st.error("No staff profile linked to this account."); return
            srow=staff_df[staff_df["id"]==int(sid)].iloc[0]
            st.write(f"Applicant: **{srow['name']}**")
        ltype=st.selectbox("Type", ["Annual","Casual","Sick","Maternity","Paternity","Other"], key="lv_type")
        start=st.date_input("Start Date", value=date.today(), key="lv_start")

        yr=start.year
        casual_taken_row=fetch_df("SELECT SUM(working_days) d FROM leaves WHERE staff_id=? AND leave_type='Casual' AND substr(start_date,1,4)=?",
                                  (int(srow["id"]), str(yr)))
        taken_so_far=int(casual_taken_row["d"].iloc[0]) if (not casual_taken_row.empty and pd.notna(casual_taken_row["d"].iloc[0])) else 0
        casual_remaining=max(0, 14-taken_so_far)

        max_days=None; cap_dec31=True; force_days=None
        if ltype=="Annual":
            max_days=30; cap_dec31=True
        elif ltype=="Casual":
            max_days=casual_remaining; cap_dec31=True
        elif ltype=="Paternity":
            force_days=14; cap_dec31=True
        elif ltype=="Maternity":
            force_days=112; cap_dec31=False
        else:
            max_days=30

        if force_days is not None:
            req=force_days; st.write(f"Working days (fixed): **{force_days}**")
        else:
            if ltype=="Casual" and max_days==0:
                st.warning(f"You have used all 14 casual leave days in {yr}.")
            req=st.number_input("Requested working days", min_value=0 if ltype=="Casual" else 1,
                                max_value=max_days if (max_days and max_days>0) else 60,
                                value=min(5, max_days or 5), key="lv_req")

        end=add_working_days(start, int(req if req else 0), holidays, cap_dec31=cap_dec31)
        st.write(f"Computed End Date: **{end}**")

    with colB:
        st.markdown("**Casual Balance**")
        st.metric(label=f"{start.year} casual remaining", value=f"{casual_remaining} days")

    # --- Reliever enforcement (relaxed for future-year planning and unknown ranks) ---
    all_staff=fetch_df("SELECT id,name,rank FROM staff ORDER BY name")
    all_leaves=fetch_df("SELECT staff_id, relieving_staff_id, start_date, end_date, status FROM leaves")

    def is_on_leave(sid, s, e):
        if all_leaves.empty: return False
        for _,L in all_leaves.iterrows():
            if int(L["staff_id"])==int(sid):
                try:
                    Ls=dtparser.parse(L["start_date"]).date(); Le=dtparser.parse(L["end_date"]).date()
                except: continue
                if (s<=Le and Ls<=e) and (str(L.get("status","Pending"))!="Rejected"):
                    return True
        return False

    def is_already_relieving(sid, s, e):
        if all_leaves.empty: return False
        for _,L in all_leaves.iterrows():
            if pd.notna(L["relieving_staff_id"]) and int(L["relieving_staff_id"])==int(sid):
                try:
                    Ls=dtparser.parse(L["start_date"]).date(); Le=dtparser.parse(L["end_date"]).date()
                except: continue
                if (s<=Le and Ls<=e) and (str(L.get("status","Pending"))!="Rejected"):
                    return True
        return False

    planning_future_year = start.year > date.today().year
    enforce_nearest = not planning_future_year

    app_idx = rank_index_safe(srow["rank"])
    pool=[]
    for _,cand in all_staff.iterrows():
        if int(cand["id"])==int(srow["id"]): 
            continue
        if is_on_leave(int(cand["id"]), start, end): 
            continue
        if is_already_relieving(int(cand["id"]), start, end): 
            continue
        c_idx = rank_index_safe(cand["rank"])
        if enforce_nearest and (app_idx is not None and c_idx is not None):
            dist = abs(c_idx - app_idx)
        else:
            dist = 0
        pool.append((int(cand["id"]), cand["name"], cand["rank"], dist))

    if not pool:
        st.error("No available reliever found for the requested period. Adjust dates or add staff.")
        allowed_names=[]; nearest_names=[]
    else:
        min_dist=min(p[3] for p in pool)
        nearest=[p for p in pool if p[3]==min_dist]
        nearest_names=[p[1] for p in nearest]
        allowed_names=[p[1] for p in pool]
        cap_note=" (relaxed for future-year planning)" if planning_future_year else ""
        st.caption("Reliever must be nearest in rank and available" + cap_note + ".")

    reliever=st.selectbox("Relieving Officer", allowed_names, key="lv_rel", disabled=(len(allowed_names)==0))

    non_nearest_selected = (not planning_future_year) and (reliever and (reliever not in nearest_names))
    if non_nearest_selected:
        ch = [p for p in pool if p[1]==reliever][0]
        chosen_rank = ch[2]; chosen_dist = ch[3]
        nearest_dist = min(p[3] for p in pool) if pool else None
        st.warning(f"Selected reliever **{reliever} ({chosen_rank})** is not nearest in rank (distance={chosen_dist}). "
                   f"Nearest allowed distance is **{nearest_dist}**. Choose from: " + ", ".join(nearest_names))

    reason=st.text_area("Reason (optional)", key="lv_reason")
    wd=working_days_between(start, end, holidays)
    st.write(f"Working days in this request: **{wd}**")

    can_submit=True; msg=None
    if ltype=="Casual":
        remaining_after=max(0, casual_remaining - wd)
        st.info(f"Casual leave remaining after this request in {yr}: **{remaining_after}** working days")
        if wd>casual_remaining: can_submit=False; msg=f"Casual request exceeds remaining balance ({casual_remaining})."
        if end.year>yr: can_submit=False; msg="Casual leave end date cannot exceed 31st December."
    if ltype=="Paternity" and wd!=14: can_submit=False; msg="Paternity leave must be exactly 14 working days."
    if ltype=="Maternity" and wd!=112: can_submit=False; msg="Maternity leave must be exactly 112 working days."
    if ltype=="Annual" and wd>30: can_submit=False; msg="Annual leave exceeds 30 working days."
    if not reliever: can_submit=False; msg = msg or "No reliever selected."
    elif non_nearest_selected: can_submit=False; msg = msg or "You must select a nearest-in-rank reliever."
    else:
        chosen = [p for p in pool if p[1]==reliever]
        if chosen:
            ch_id = chosen[0][0]
            if is_on_leave(ch_id, start, end): can_submit=False; msg="Relieving officer is on leave in the requested period."
            if is_already_relieving(ch_id, start, end): can_submit=False; msg="Relieving officer is already assigned to relieve another staff in the requested period."

    if st.button("📝 Submit Leave Application", key="lv_submit"):
        if can_submit:
            reliever_id=int([p for p in pool if p[1]==reliever][0][0])
            execute("INSERT INTO leaves (staff_id,leave_type,start_date,end_date,working_days,relieving_staff_id,status,reason) VALUES (?,?,?,?,?,?,'Pending',?)",
                    (int(srow["id"]),ltype,str(start),str(end),int(wd),reliever_id,reason or None))
            st.success("Leave application submitted.")
        else:
            st.error(msg or "Validation failed.")

def page_leave_table():
    st.markdown("<div class='worknest-header'><h2>📄 Leave Table</h2></div>", unsafe_allow_html=True)
    df=fetch_df("""
        SELECT L.id, S.name AS staff, S.rank, L.leave_type, L.start_date, L.end_date, L.working_days,
               R.name AS reliever, L.status, L.reason
        FROM leaves L
        JOIN staff S ON S.id = L.staff_id
        LEFT JOIN staff R ON R.id = L.relieving_staff_id
        ORDER BY date(L.start_date) DESC, S.name
    """)
    if df.empty:
        st.info("No leave applications yet."); return
    c1,c2,c3=st.columns(3)
    with c1:
        staff_filter = st.selectbox("Filter by staff", ["All"] + sorted(df["staff"].unique().tolist()), key="lvf1")
    with c2:
        type_filter = st.selectbox("Filter by type", ["All"] + sorted(df["leave_type"].unique().tolist()), key="lvf2")
    with c3:
        years = sorted({dtparser.parse(d).year for d in df["start_date"]})
        year_filter = st.selectbox("Filter by year", ["All"] + [str(y) for y in years], key="lvf3")
    f = df.copy()
    if staff_filter!="All": f = f[f["staff"]==staff_filter]
    if type_filter!="All": f = f[f["leave_type"]==type_filter]
    if year_filter!="All": f = f[f["start_date"].str.startswith(year_filter)]
    st.dataframe(f.reset_index(drop=True), width='stretch')

# ---------- Tasks & Performance ----------
def _build_expected_biweekly_windows(start_date:date, today:date)->list:
    out=[]
    if not isinstance(start_date, date): return out
    cur=start_date
    while cur + timedelta(days=14) <= today:
        nxt=cur + timedelta(days=14)
        out.append((cur, nxt))
        cur=nxt
    return out

def page_tasks():
    st.markdown("<div class='worknest-header'><h2>🗂️ Tasks & Performance</h2></div>", unsafe_allow_html=True)

    st.markdown("### ⏰ Reminders")
    ass=fetch_df("""
        SELECT
            ta.id AS assignment_id,
            t.title,
            s.name AS staff,
            COALESCE(p.code || ' — ' || p.name, p.code, '—') AS project,
            t.due_date,
            ta.status
        FROM task_assignments ta
        JOIN tasks t ON t.id=ta.task_id
        JOIN staff s ON s.id=ta.staff_id
        LEFT JOIN projects p ON p.id=t.project_id
        WHERE ta.status!='Completed'
        ORDER BY date(t.due_date) ASC
    """)
    if ass.empty:
        st.caption("No open assignments, so no reminders.")
    else:
        ass["due_date"]=ass["due_date"].astype(str)
        ass["days_to_due"]=ass["due_date"].apply(lambda d: (dtparser.parse(d).date()-date.today()).days if d else None)
        due_soon=ass[(ass["days_to_due"].notna()) & (ass["days_to_due"]>=0) & (ass["days_to_due"]<=2)].copy()
        overdue=ass[(ass["days_to_due"].notna()) & (ass["days_to_due"]<0)].copy()

        c1,c2=st.columns(2)
        with c1:
            st.caption("Due soon (0–2 days)")
            st.dataframe(due_soon[["project","title","staff","due_date","days_to_due"]] if not due_soon.empty else pd.DataFrame(columns=["project","title","staff","due_date","days_to_due"]),
                         width='stretch')
        with c2:
            st.caption("Overdue")
            st.dataframe(overdue[["project","title","staff","due_date","days_to_due"]] if not overdue.empty else pd.DataFrame(columns=["project","title","staff","due_date","days_to_due"]),
                         width='stretch')

        if is_admin():
            st.caption("Email reminders are optional. Configure SMTP_* env vars to enable sending.")
            if st.button("📨 Run reminder email check now", key="run_reminders_now"):
                stats=run_task_reminders()
                if smtp_configured():
                    st.success(f"Checked {stats['checked']} assignments. Sent {stats['sent']} emails. Skipped {stats['skipped']}. Errors {stats['errors']}.")
                else:
                    st.warning("SMTP is not configured, so no emails were sent. In-app reminders above still work.")
    staff=fetch_df("SELECT id,name FROM staff ORDER BY name")
    projects=fetch_df("SELECT id,code,name,start_date FROM projects ORDER BY code")
    st.subheader("Create / Edit Task")
    titles=fetch_df("SELECT id,title FROM tasks ORDER BY id DESC")
    mode=st.radio("Mode", ["Create new","Edit existing"], horizontal=True, key="tsk_mode")
    if (not can_assign_tasks()) and mode=="Create new":
        st.info("Only Admin and Sectional Heads can create/assign tasks. Switching to view/edit attachments mode.")
        mode="Edit existing"

    if mode=="Edit existing" and titles.empty:
        st.info("No tasks to edit. Switch to 'Create new'.")
        mode="Create new"
    if mode=="Edit existing":
        label_map={f"#{r['id']} — {r['title']}":int(r['id']) for _,r in titles.iterrows()}
        pick=st.selectbox("Select task", list(label_map.keys()), key="tsk_pick")
        tid=label_map[pick]
        trow=fetch_df("SELECT * FROM tasks WHERE id=?", (tid,)).iloc[0]
        task_dict = dict(trow)
        title=st.text_input("Title", value=trow["title"], key="tsk_title")
        desc=st.text_area("Description", value=trow["description"] or "", key="tsk_desc")
        date_assigned=st.date_input("Date assigned", value=dtparser.parse(trow["date_assigned"]).date(), key="tsk_da")
        due=st.date_input("Due date", value=dtparser.parse(trow["due_date"]).date(), key="tsk_due")
        da=int(max((due - date_assigned).days + 1, 1))
        st.write(f"Days allotted (auto): **{da}**")
        proj_opt=["—"]+[f"{r['code']} — {r['name']}" for _,r in projects.iterrows()]
        proj_value="—"
        if pd.notna(trow["project_id"]):
            pr=projects[projects["id"]==int(trow["project_id"])]
            if not pr.empty: proj_value=f"{pr['code'].iloc[0]} — {pr['name'].iloc[0]}"
        proj=st.selectbox("Project (optional)", proj_opt, index=proj_opt.index(proj_value) if proj_value in proj_opt else 0, key="tsk_proj")
        assignees=st.multiselect("Assignees", staff["name"].tolist(), key="tsk_asg",
                                 default=fetch_df("SELECT name FROM task_assignments ta JOIN staff s ON s.id=ta.staff_id WHERE ta.task_id=?", (tid,))["name"].tolist())
        colA,colB,colC=st.columns(3)
        with colA:
            if can_assign_tasks() and st.button("💾 Save", key="tsk_save"):
                execute("UPDATE tasks SET title=?,description=?,date_assigned=?,days_allotted=?,due_date=?,project_id=? WHERE id=?",
                        (title, desc or None, str(date_assigned), int(da), str(due),
                         int(projects[projects['code']==proj.split(' — ')[0]]['id'].iloc[0]) if proj!="—" else None, tid))
                execute("DELETE FROM task_assignments WHERE task_id=?", (tid,))
                for nm in assignees:
                    sid=int(staff[staff["name"]==nm]["id"].iloc[0])
                    execute("INSERT INTO task_assignments (task_id,staff_id,status) VALUES (?,?,?)",(tid,sid,"In progress"))
                st.success("Task updated."); st.rerun()
        with colB:
            if st.button("✅ Mark Completed (today)", key="tsk_done"):
                today=str(date.today())
                ass=fetch_df("SELECT id, staff_id FROM task_assignments WHERE task_id=?", (tid,))
                for _,ar in ass.iterrows():
                    execute("UPDATE task_assignments SET status='Completed', completed_date=?, days_taken=(JULIANDAY(?) - JULIANDAY(?)) WHERE id=?",
                            (today, today, trow["date_assigned"], int(ar["id"])))
                    try:
                        execute("INSERT OR IGNORE INTO points (staff_id, source, source_id, points, awarded_at) VALUES (?,?,?,?,?)",
                                (int(ar["staff_id"]), "task", int(ar["id"]), 5, datetime.now().isoformat(timespec="seconds")))
                    except Exception:
                        pass
                st.success("Marked all assignees as completed and awarded points."); st.rerun()
            if st.button("🗑️ Delete Task", key="tsk_del"):
                execute("DELETE FROM task_assignments WHERE task_id=?", (tid,))
                execute("DELETE FROM tasks WHERE id=?", (tid,))
                st.success("Task deleted."); st.rerun()
        with colC:
            st.caption("Scores only computed for **Completed** tasks. Overdue **In progress** tasks are flagged below.")

    if mode=="Edit existing":
        # --- Task Attachments ---
        st.markdown("#### 📎 Task Attachments")
        attach_files = st.file_uploader("Attach files (PDF/Image)", type=["pdf","png","jpg","jpeg"],
                                        accept_multiple_files=True,
                                        key=f"tsk_attach_{tid}")
        if st.button("📎 Upload Attachment(s)", key=f"tsk_attach_btn_{tid}"):
            if not can_upload_task_files(task_dict):
                st.error("You don't have permission to upload attachments for this task.")
                st.stop()
            if not attach_files:
                st.error("Select one or more files first.")
            else:
                ok=0
                for f in attach_files:
                    path=save_uploaded_file(f, f"task_{tid}/attachments")
                    if path:
                        execute("""INSERT INTO task_documents (task_id,file_path,original_name,uploaded_at,uploader_staff_id)
                                   VALUES (?,?,?,?,?)""",
                                (int(tid), path, getattr(f, "name", None), datetime.now().isoformat(timespec="seconds"), current_staff_id()))
                        ok += 1
                st.success(f"Uploaded {ok} attachment(s)."); st.rerun()
    
        adf=fetch_df("SELECT id, original_name, file_path, uploaded_at FROM task_documents WHERE task_id=? ORDER BY uploaded_at DESC",(int(tid),))
        if adf.empty:
            st.caption("No attachments yet.")
        else:
            for _,r in adf.iterrows():
                c1,c2,c3=st.columns([4,1,1])
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
                        st.success("Attachment removed."); st.rerun()
    
    else:
        title=st.text_input("Title", key="tsk_title_new")
        desc=st.text_area("Description", key="tsk_desc_new")
        date_assigned=st.date_input("Date assigned", value=date.today(), key="tsk_da_new")
        due=st.date_input("Due date", value=date.today()+timedelta(days=7), key="tsk_due_new")
        da=int(max((due - date_assigned).days + 1, 1))
        st.write(f"Days allotted (auto): **{da}**")
        proj_opt=["—"]+[f"{r['code']} — {r['name']}" for _,r in projects.iterrows()]
        proj=st.selectbox("Project (optional)", proj_opt, key="tsk_proj_new")
        assignees=st.multiselect("Assignees", staff["name"].tolist(), key="tsk_asg_new")
        if can_assign_tasks() and st.button("➕ Create Task", key="tsk_create"):
            pid = int(projects[projects['code']==proj.split(' — ')[0]]['id'].iloc[0]) if proj!="—" else None
            tid=execute("INSERT INTO tasks (title,description,date_assigned,days_allotted,due_date,project_id,created_by_staff_id) VALUES (?,?,?,?,?,?,?)",
                        (title, desc or None, str(date_assigned), int(da), str(due), pid, current_staff_id()))
            for nm in assignees:
                sid=int(staff[staff["name"]==nm]["id"].iloc[0])
                execute("INSERT INTO task_assignments (task_id,staff_id,status) VALUES (?,?,?)",(tid,sid,"In progress"))
            st.success("Task created."); st.rerun()

    st.subheader("Assignments")
    df=fetch_df("""
        SELECT
            ta.id,
            t.title,
            s.name AS staff,
            COALESCE(p.code || ' — ' || p.name, p.code, '—') AS project,
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
    """)
    if df.empty:
        st.info("No assignments yet.")
    else:
        df["overdue"]=df.apply(lambda r: (r["status"]!="Completed") and (date.today()>dtparser.parse(r["due_date"]).date()), axis=1)
        def score_row(r):
            if r["status"]!="Completed" or pd.isna(r["completed_date"]):
                return 0
            due=dtparser.parse(r["due_date"]).date()
            cd=dtparser.parse(r["completed_date"]).date()
            late=max((cd-due).days, 0)
            return max(0, 100 - 5*late)
        df["score"]=df.apply(score_row, axis=1)
        st.dataframe(df[["project","title","staff","due_date","status","completed_date","days_allotted","overdue","score"]], width='stretch')

    st.subheader("♟️ Chess Points (5 per completed task, 5 per bi-weekly report upload)")
    pts=fetch_df("""
        SELECT P.staff_id, S.name AS staff, SUM(P.points) AS total_points
        FROM points P JOIN staff S ON S.id=P.staff_id
        GROUP BY P.staff_id, S.name ORDER BY total_points DESC, staff
    """)
    if pts.empty:
        st.info("No points yet. Complete tasks or upload bi-weekly reports to earn points.")
    else:
        st.dataframe(pts, width='stretch')

    st.subheader("Staff Performance (Task Scores + Report Compliance)")
    if df.empty:
        task_avg = pd.DataFrame(columns=["staff","task_avg_score"])
    else:
        comp = df[df["status"]=="Completed"].copy()
        task_avg = comp.groupby("staff")["score"].mean().reset_index().rename(columns={"score":"task_avg_score"})
    today=date.today()
    proj_list=fetch_df("SELECT id, code, start_date FROM projects WHERE start_date IS NOT NULL")
    reports=fetch_df("SELECT project_id, report_date FROM biweekly_reports")
    project_staff=fetch_df("SELECT project_id, staff_id FROM project_staff")
    staff_df=fetch_df("SELECT id, name FROM staff")
    reports_by_project={}
    if not reports.empty:
        for _,r in reports.iterrows():
            pid=int(r["project_id"])
            try:
                d=dtparser.parse(r["report_date"]).date()
            except:
                continue
            reports_by_project.setdefault(pid, []).append(d)
    expected_by_project={}
    if not proj_list.empty:
        for _,p in proj_list.iterrows():
            try:
                sd=dtparser.parse(p["start_date"]).date()
            except:
                continue
            expected_by_project[int(p["id"])]=_build_expected_biweekly_windows(sd, today)
    compliance_by_project={}
    for pid, windows in expected_by_project.items():
        rdates=reports_by_project.get(pid, [])
        count_ok=0
        for (ws,we) in windows:
            ok=False
            for rd in rdates:
                if ws <= rd <= we:
                    ok=True; break
            if ok: count_ok+=1
        compliance_by_project[pid]=(count_ok, len(windows))
    rows=[]
    if not project_staff.empty:
        for _,ps in project_staff.iterrows():
            pid=int(ps["project_id"]); sid=int(ps["staff_id"])
            ok, tot = compliance_by_project.get(pid, (0,0))
            rows.append({"staff_id":sid, "ok":ok, "tot":tot})
    rep_df=pd.DataFrame(rows)
    if rep_df.empty:
        rep_comp = pd.DataFrame(columns=["staff","report_compliance_pct"])
    else:
        agg = rep_df.groupby("staff_id").sum(numeric_only=True).reset_index()
        agg["report_compliance_pct"]=agg.apply(lambda r: (100.0*r["ok"]/r["tot"]) if r["tot"]>0 else np.nan, axis=1)
        rep_comp = agg.merge(staff_df, left_on="staff_id", right_on="id", how="left")[["name","report_compliance_pct"]]
        rep_comp = rep_comp.rename(columns={"name":"staff"})
    perf = pd.merge(task_avg, rep_comp, on="staff", how="outer")
    if perf.empty:
        st.info("No performance data yet.")
    else:
        def combined(row):
            vals=[v for v in [row.get("task_avg_score"), row.get("report_compliance_pct")] if pd.notna(v)]
            return float(np.mean(vals)) if vals else np.nan
        perf["combined_score"]=perf.apply(combined, axis=1).round(1)
        perf = perf.sort_values(by=["combined_score","staff"], ascending=[False,True])
        st.dataframe(perf, width='stretch')

# ---------- Import CSVs ----------
def page_import():
    st.markdown("<div class='worknest-header'><h2>⬆️ Import CSVs</h2></div>", unsafe_allow_html=True)
    st.caption("Upload your CSV templates below (recommended for Render), or place them inside a local <b>data</b> folder next to app.py.", unsafe_allow_html=True)

    up_staff = st.file_uploader("Upload staff_template.csv", type=["csv"], key="up_staff")
    up_projects = st.file_uploader("Upload structural_project_info_min.csv", type=["csv"], key="up_projects")
    up_holidays = st.file_uploader("Upload nigeria_public_holidays_2025_2026.csv", type=["csv"], key="up_holidays")

    c1,c2=st.columns(2)
    c3,c4=st.columns(2)

    if c1.button("Import staff_template.csv", key="imp_staff"):
        path=os.path.join("data","staff_template.csv")
        if os.path.exists(path):
            df=pd.read_csv(path)
        elif up_staff is not None:
            df=pd.read_csv(up_staff)
        else:
            st.error("staff_template.csv not found. Upload it above or place it in data/.");
            df=None
        if df is not None:
            for _,r in df.iterrows():
                if pd.isna(r.get("name")) or pd.isna(r.get("rank")): continue
                email = r.get("email") if pd.notna(r.get("email")) else None
                ex = fetch_df("SELECT id FROM staff WHERE email=? OR name=?", (email, r["name"]))
                if ex.empty:
                    execute("""INSERT INTO staff (name,rank,email,phone,section,role,grade,join_date)
                               VALUES (?,?,?,?,?,?,?,?)""",
                            (r["name"], r.get("rank"), email, r.get("phone"), r.get("section"), r.get("role"), r.get("grade"), r.get("join_date")))
                else:
                    execute("""UPDATE staff SET rank=?, phone=?, section=?, role=?, grade=?, join_date=? WHERE id=?""",
                            (r.get("rank"), r.get("phone"), r.get("section"), r.get("role"), r.get("grade"), r.get("join_date"), int(ex["id"].iloc[0])))
            st.success("Staff imported/updated.")
        else:
            st.error("data/staff_template.csv not found.")

    if c3.button("Import nigeria_public_holidays_2025_2026.csv", key="imp_hol"):
        path=os.path.join("data","nigeria_public_holidays_2025_2026.csv")
        if os.path.exists(path):
            df=pd.read_csv(path)
        elif up_holidays is not None:
            df=pd.read_csv(up_holidays)
        else:
            st.error("nigeria_public_holidays_2025_2026.csv not found. Upload it above or place it in data/.");
            df=None
        if df is not None:
            for _,r in df.iterrows():
                execute("INSERT INTO public_holidays (date,name) VALUES (?,?)", (str(r["date"]), r.get("name")))
            st.success("Public holidays imported.")
        else:
            st.error("data/nigeria_public_holidays_2025_2026.csv not found.")

    if c2.button("Import structural_project_info_min.csv", key="imp_proj"):
        path=os.path.join("data","structural_project_info_min.csv")
        if os.path.exists(path):
            df=pd.read_csv(path)
        elif up_projects is not None:
            df=pd.read_csv(up_projects)
        else:
            st.error("structural_project_info_min.csv not found. Upload it above or place it in data/.");
            df=None
        if df is not None:
            staff_df=fetch_df("SELECT id,name,email FROM staff")
            def staff_id_from(row):
                sup_email = row.get("supervisor_email") if isinstance(row.get("supervisor_email"), str) else None
                sup_name  = row.get("supervisor") if isinstance(row.get("supervisor"), str) else None
                if sup_email and not staff_df.empty:
                    m = staff_df[staff_df["email"].str.lower()==sup_email.lower()]
                    if not m.empty: return int(m["id"].iloc[0])
                if sup_name and not staff_df.empty:
                    m = staff_df[staff_df["name"]==sup_name]
                    if not m.empty: return int(m["id"].iloc[0])
                return None
            for _,r in df.iterrows():
                code = r.get("code") or r.get("project_code")
                name = r.get("name") or r.get("project_name")
                client = r.get("client")
                location = r.get("location")
                sd = r.get("start_date"); ed = r.get("end_date")
                try:
                    if pd.notna(sd): sd = dtparser.parse(str(sd)).date().isoformat()
                except: sd = None
                try:
                    if pd.notna(ed): ed = dtparser.parse(str(ed)).date().isoformat()
                except: ed = None
                sup_id = staff_id_from(r)
                rs = r.get("rebar_strength"); cs = r.get("concrete_strength")
                smin = r.get("target_slump_min"); smax = r.get("target_slump_max")
                if pd.isna(code) or pd.isna(name): continue
                ex=fetch_df("SELECT id FROM projects WHERE code=?", (code,))
                if ex.empty:
                    execute("""INSERT INTO projects (code,name,client,location,rebar_strength,concrete_strength,target_slump_min,target_slump_max,supervisor_staff_id,start_date,end_date)
                               VALUES (?,?,?,?,?,?,?,?,?,?,?)""", (code,name,client,location,rs,cs,smin,smax,sup_id,sd,ed))
                else:
                    execute("""UPDATE projects SET name=?, client=?, location=?, rebar_strength=?, concrete_strength=?, target_slump_min=?, target_slump_max=?, supervisor_staff_id=?, start_date=?, end_date=? WHERE id=?""",
                            (name,client,location,rs,cs,smin,smax,sup_id,sd,ed,int(ex["id"].iloc[0])))
            st.success("Projects imported/updated.")
        else:
            st.error("data/structural_project_info_min.csv not found.")

    if c4.button("Import postings.csv", key="imp_postings"):
        path=os.path.join("data","postings.csv")
        if os.path.exists(path):
            df=pd.read_csv(path)
            staff_df=fetch_df("SELECT id,name,email FROM staff")
            proj_df=fetch_df("SELECT id,code FROM projects")
            for _,r in df.iterrows():
                pcode = r.get("project_code") or r.get("code")
                role = r.get("role") if pd.notna(r.get("role")) else None
                staff_email = r.get("staff_email") if isinstance(r.get("staff_email"), str) else None
                staff_name = r.get("staff_name") if isinstance(r.get("staff_name"), str) else None
                if pd.isna(pcode): continue
                pr = proj_df[proj_df["code"]==pcode]
                if pr.empty: continue
                pid=int(pr["id"].iloc[0])
                sid=None
                if staff_email:
                    m=staff_df[staff_df["email"].str.lower()==staff_email.lower()]
                    if not m.empty: sid=int(m["id"].iloc[0])
                if sid is None and staff_name:
                    m=staff_df[staff_df["name"]==staff_name]
                    if not m.empty: sid=int(m["id"].iloc[0])
                if sid is None: continue
                ex=fetch_df("SELECT id FROM project_staff WHERE project_id=? AND staff_id=?", (pid,sid))
                if ex.empty:
                    execute("INSERT INTO project_staff (project_id,staff_id,role) VALUES (?,?,?)", (pid,sid,role))
                else:
                    execute("UPDATE project_staff SET role=? WHERE id=?", (role, int(ex["id"].iloc[0])))
            st.success("Project postings imported/updated.")
        else:
            st.error("data/postings.csv not found.")


# ---------- Access Control (Admin) ----------
def page_access_control():
    st.subheader("🔐 Access Control")
    if not is_admin():
        st.error("Only Admin can manage access control.")
        return
    st.caption("Admin can define what staff see by assigning roles and enabling/disabling accounts. Default password for new staff accounts is 'fcda'.")
    if not is_admin():
        st.error("Admin only.")
        return

    df=fetch_df("""SELECT u.id as user_id,u.username,
                          COALESCE(u.role, CASE WHEN u.is_admin=1 THEN 'admin' ELSE 'staff' END) as role,
                          u.is_admin,
                          COALESCE(u.is_active,1) as is_active,
                          s.id as staff_id,s.name,s.email,s.rank
                   FROM users u
                   LEFT JOIN staff s ON s.id=u.staff_id
                   ORDER BY COALESCE(s.name,u.username)""")
    if df.empty:
        st.info("No users found.")
        return

    st.dataframe(df[["user_id","username","role","is_admin","is_active","name","email","rank"]], width='stretch')

    st.markdown("### Update a user")
    user_id=st.number_input("User ID", min_value=1, step=1)
    col1,col2,col3=st.columns(3)
    with col1:
        new_role=st.selectbox("Role", ["staff","admin"], index=0)
    with col2:
        active=st.selectbox("Status", [1,0], index=0, format_func=lambda x: "Active" if x==1 else "Disabled")
    with col3:
        new_pwd=st.text_input("Reset password (optional)", type="password", help="Leave blank to keep existing password.")
    if st.button("Apply changes"):
        isadm = 1 if new_role=="admin" else 0
        if new_pwd.strip():
            execute("UPDATE users SET role=?, is_admin=?, is_active=?, password_hash=? WHERE id=?",
                    (new_role, isadm, int(active), hash_pwd(new_pwd.strip()), int(user_id)))
        else:
            execute("UPDATE users SET role=?, is_admin=?, is_active=? WHERE id=?",
                    (new_role, isadm, int(active), int(user_id)))
        st.success("Updated.")
        st.rerun()

def main():
    init_db(); apply_styles()
    if not current_user():
        login_ui(); return

    # Run task reminder checks at most once per day per session (emails only if SMTP_* is configured)
    try:
        if st.session_state.get("reminder_ran_on") != str(date.today()):
            if smtp_configured():
                run_task_reminders()
            st.session_state["reminder_ran_on"] = str(date.today())
    except Exception:
        pass

        page=sidebar_nav()
    if page.startswith("🏠"): page_dashboard()
    elif page.startswith("🏗️"): page_projects()
    elif page.startswith("👥"): page_staff()
    elif page.startswith("🧳"): page_leave()
    elif page.startswith("📄"): page_leave_table()
    elif page.startswith("🗂️"): page_tasks()
    elif page.startswith("⬆️"): page_import()
    elif page.startswith("🔐"): page_access_control()
    else: page_dashboard()


if __name__=="__main__":
    main()