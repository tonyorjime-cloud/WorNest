import os, hashlib, secrets
import datetime as dt
import smtplib, ssl
from email.message import EmailMessage
from datetime import datetime, date, timedelta
from dateutil import parser as dtparser
from dateutil.relativedelta import relativedelta
import pandas as pd, numpy as np, streamlit as st
from streamlit_cookies_manager import CookieManager
import uuid
import streamlit.components.v1 as components

# Optional HTTP client (for push notifications)
try:
    import requests
except Exception:
    requests = None

# ML (optional)
try:
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import OneHotEncoder
    from sklearn.compose import ColumnTransformer
    from sklearn.pipeline import Pipeline
    from sklearn.metrics import accuracy_score, roc_auc_score, mean_absolute_error
    from sklearn.linear_model import LogisticRegression
    from sklearn.ensemble import RandomForestRegressor
    import joblib
except Exception:
    train_test_split = None
    OneHotEncoder = None
    ColumnTransformer = None
    Pipeline = None
    accuracy_score = None
    roc_auc_score = None
    mean_absolute_error = None
    LogisticRegression = None
    RandomForestRegressor = None
    joblib = None

try:
    import psycopg2
    import psycopg2.extras
except Exception:
    psycopg2 = None  # type: ignore

import sqlite3

# Database backend selection
DB_URL = os.getenv('DATABASE_URL') or os.getenv('WORKNEST_DB_URL') or ''
DB_IS_POSTGRES = bool(DB_URL.strip().lower().startswith(('postgres://','postgresql://')))
# Backwards-compat alias used by older helper functions / branches
# (Some parts of the app still reference USE_PG; keep it in sync with DB_IS_POSTGRES.)
USE_PG = DB_IS_POSTGRES


st.set_page_config(page_title="WorkNest Mini v3.2.4", layout="wide")
# --- Persistent login (Remember me) ---
cookies = CookieManager(prefix="worknest")
TOKEN_SALT = os.environ.get("WORKNEST_TOKEN_SALT") or os.environ.get("SECRET_KEY") or "worknest-mini"

def _hash_token(raw: str) -> str:
    return hashlib.sha256((raw + TOKEN_SALT).encode("utf-8")).hexdigest()

def _utcnow_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat()

def _parse_iso(s: str):
    try:
        return dtparser.parse(s) if s else None
    except Exception:
        return None

def try_auto_login_from_cookie():
    """
    If session is empty but a remember_token cookie exists, validate it and restore st.session_state['user'].
    This keeps the existing auth model intact (users table is still the source of truth for accounts).
    """
    if st.session_state.get("user"):
        return True

    # CookieManager needs a ready() handshake
    try:
        if not cookies.ready():
            st.stop()
    except Exception:
        # If cookie manager fails, fall back to normal login
        return False

    raw = cookies.get("remember_token")
    if not raw:
        return False

    token_hash = _hash_token(str(raw))
    row = fetch_df("""
        SELECT a.expires_at, u.*
        FROM auth_tokens a
        JOIN users u ON u.id = a.user_id
        WHERE a.token_hash = ?
        LIMIT 1
    """, (token_hash,))

    if row.empty:
        return False

    expires_at = _parse_iso(str(row["expires_at"].iloc[0]))
    if (expires_at is not None) and (expires_at < dt.datetime.utcnow()):
        # Expired token: cleanup and force login
        try:
            execute("DELETE FROM auth_tokens WHERE token_hash=?", (token_hash,))
        except Exception:
            pass
        try:
            cookies.delete("remember_token"); cookies.save()
        except Exception:
            pass
        return False

    # User must still be active
    if int(row["is_active"].iloc[0] if "is_active" in row.columns else 1) != 1:
        return False

    st.session_state["user"] = dict(row.iloc[0].drop(labels=["expires_at"], errors="ignore"))
    # Touch last_used_at (best-effort)
    try:
        execute("UPDATE auth_tokens SET last_used_at=? WHERE token_hash=?", (_utcnow_iso(), token_hash))
    except Exception:
        pass
    return True

def clear_remember_cookie_and_token():
    """Invalidate the current remember-token (if any) and clear the browser cookie."""
    try:
        if not cookies.ready():
            return
    except Exception:
        return
    raw = cookies.get("remember_token")
    if raw:
        try:
            execute("DELETE FROM auth_tokens WHERE token_hash=?", (_hash_token(str(raw)),))
        except Exception:
            pass
    try:
        cookies.delete("remember_token"); cookies.save()
    except Exception:
        pass


# --- Navigation constants (avoid accidental indentation bugs) ---
BASE_PAGES = ["🏠 Dashboard","🏗️ Projects","🗂️ Tasks & Performance","🧳 Leave","💬 Chat","⚙️ Account","❓ Help"]
ADMIN_PAGES = ["👥 Staff","📄 Leave Table","⬆️ Import CSVs","🔐 Access Control","🤖 ML / Insights","📥 Admin Inbox"]



def inject_mobile_drawer():
    """Enable a slide-in/slide-out sidebar drawer on small screens (mobile)."""
    # CSS (inject via markdown with unsafe HTML)
    st.markdown(
        """
        <style>
        /* Drawer behavior only on narrow screens */
        @media (max-width: 900px){
          [data-testid="stSidebar"]{
            position: fixed;
            top: 0;
            left: 0;
            height: 100vh;
            width: 82vw;
            max-width: 340px;
            transform: translateX(-105%);
            transition: transform .25s ease;
            z-index: 1002;
          }
          body.worknest-drawer-open [data-testid="stSidebar"]{
            transform: translateX(0);
          }
          .worknest-drawer-backdrop{
            display: none;
            position: fixed;
            inset: 0;
            background: rgba(0,0,0,.35);
            z-index: 1001;
          }
          body.worknest-drawer-open .worknest-drawer-backdrop{display:block;}
          .worknest-drawer-btn{
            position: fixed;
            top: 10px;
            left: 10px;
            z-index: 1003;
            border-radius: 10px;
            padding: 8px 10px;
            background: rgba(20,20,20,.55);
            border: 1px solid rgba(255,255,255,.12);
            color: #fff;
            font-weight: 700;
            cursor: pointer;
            user-select: none;
          }
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    # HTML + JS (use components.html so script executes, not printed)
    import streamlit.components.v1 as components
    components.html(
        """
        <div class="worknest-drawer-backdrop" id="wn_backdrop"></div>
        <div class="worknest-drawer-btn" id="wn_drawer_btn">☰</div>
        <script>
        (function(){
          function setOpen(v){
            document.body.classList.toggle('worknest-drawer-open', !!v);
          }
          function isOpen(){
            return document.body.classList.contains('worknest-drawer-open');
          }

          window.__wnOpenDrawer = function(){ setOpen(true); };
          window.__wnCloseDrawer = function(){ setOpen(false); };
          window.__wnToggleDrawer = function(){ setOpen(!isOpen()); };

          var btn = document.getElementById('wn_drawer_btn');
          if(btn){
            btn.addEventListener('click', function(e){
              e.preventDefault(); window.__wnToggleDrawer();
            });
          }
          var backdrop = document.getElementById('wn_backdrop');
          if(backdrop){
            backdrop.addEventListener('click', function(e){
              e.preventDefault(); window.__wnCloseDrawer();
            });
          }

          // Swipe handling: swipe right from left edge opens; swipe left closes
          var touchStartX=null, touchStartY=null;
          document.addEventListener('touchstart', function(e){
            if(!e.touches || !e.touches.length) return;
            touchStartX=e.touches[0].clientX;
            touchStartY=e.touches[0].clientY;
          }, {passive:true});

          document.addEventListener('touchmove', function(e){
            if(touchStartX===null || !e.touches || !e.touches.length) return;
            var x=e.touches[0].clientX, y=e.touches[0].clientY;
            var dx=x-touchStartX, dy=y-touchStartY;

            // Ignore small moves or vertical swipes
            if(Math.abs(dx) < 35 || Math.abs(dx) < Math.abs(dy)) return;

            // open gesture: start near left edge and swipe right
            if(!isOpen() && touchStartX < 25 && dx > 60){
              setOpen(true); touchStartX=null; return;
            }
            // close gesture: swipe left when open
            if(isOpen() && dx < -60){
              setOpen(false); touchStartX=null; return;
            }
          }, {passive:true});

          document.addEventListener('touchend', function(){
            touchStartX=null; touchStartY=null;
          }, {passive:true});
        })();
        </script>
        """,
        height=0,
        width=0
    )



def safe_parse_date(v):
    """Parse a date string safely; returns datetime.date or None."""
    if v is None: return None
    s=str(v).strip()
    if not s or s.lower()=="nan": return None
    try:
        return dtparser.parse(s).date()
    except Exception:
        return None

APP_TITLE="WorkNest Mini v3.2.4"
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
# Prefer Render persistent disk if present. If you mount a disk at /var/data,
# files written under /var/data will survive redeploys/restarts.
RENDER_DISK_DIR = "/var/data/worknest_data"
DEFAULT_LOCAL_DATA = RENDER_DISK_DIR if os.path.isdir("/var/data") else os.path.join(os.getcwd(), "data")

# If the env var points to /tmp (ephemeral on Render), ignore it and use the
# persistent disk path instead.
effective_env_dir = ENV_DATA_DIR
if ENV_DATA_DIR.startswith("/tmp") and os.path.isdir("/var/data"):
    effective_env_dir = ""

DATA_DIR = _first_writable_dir([effective_env_dir, DEFAULT_LOCAL_DATA, os.getcwd()])

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
  id BIGSERIAL PRIMARY KEY,
  project_id INTEGER NOT NULL,
  report_date TEXT NOT NULL,
  file_path TEXT NOT NULL,
  uploaded_at TEXT NOT NULL,
  uploader_staff_id INTEGER,
  status TEXT,
  approved_at TEXT,
  approved_by_staff_id INTEGER,
  rejected_reason TEXT
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
  id BIGSERIAL PRIMARY KEY,
  project_id INTEGER NOT NULL,
  building_id INTEGER,
  stage TEXT,
  test_type TEXT NOT NULL,
  batch_id TEXT,
  file_path TEXT NOT NULL,
  uploaded_at TEXT NOT NULL,
  uploader_staff_id INTEGER,
  test_date TEXT,
  notes TEXT,
  status TEXT,
  approved_at TEXT,
  approved_by_staff_id INTEGER,
  rejected_reason TEXT
);

CREATE TABLE IF NOT EXISTS chat_messages (
  id SERIAL PRIMARY KEY,
  staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL,
  message TEXT,
  image_path TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
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


CREATE TABLE IF NOT EXISTS auth_tokens (
  id SERIAL PRIMARY KEY,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  token_hash TEXT UNIQUE NOT NULL,
  expires_at TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT NOW(),
  last_used_at TEXT
);

CREATE TABLE IF NOT EXISTS performance_index (
  id SERIAL PRIMARY KEY,
  staff_id INTEGER NOT NULL REFERENCES staff(id) ON DELETE CASCADE,
  month TEXT NOT NULL, -- YYYY-MM-01
  task_points INTEGER DEFAULT 0,
  report_points INTEGER DEFAULT 0,
  test_points INTEGER DEFAULT 0,
  reliability_score INTEGER DEFAULT 0,
  attention_to_detail_score INTEGER DEFAULT 0,
  UNIQUE(staff_id, month)
);

CREATE TABLE IF NOT EXISTS staff_of_month_posts (
  id SERIAL PRIMARY KEY,
  month TEXT NOT NULL UNIQUE, -- YYYY-MM-01
  staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL,
  total_score INTEGER DEFAULT 0,
  posted_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ml_runs (
  id SERIAL PRIMARY KEY,
  model_name TEXT NOT NULL,
  trained_at TEXT NOT NULL,
  train_rows INTEGER,
  metrics_json TEXT,
  model_path TEXT
);

CREATE TABLE IF NOT EXISTS ml_predictions (
  id SERIAL PRIMARY KEY,
  created_at TEXT NOT NULL,
  model_name TEXT NOT NULL,
  task_id INTEGER REFERENCES tasks(id) ON DELETE SET NULL,
  assignment_id INTEGER REFERENCES task_assignments(id) ON DELETE SET NULL,
  staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL,
  predicted_overdue_prob REAL,
  predicted_days_taken REAL,
  features_json TEXT,
  actual_overdue INTEGER,
  actual_days_taken REAL
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
            try:
                cur.execute(ddl)
            except Exception as e:
                msg = str(e)
                # Ignore missing-table errors during incremental migrations
                if ('does not exist' in msg and ('relation' in msg or 'table' in msg)):
                    return
                # Ignore duplicate-column errors
                if ('already exists' in msg or 'duplicate column' in msg):
                    return
                raise

        # leaves: align old schema to new fields expected by UI
        if not _pg_has_column('leaves', 'relieving_staff_id'):
            _pg_add_column("ALTER TABLE leaves ADD COLUMN relieving_staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL")
        if not _pg_has_column('leaves', 'status'):
            _pg_add_column("ALTER TABLE leaves ADD COLUMN status TEXT DEFAULT 'Pending'")

        # biweekly_reports: add uploaded_at for true submission timestamp
        if not _pg_has_column('biweekly_reports', 'uploaded_at'):
            _pg_add_column("ALTER TABLE biweekly_reports ADD COLUMN uploaded_at TEXT")
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


        # app_settings: key/value configuration
        cur.execute("""CREATE TABLE IF NOT EXISTS app_settings (
          key TEXT PRIMARY KEY,
          value TEXT
        );""")

        # users: must_change_password
        if not _pg_has_column('users', 'must_change_password'):
            _pg_add_column("ALTER TABLE users ADD COLUMN must_change_password INTEGER DEFAULT 0")

        # password resets (for 'forgot password')
        cur.execute("""CREATE TABLE IF NOT EXISTS password_resets (
          id SERIAL PRIMARY KEY,
          user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
          token_hash TEXT NOT NULL,
          expires_at TEXT NOT NULL,
          used INTEGER DEFAULT 0
        );""")

        # user_permissions: per-user capability toggles (feature flags)
        cur.execute("""CREATE TABLE IF NOT EXISTS user_permissions (
          user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
          can_assign_tasks INTEGER DEFAULT 0,
          can_confirm_task_completion INTEGER DEFAULT 0,
          can_upload_project_docs INTEGER DEFAULT 0
        );""")

        # projects: next_due_date for bi-weekly reporting schedule
        if not _pg_has_column('projects', 'next_due_date'):
            _pg_add_column("ALTER TABLE projects ADD COLUMN next_due_date TEXT")

        # chat_messages: allow pdf and other attachments
        if not _pg_has_column('chat_messages', 'attachment_path'):
            _pg_add_column("ALTER TABLE chat_messages ADD COLUMN attachment_path TEXT")
        if not _pg_has_column('chat_messages', 'attachment_name'):
            _pg_add_column("ALTER TABLE chat_messages ADD COLUMN attachment_name TEXT")
        if not _pg_has_column('chat_messages', 'attachment_type'):
            _pg_add_column("ALTER TABLE chat_messages ADD COLUMN attachment_type TEXT")

        # ---- Content approval workflow hardening (older DBs may miss these)
        # biweekly_reports
        if not _pg_has_column('biweekly_reports', 'status'):
            _pg_add_column("ALTER TABLE biweekly_reports ADD COLUMN status TEXT")
        if not _pg_has_column('biweekly_reports', 'approved_at'):
            _pg_add_column("ALTER TABLE biweekly_reports ADD COLUMN approved_at TEXT")
        if not _pg_has_column('biweekly_reports', 'approved_by_staff_id'):
            _pg_add_column("ALTER TABLE biweekly_reports ADD COLUMN approved_by_staff_id INTEGER")
        if not _pg_has_column('biweekly_reports', 'rejected_reason'):
            _pg_add_column("ALTER TABLE biweekly_reports ADD COLUMN rejected_reason TEXT")

        
        # documents (core docs)
        if not _pg_has_column('documents', 'doc_date'):
            _pg_add_column("ALTER TABLE documents ADD COLUMN doc_date TEXT")
# test_results
        if not _pg_has_column('test_results', 'test_date'):
            _pg_add_column("ALTER TABLE test_results ADD COLUMN test_date TEXT")
        if not _pg_has_column('test_results', 'status'):
            _pg_add_column("ALTER TABLE test_results ADD COLUMN status TEXT")
        if not _pg_has_column('test_results', 'approved_at'):
            _pg_add_column("ALTER TABLE test_results ADD COLUMN approved_at TEXT")
        if not _pg_has_column('test_results', 'approved_by_staff_id'):
            _pg_add_column("ALTER TABLE test_results ADD COLUMN approved_by_staff_id INTEGER")
        if not _pg_has_column('test_results', 'rejected_reason'):
            _pg_add_column("ALTER TABLE test_results ADD COLUMN rejected_reason TEXT")

    else:
        sqlite_schema = """CREATE TABLE IF NOT EXISTS public_holidays (id INTEGER PRIMARY KEY, date TEXT NOT NULL, name TEXT);
CREATE TABLE IF NOT EXISTS staff (id INTEGER PRIMARY KEY, name TEXT NOT NULL, rank TEXT NOT NULL, email TEXT UNIQUE, phone TEXT, section TEXT, role TEXT, grade TEXT, join_date TEXT, dob TEXT);
CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, staff_id INTEGER, username TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL, is_admin INTEGER DEFAULT 0, role TEXT DEFAULT 'staff', is_active INTEGER DEFAULT 1);
CREATE TABLE IF NOT EXISTS projects (id INTEGER PRIMARY KEY, code TEXT UNIQUE NOT NULL, name TEXT NOT NULL, client TEXT, location TEXT, rebar_strength REAL, concrete_strength REAL, target_slump_min REAL, target_slump_max REAL, supervisor_staff_id INTEGER, start_date TEXT, end_date TEXT, next_due_date TEXT);
CREATE TABLE IF NOT EXISTS project_staff (id INTEGER PRIMARY KEY, project_id INTEGER NOT NULL, staff_id INTEGER NOT NULL, role TEXT, UNIQUE(project_id,staff_id));
CREATE TABLE IF NOT EXISTS buildings (id INTEGER PRIMARY KEY, project_id INTEGER NOT NULL, name TEXT NOT NULL, floors INTEGER DEFAULT 0);
CREATE TABLE IF NOT EXISTS documents (id INTEGER PRIMARY KEY, project_id INTEGER NOT NULL, building_id INTEGER, category TEXT NOT NULL, file_path TEXT NOT NULL, uploaded_at TEXT NOT NULL, uploader_staff_id INTEGER);
CREATE TABLE IF NOT EXISTS biweekly_reports (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id INTEGER NOT NULL,
  report_date TEXT NOT NULL,
  file_path TEXT NOT NULL,
  uploaded_at TEXT NOT NULL,
  uploader_staff_id INTEGER,
  status TEXT,
  approved_at TEXT,
  approved_by_staff_id INTEGER,
  rejected_reason TEXT
);
CREATE TABLE IF NOT EXISTS tasks (id INTEGER PRIMARY KEY, title TEXT NOT NULL, description TEXT, date_assigned TEXT NOT NULL, days_allotted INTEGER NOT NULL, due_date TEXT NOT NULL, project_id INTEGER, created_by_staff_id INTEGER);
CREATE TABLE IF NOT EXISTS task_assignments (id INTEGER PRIMARY KEY, task_id INTEGER NOT NULL, staff_id INTEGER NOT NULL, status TEXT DEFAULT 'In progress', completed_date TEXT, days_taken INTEGER);
CREATE TABLE IF NOT EXISTS task_documents (id INTEGER PRIMARY KEY, task_id INTEGER NOT NULL, file_path TEXT NOT NULL, original_name TEXT, uploaded_at TEXT NOT NULL, uploader_staff_id INTEGER);
CREATE TABLE IF NOT EXISTS reminders_sent (id INTEGER PRIMARY KEY, assignment_id INTEGER NOT NULL, reminder_type TEXT NOT NULL, sent_on TEXT NOT NULL, UNIQUE(assignment_id, reminder_type, sent_on));
CREATE TABLE IF NOT EXISTS leaves (id INTEGER PRIMARY KEY, staff_id INTEGER NOT NULL, leave_type TEXT NOT NULL, start_date TEXT NOT NULL, end_date TEXT NOT NULL, working_days INTEGER DEFAULT 0, relieving_staff_id INTEGER, status TEXT DEFAULT 'Pending', reason TEXT, request_date TEXT, approved_by_staff_id INTEGER);
CREATE TABLE IF NOT EXISTS test_results (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  project_id INTEGER NOT NULL,
  building_id INTEGER,
  stage TEXT,
  test_type TEXT NOT NULL,
  batch_id TEXT,
  file_path TEXT NOT NULL,
  uploaded_at TEXT NOT NULL,
  uploader_staff_id INTEGER,
  test_date TEXT,
  notes TEXT,
  status TEXT,
  approved_at TEXT,
  approved_by_staff_id INTEGER,
  rejected_reason TEXT
);
CREATE TABLE IF NOT EXISTS points (id INTEGER PRIMARY KEY, staff_id INTEGER NOT NULL, source TEXT NOT NULL, source_id INTEGER NOT NULL, points INTEGER NOT NULL, awarded_at TEXT NOT NULL, UNIQUE(staff_id, source, source_id));
CREATE TABLE IF NOT EXISTS auth_tokens (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, token_hash TEXT UNIQUE NOT NULL, expires_at TEXT NOT NULL, created_at TEXT NOT NULL, last_used_at TEXT);
CREATE TABLE IF NOT EXISTS performance_index (id INTEGER PRIMARY KEY, staff_id INTEGER NOT NULL, month TEXT NOT NULL, task_points INTEGER DEFAULT 0, report_points INTEGER DEFAULT 0, test_points INTEGER DEFAULT 0, reliability_score INTEGER DEFAULT 0, attention_to_detail_score INTEGER DEFAULT 0, UNIQUE(staff_id, month));
CREATE TABLE IF NOT EXISTS staff_of_month_posts (id INTEGER PRIMARY KEY, month TEXT NOT NULL UNIQUE, staff_id INTEGER, total_score INTEGER DEFAULT 0, posted_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS notices (id INTEGER PRIMARY KEY, staff_id INTEGER NOT NULL, title TEXT NOT NULL, message TEXT NOT NULL, image_path TEXT, posted_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS notice_comments (id INTEGER PRIMARY KEY, notice_id INTEGER NOT NULL, staff_id INTEGER NOT NULL, comment TEXT NOT NULL, posted_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS chat_messages (id INTEGER PRIMARY KEY, staff_id INTEGER NOT NULL, message TEXT, image_path TEXT, attachment_path TEXT, attachment_name TEXT, attachment_type TEXT, posted_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS app_settings (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS password_resets (id INTEGER PRIMARY KEY, user_id INTEGER, token_hash TEXT NOT NULL, expires_at TEXT NOT NULL, used INTEGER DEFAULT 0);
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
            cols = [r[1] for r in cur.execute("PRAGMA table_info(biweekly_reports)").fetchall()]
            if "uploaded_at" not in cols:
                cur.execute("ALTER TABLE biweekly_reports ADD COLUMN uploaded_at TEXT")
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
            cols = [r[1] for r in cur.execute("PRAGMA table_info(users)").fetchall()]
            if "must_change_password" not in cols:
                cur.execute("ALTER TABLE users ADD COLUMN must_change_password INTEGER DEFAULT 0")
        except Exception:
            pass


        # projects: next_due_date
        try:
            cols = [r[1] for r in cur.execute("PRAGMA table_info(projects)").fetchall()]
            if "next_due_date" not in cols:
                cur.execute("ALTER TABLE projects ADD COLUMN next_due_date TEXT")
        except Exception:
            pass

        # user_permissions
        try:
            cur.execute("CREATE TABLE IF NOT EXISTS user_permissions (user_id INTEGER PRIMARY KEY, can_assign_tasks INTEGER DEFAULT 0, can_confirm_task_completion INTEGER DEFAULT 0, can_upload_project_docs INTEGER DEFAULT 0)")
        except Exception:
            pass

        try:
            cols = [r[1] for r in cur.execute("PRAGMA table_info(chat_messages)").fetchall()]
            for col, ddl in [
                ("attachment_path", "ALTER TABLE chat_messages ADD COLUMN attachment_path TEXT"),
                ("attachment_name", "ALTER TABLE chat_messages ADD COLUMN attachment_name TEXT"),
                ("attachment_type", "ALTER TABLE chat_messages ADD COLUMN attachment_type TEXT"),
            ]:
                if col not in cols:
                    cur.execute(ddl)
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


    # Ensure forward-compatible columns exist (older DBs may not have them yet)
    try:
        if DB_IS_POSTGRES:
            execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS must_change_password INTEGER DEFAULT 0")
            execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS password_changed_at TEXT")
            # Governance: approvals for uploads (reports/tests)
            execute("ALTER TABLE biweekly_reports ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'PENDING'")
            execute("ALTER TABLE biweekly_reports ADD COLUMN IF NOT EXISTS reviewed_by_staff_id INTEGER")
            execute("ALTER TABLE biweekly_reports ADD COLUMN IF NOT EXISTS reviewed_at TEXT")
            execute("ALTER TABLE biweekly_reports ADD COLUMN IF NOT EXISTS review_note TEXT")
            execute("ALTER TABLE test_results ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'PENDING'")
            execute("ALTER TABLE test_results ADD COLUMN IF NOT EXISTS reviewed_by_staff_id INTEGER")
            execute("ALTER TABLE test_results ADD COLUMN IF NOT EXISTS reviewed_at TEXT")
            execute("ALTER TABLE test_results ADD COLUMN IF NOT EXISTS review_note TEXT")
            # Backfill NULL statuses to APPROVED for legacy rows (so existing history doesn't vanish)
            execute("UPDATE biweekly_reports SET status='APPROVED' WHERE status IS NULL")
            execute("UPDATE test_results SET status='APPROVED' WHERE status IS NULL")

        else:
            # SQLite: IF NOT EXISTS for columns is not supported; ignore errors
            try: execute("ALTER TABLE users ADD COLUMN must_change_password INTEGER DEFAULT 0")
            except Exception: pass
            try: execute("ALTER TABLE users ADD COLUMN password_changed_at TEXT")
            except Exception: pass
            # Governance: approvals for uploads (reports/tests)
            for q in [
                "ALTER TABLE biweekly_reports ADD COLUMN status TEXT DEFAULT 'PENDING'",
                "ALTER TABLE biweekly_reports ADD COLUMN reviewed_by_staff_id INTEGER",
                "ALTER TABLE biweekly_reports ADD COLUMN reviewed_at TEXT",
                "ALTER TABLE biweekly_reports ADD COLUMN review_note TEXT",
                "ALTER TABLE test_results ADD COLUMN status TEXT DEFAULT 'PENDING'",
                "ALTER TABLE test_results ADD COLUMN reviewed_by_staff_id INTEGER",
                "ALTER TABLE test_results ADD COLUMN reviewed_at TEXT",
                "ALTER TABLE test_results ADD COLUMN review_note TEXT",
            ]:
                try: execute(q)
                except Exception: pass
            try: execute("UPDATE biweekly_reports SET status='APPROVED' WHERE status IS NULL")
            except Exception: pass
            try: execute("UPDATE test_results SET status='APPROVED' WHERE status IS NULL")
            except Exception: pass

    except Exception:
        pass

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
                    # Postgres compatibility: SQLite uses INSERT OR IGNORE
                    if q.lower().startswith("insert or ignore"):
                        q = "INSERT" + q[len("INSERT OR IGNORE"):]
                        if " on conflict" not in q.lower():
                            q = q.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
                    low=q.lower()

                    # For INSERTs, we *optionally* try to fetch the new id (common for SERIAL PK tables).
                    # We must use a SAVEPOINT because a failed RETURNING attempt aborts the transaction in Postgres.
                    if low.startswith("insert") and "returning" not in low:
                        cur.execute("SAVEPOINT sp_worknest_insert")
                        q2=q.rstrip().rstrip(";")+" RETURNING id"
                        try:
                            cur.execute(q2, p)
                            row=cur.fetchone()
                            cur.execute("RELEASE SAVEPOINT sp_worknest_insert")
                            return int(row[0]) if row else None
                        except Exception as e:
                            cur.execute("ROLLBACK TO SAVEPOINT sp_worknest_insert")
                            cur.execute("RELEASE SAVEPOINT sp_worknest_insert")
                            # Some tables (e.g., app_settings) don't have an 'id' column.
                            msg=str(e)
                            if ("does not exist" in msg.lower()) and ("id" in msg.lower()):
                                cur.execute(q, p)
                                return None
                            # Fall back to plain execute for other RETURNING failures too.
                            cur.execute(q, p)
                            return None

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

def execute_sql(q, p=()):
    """Backward-compatible alias used by some pages."""
    return execute(q, p)


def exec_sql(q: str, p=None):
    """Backward-compatible alias for execute_sql (some modules still call exec_sql)."""
    return execute_sql(q, p)


# ---------------------------
# Push Notifications (OneSignal Web Push)
# ---------------------------
def _onesignal_cfg():
    """Return (app_id, rest_api_key) or (None, None) if not configured."""
    app_id = os.getenv("ONESIGNAL_APP_ID")
    api_key = os.getenv("ONESIGNAL_REST_API_KEY")
    if not app_id or not api_key or requests is None:
        return None, None
    return app_id.strip(), api_key.strip()


def render_push_opt_in(external_user_id: str):
    """Inject OneSignal SDK and (if configured) bind the user to external_user_id (usually email)."""
    app_id, _ = _onesignal_cfg()
    if not app_id:
        return
    # Avoid re-injecting every rerun
    key = f"onesignal_bound::{external_user_id}"
    if st.session_state.get(key):
        return
    st.session_state[key] = True
    components.html(
        f"""
        <script src="https://cdn.onesignal.com/sdks/OneSignalSDK.js" async=""></script>
        <script>
          window.OneSignal = window.OneSignal || [];
          OneSignal.push(function() {{
            OneSignal.init({{ appId: "{app_id}", notifyButton: {{ enable: true }} }});
            try {{ OneSignal.login("{external_user_id}"); }} catch (e) {{ /* ignore */ }}
          }});
        </script>
        """,
        height=0,
    )


def send_push(external_user_ids, title: str, message: str):
    """Send a push notification to OneSignal users identified by external_user_ids (emails).

    No-op if OneSignal is not configured.
    """
    app_id, api_key = _onesignal_cfg()
    if not app_id:
        return False
    if not external_user_ids:
        return False
    # De-dup + sanitize
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


def _staff_emails_for_project(project_id: int):
    try:
        df = fetch_df(
            """
            SELECT s.email
            FROM project_staff ps
            JOIN staff s ON s.id = ps.staff_id
            WHERE ps.project_id=? AND COALESCE(s.email,'')<>''
            """,
            (project_id,),
        )
        return [str(x) for x in df["email"].tolist()] if not df.empty else []
    except Exception:
        return []

def get_setting(key:str, default:str|None=None)->str|None:
    """Read a setting from DB (app_settings)."""
    try:
        df = fetch_df("SELECT value FROM app_settings WHERE key=?", (key,))
        if not df.empty:
            v = df.iloc[0]["value"]
            return None if v is None else str(v)
    except Exception:
        pass
    return default

def set_setting(key:str, value:str|None)->None:
    """Upsert a setting."""
    if value is None:
        execute("DELETE FROM app_settings WHERE key=?", (key,))
        return
    if DB_IS_POSTGRES:
        execute("""INSERT INTO app_settings(key,value) VALUES(?,?)
                   ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value""", (key, value))
    else:
        execute("""INSERT OR REPLACE INTO app_settings(key,value) VALUES(?,?)""", (key, value))

def _today()->dt.date:
    return dt.date.today()

def _parse_date_safe(s)->dt.date|None:
    if s is None: 
        return None
    try:
        s=str(s).strip()
        if not s or s.lower() in ("nan","none","null"):
            return None
        return dtparser.parse(s).date()
    except Exception:
        return None


def _parse_date(v):
    """Backwards-compatible alias used by older perf code."""
    return _parse_date_safe(v)

def _biweekly_start_date()->dt.date:
    # Global org start date; default to next Tuesday from today if not set
    v = get_setting("BIWEEKLY_START_DATE")
    d = _parse_date_safe(v)
    if d:
        return d
    # next Tuesday (weekday 1 where Monday=0)
    t=_today()
    delta=(1 - t.weekday()) % 7
    if delta==0:
        delta=7
    return t + dt.timedelta(days=delta)

def _task_points(date_assigned, days_allotted:int, completed_date)->int:
    da=_parse_date_safe(date_assigned)
    cd=_parse_date_safe(completed_date)
    if not da or not cd or not days_allotted:
        return 0
    days=(cd - da).days + 1
    # Scoring rule (agreed):
    # - completed within allotted duration: 3
    # - completed within 1.5× duration: 2
    # - completed after 1.5× duration (including >2×): 1
    if days <= days_allotted:
        return 3
    if days <= int(np.ceil(1.5*days_allotted)):
        return 2
    return 1

def _report_points(due:dt.date, submitted:dt.date)->int:
    if submitted <= due:
        return 3
    if submitted <= (due + dt.timedelta(days=7)):
        return 2
    return 1

def _test_points()->int:
    # Any submitted test report earns 3 points
    return 3

def compute_staff_activity_points(staff_id:int)->dict:
    """Compute transparent points from tasks + biweekly reports + test reports."""
    out={"task_points":0, "report_points":0, "test_points":0, "total":0}
    # tasks
    tdf = fetch_df("""SELECT T.date_assigned, T.days_allotted, A.completed_date
                       FROM task_assignments A
                       JOIN tasks T ON T.id=A.task_id
                       WHERE A.staff_id=? AND A.status='Completed'""", (staff_id,))
    for _,r in tdf.iterrows():
        out["task_points"] += _task_points(r.get("date_assigned"), int(r.get("days_allotted") or 0), r.get("completed_date"))
    # biweekly reports (award to staff assigned to that project)
    start=_biweekly_start_date()
    today=_today()
    # all projects the staff is posted to
    pdf = fetch_df("""SELECT P.id, P.code, P.name
                        FROM project_staff PS JOIN projects P ON P.id=PS.project_id
                        WHERE PS.staff_id=?""", (staff_id,))
    if not pdf.empty:
        # prefetch reports for those projects
        proj_ids=tuple(int(x) for x in pdf["id"].tolist())
        qmarks=",".join(["?"]*len(proj_ids))
        rdf = fetch_df(
            f"""SELECT project_id, report_date
                 FROM biweekly_reports
                 WHERE project_id IN ({qmarks})
                   AND COALESCE(status,'APPROVED')='APPROVED'""",
            proj_ids,
        )
        # map project->list of dates
        rmap={}
        for _,rr in rdf.iterrows():
            d=_parse_date_safe(rr.get("report_date"))
            if d:
                rmap.setdefault(int(rr["project_id"]), []).append(d)
        # for each project, for each due date up to today, pick first submitted after due-14 days window
        for pid in proj_ids:
            due=start
            while due <= today:
                # find report in window [due-13, due+14] (submit can be late up to 14 days for scoring)
                window_start=due - dt.timedelta(days=13)
                window_end=due + dt.timedelta(days=14)
                candidates=[d for d in rmap.get(pid, []) if window_start <= d <= window_end]
                if candidates:
                    submitted=min(candidates)
                    out["report_points"] += _report_points(due, submitted)
                # if none, 0 points for that cycle
                due += dt.timedelta(days=14)

    # test reports (award to uploader)
    tdf = fetch_df(
        "SELECT id FROM test_results WHERE uploader_staff_id=? AND COALESCE(status,'APPROVED')='APPROVED'",
        (staff_id,),
    )
    if not tdf.empty:
        out["test_points"] = int(len(tdf)) * _test_points()

    out["total"]=out["task_points"]+out["report_points"]+out["test_points"]
    return out


# ---------- Performance Index (Monthly) ----------

def _month_start(d:dt.date)->dt.date:
    return dt.date(d.year, d.month, 1)

def _month_end(d:dt.date)->dt.date:
    ms=_month_start(d)
    if ms.month==12:
        nxt=dt.date(ms.year+1, 1, 1)
    else:
        nxt=dt.date(ms.year, ms.month+1, 1)
    return nxt - dt.timedelta(days=1)

def _is_last_day_of_month(d:dt.date)->bool:
    return d == _month_end(d)

def _perf_include_soft()->bool:
    return str(get_setting("PERF_INCLUDE_SOFT", "0") or "0").strip() in ("1","true","True","yes","YES")

def upsert_performance_index(staff_id:int, month:dt.date, task_pts:int, report_pts:int, test_pts:int,
                             reliability:int|None=None, attention:int|None=None)->None:
    m=str(_month_start(month))
    if reliability is None or attention is None:
        ex=fetch_df("SELECT reliability_score, attention_to_detail_score FROM performance_index WHERE staff_id=? AND month=?", (int(staff_id), m))
        if ex.empty:
            reliability = 0 if reliability is None else reliability
            attention = 0 if attention is None else attention
        else:
            if reliability is None: reliability=int(ex.iloc[0].get("reliability_score") or 0)
            if attention is None: attention=int(ex.iloc[0].get("attention_to_detail_score") or 0)

    if DB_IS_POSTGRES:
        execute(
            """INSERT INTO performance_index (staff_id, month, task_points, report_points, test_points, reliability_score, attention_to_detail_score)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT (staff_id, month) DO UPDATE
               SET task_points=EXCLUDED.task_points,
                   report_points=EXCLUDED.report_points,
                   test_points=EXCLUDED.test_points,
                   reliability_score=EXCLUDED.reliability_score,
                   attention_to_detail_score=EXCLUDED.attention_to_detail_score""",
            (int(staff_id), m, int(task_pts), int(report_pts), int(test_pts), int(reliability), int(attention)),
        )
    else:
        execute(
            """INSERT OR REPLACE INTO performance_index (staff_id, month, task_points, report_points, test_points, reliability_score, attention_to_detail_score)
               VALUES (?,?,?,?,?,?,?)""",
            (int(staff_id), m, int(task_pts), int(report_pts), int(test_pts), int(reliability), int(attention)),
        )


def compute_monthly_base_points(month_start: dt.date) -> pd.DataFrame:
    """Compute per-staff points for the calendar month that contains month_start.

    Scoring rules:
      • Tasks (completed in the selected month):
          - within allotted days  => 3 pts
          - within 1.5× allotted  => 2 pts
          - beyond 1.5×           => 1 pt
      • Bi-weekly reports (APPROVED; due date (report_date) in the selected month):
          - uploaded_at <= report_date          => 3 pts
          - uploaded_at <= report_date + 7 days => 2 pts
          - otherwise                           => 1 pt
        Points are shared across all staff posted to that project.
      • Test results (APPROVED; submitted_at in the selected month):
          - each approved submission => 3 pts (shared across all staff posted to the project)
    """
    ms = dt.date(month_start.year, month_start.month, 1)
    me = (ms + relativedelta(months=1)) - dt.timedelta(days=1)

    def _d(x):
        return _parse_date(x) if x is not None else None

    staff_df = fetch_df("SELECT id, name, rank, section FROM staff ORDER BY name", ())
    if staff_df.empty:
        return pd.DataFrame(columns=[
            "staff_id","name","rank","section",
            "task_points","report_points","test_points","total"
        ])

    acc = {}
    for _, r in staff_df.iterrows():
        sid = int(r["id"])
        acc[sid] = {
            "staff_id": sid,
            "name": r.get("name") or "",
            "rank": r.get("rank") or "",
            "section": r.get("section") or "",
            "task_points": 0,
            "report_points": 0,
            "test_points": 0,
        }

    # TASKS: our schema tracks completion via task_assignments.status + completed_date (no boolean column).
    # Status is set to "Completed" by the UI when staff mark an assignment done.
    task_rows = fetch_df(
        (
            "SELECT ta.staff_id, t.date_assigned, t.days_allotted, ta.completed_date "
            "FROM task_assignments ta "
            "JOIN tasks t ON t.id = ta.task_id "
            "WHERE ta.status = 'Completed'"
        ) if not USE_PG else
        (
            "SELECT ta.staff_id, t.date_assigned, t.days_allotted, ta.completed_date "
            "FROM task_assignments ta "
            "JOIN tasks t ON t.id = ta.task_id "
            "WHERE ta.status = 'Completed'"
        ),
        ()
    )

    for _, r in task_rows.iterrows():
        try:
            sid = int(r.get("staff_id"))
        except Exception:
            continue
        if sid not in acc:
            continue
        cd = _d(r.get("completed_date"))
        if cd is None or cd < ms or cd > me:
            continue
        ad = _d(r.get("date_assigned"))
        allotted = r.get("days_allotted")
        try:
            allotted = int(allotted) if allotted is not None else None
        except Exception:
            allotted = None

        days_taken = None
        if ad is not None:
            days_taken = (cd - ad).days
            if days_taken < 0:
                days_taken = None

        if allotted is None or allotted <= 0 or days_taken is None:
            pts = 1
        else:
            if days_taken <= allotted:
                pts = 3
            elif days_taken <= int(1.5 * allotted):
                pts = 2
            else:
                pts = 1

        acc[sid]["task_points"] += pts

    # REPORTS: approved, due date within month; points to all posted staff
    rpt_rows = fetch_df(
        (
            "SELECT id, project_id, report_date, uploaded_at "
            "FROM biweekly_reports "
            "WHERE approved = 1"
        ) if not USE_PG else
        (
            "SELECT id, project_id, report_date, uploaded_at "
            "FROM biweekly_reports "
            "WHERE COALESCE(approved, FALSE) = TRUE"
        ),
        ()
    )

    for _, r in rpt_rows.iterrows():
        pid = r.get("project_id")
        due = _d(r.get("report_date"))
        up = _d(r.get("uploaded_at"))
        if pid is None or due is None or up is None:
            continue
        if due < ms or due > me:
            continue

        if up <= due:
            pts = 3
        elif up <= (due + dt.timedelta(days=7)):
            pts = 2
        else:
            pts = 1

        posted_df = fetch_df(
            ("SELECT staff_id FROM staff_projects WHERE project_id=?" if not USE_PG else
             "SELECT staff_id FROM staff_projects WHERE project_id=%s"),
            (pid,)
        )
        if posted_df.empty:
            continue
        for sid in posted_df["staff_id"].tolist():
            try:
                sid = int(sid)
            except Exception:
                continue
            if sid in acc:
                acc[sid]["report_points"] += pts

    # TEST RESULTS: approved, submitted within month; points to all posted staff
    test_rows = fetch_df(
        (
            "SELECT id, project_id, submitted_at "
            "FROM test_results "
            "WHERE approved = 1"
        ) if not USE_PG else
        (
            "SELECT id, project_id, submitted_at "
            "FROM test_results "
            "WHERE COALESCE(approved, FALSE) = TRUE"
        ),
        ()
    )

    for _, r in test_rows.iterrows():
        pid = r.get("project_id")
        sub = _d(r.get("submitted_at"))
        if pid is None or sub is None:
            continue
        if sub < ms or sub > me:
            continue

        pts = 3
        posted_df = fetch_df(
            ("SELECT staff_id FROM staff_projects WHERE project_id=?" if not USE_PG else
             "SELECT staff_id FROM staff_projects WHERE project_id=%s"),
            (pid,)
        )
        if posted_df.empty:
            continue
        for sid in posted_df["staff_id"].tolist():
            try:
                sid = int(sid)
            except Exception:
                continue
            if sid in acc:
                acc[sid]["test_points"] += pts

    df = pd.DataFrame(list(acc.values()))
    if df.empty:
        return df
    df["total"] = df[["task_points","report_points","test_points"]].sum(axis=1)
    df = df.sort_values(
        by=["total","task_points","report_points","test_points","name"],
        ascending=[False,False,False,False,True]
    ).reset_index(drop=True)
    return df

def compute_and_store_monthly_performance(month_start: dt.date) -> None:
    """Compute base points for the given month and persist to performance_index.

    Updates task/report/test points while preserving any admin-entered soft-factor scores
    (reliability/attention) already stored for that month.
    """
    df = compute_monthly_base_points(month_start)
    if df is None or df.empty:
        return
    month_key = month_start.strftime('%Y-%m')
    existing = fetch_df('SELECT staff_id, reliability_score, attention_to_detail_score FROM performance_index WHERE month=?', (month_key,))
    soft = {}
    if existing is not None and not existing.empty:
        for _, er in existing.iterrows():
            try:
                soft[int(er['staff_id'])] = (int(er.get('reliability_score') or 0), int(er.get('attention_to_detail_score') or 0))
            except Exception:
                continue
    now_iso = dt.datetime.now().isoformat(timespec='seconds')
    for _, r in df.iterrows():
        sid = int(r['staff_id'])
        rel, att = soft.get(sid, (0, 0))
        base_total = int(r.get('task_points') or 0) + int(r.get('report_points') or 0) + int(r.get('test_points') or 0)
        upsert_performance_index(
            staff_id=sid,
            month=month_key,
            task_points=int(r.get('task_points') or 0),
            report_points=int(r.get('report_points') or 0),
            test_points=int(r.get('test_points') or 0),
            reliability_score=int(rel or 0),
            attention_to_detail_score=int(att or 0),
            total_score=base_total + int(rel or 0) + int(att or 0),
            updated_at=now_iso,
        )


def get_monthly_leaderboard(month:dt.date, include_soft:bool|None=None)->pd.DataFrame:
    ms=str(_month_start(month))
    df=fetch_df(
        """SELECT PI.staff_id, S.name AS name, S.rank AS rank, S.section AS section,
                  PI.task_points, PI.report_points, PI.test_points,
                  PI.reliability_score, PI.attention_to_detail_score
             FROM performance_index PI
             JOIN staff S ON S.id=PI.staff_id
            WHERE PI.month=?""",
        (ms,),
    )
    if df.empty:
        return df
    inc = _perf_include_soft() if include_soft is None else bool(include_soft)
    if inc:
        df["Total Score"] = df[["task_points","report_points","test_points","reliability_score","attention_to_detail_score"]].fillna(0).sum(axis=1)
    else:
        df["Total Score"] = df[["task_points","report_points","test_points"]].fillna(0).sum(axis=1)
    df = df.rename(columns={
        "name":"Name","rank":"Rank","section":"Section",
        "task_points":"Task Points","report_points":"Report Points","test_points":"Test Points",
        "reliability_score":"Reliability","attention_to_detail_score":"Attention to Detail",
    })
    df = df.sort_values(["Total Score","Test Points","Report Points","Task Points","Name"], ascending=[False,False,False,False,True])
    return df

def post_staff_of_month(month:dt.date, force:bool=False)->tuple[bool,str]:
    """Post staff-of-the-month to in-app chat (group). Uses staff_of_month_posts to avoid duplicates."""
    ms=_month_start(month)
    mstr=str(ms)
    already = fetch_df("SELECT 1 FROM staff_of_month_posts WHERE month=?", (mstr,))
    if (not already.empty) and (not force):
        return (False, "Already posted for this month")

    lb = get_monthly_leaderboard(ms, include_soft=None)
    if lb.empty:
        return (False, "No performance records for month")
    top = lb.iloc[0]
    top_sid = int(fetch_df("SELECT id FROM staff WHERE name=? LIMIT 1", (str(top["Name"]),)).iloc[0]["id"]) if True else None
    total=int(top["Total Score"])
    inc=_perf_include_soft()
    month_label=ms.strftime("%B %Y")
    msg=(
        f"🏆 Staff of the Month — {month_label}\n\n"
        f"🥇 {top['Name']} ({top.get('Rank','')})\n"
        f"Total Score: {total} points\n\n"
        f"Breakdown: Tasks {int(top['Task Points'])} | Biweekly Reports {int(top['Report Points'])} | Test Reports {int(top['Test Points'])}"
    )
    if inc:
        msg += f" | Reliability {int(top.get('Reliability',0))} | Attention to Detail {int(top.get('Attention to Detail',0))}"
    msg += "\n\n— WorkNest (Performance Index)"

    # Insert into chat (staff_id nullable in Postgres schema, but NOT NULL in SQLite schema).
    poster_sid = current_staff_id() or 1
    nowiso=dt.datetime.now().isoformat(timespec="seconds")
    if DB_IS_POSTGRES:
        execute("INSERT INTO chat_messages (staff_id, message, created_at) VALUES (?,?,NOW())", (int(poster_sid), msg))
    else:
        execute("INSERT INTO chat_messages (staff_id, message, posted_at) VALUES (?,?,?)", (int(poster_sid), msg, nowiso))

    # Record post
    if DB_IS_POSTGRES:
        if force:
            execute("DELETE FROM staff_of_month_posts WHERE month=?", (mstr,))
        execute("INSERT INTO staff_of_month_posts (month, staff_id, total_score, posted_at) VALUES (?,?,?,?)", (mstr, top_sid, total, nowiso))
    else:
        if force:
            execute("DELETE FROM staff_of_month_posts WHERE month=?", (mstr,))
        execute("INSERT OR REPLACE INTO staff_of_month_posts (month, staff_id, total_score, posted_at) VALUES (?,?,?,?)", (mstr, top_sid, total, nowiso))

    return (True, "Posted")

def smtp_configured()->bool:
    return bool(os.getenv("SMTP_HOST") and os.getenv("SMTP_USER") and os.getenv("SMTP_PASSWORD"))


import secrets

def _hash_token(token:str)->str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()

def create_password_reset_for_user(user_id:int, minutes_valid:int=30)->str:
    token = secrets.token_urlsafe(16)
    expires = (dt.datetime.utcnow() + dt.timedelta(minutes=minutes_valid)).isoformat()
    execute("INSERT INTO password_resets (user_id, token_hash, expires_at, used) VALUES (?,?,?,0)", (user_id, _hash_token(token), expires))
    return token

def consume_password_reset(token:str)->int|None:
    th=_hash_token(token.strip())
    df=fetch_df("SELECT id, user_id, expires_at, used FROM password_resets WHERE token_hash=? ORDER BY id DESC LIMIT 1", (th,))
    if df.empty:
        return None
    row=df.iloc[0]
    if int(row.get("used") or 0)==1:
        return None
    exp=_parse_date_safe(row.get("expires_at"))
    # expires_at has datetime, parse again:
    try:
        expdt=dtparser.parse(str(row.get("expires_at")))
        if expdt < dt.datetime.utcnow():
            return None
    except Exception:
        return None
    execute("UPDATE password_resets SET used=1 WHERE id=?", (int(row["id"]),))
    return int(row["user_id"])

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

# Future-proof hook: if we later introduce a dedicated "reviewer" role, this is where it plugs in.
def is_reviewer():
    return is_admin()

def is_sub_admin():
    return user_role()=='sub_admin' or is_admin()

def is_section_head():
    return user_role()=='section_head' or is_admin()

def can_import_csv():
    return is_admin()

def can_manage_projects():
    # create/edit/delete projects
    return is_admin()

def can_upload_core_docs():
    # Core project documents (drawings, approvals, etc.)
    # Admin always. Sub-admin only when explicitly enabled.
    if is_admin():
        return True
    return (user_role()=='sub_admin') and has_perm('can_upload_project_docs')

def can_assign_tasks():
    # Create/assign tasks
    if is_admin():
        return True
    return (user_role()=='section_head') and has_perm('can_assign_tasks')

def can_confirm_task_completion():
    if is_admin():
        return True
    return (user_role()=='section_head') and has_perm('can_confirm_task_completion')

def can_approve_leave():
    return is_admin()

def current_staff_id():
    u=current_user()
    if not u: return None
    sid=u.get("staff_id")
    try: return int(sid) if sid is not None else None
    except: return None


def current_user_id():
    """Backward-compatible alias for older code paths."""
    return current_staff_id()

def _get_user_permissions(user_id:int)->dict:
    if user_id is None:
        return {"can_assign_tasks":0,"can_confirm_task_completion":0,"can_upload_project_docs":0}
    df = fetch_df("SELECT can_assign_tasks, can_confirm_task_completion, can_upload_project_docs FROM user_permissions WHERE user_id=?", (int(user_id),))
    if df.empty:
        return {"can_assign_tasks":0,"can_confirm_task_completion":0,"can_upload_project_docs":0}
    r = df.iloc[0].to_dict()
    return {
        "can_assign_tasks": int(r.get("can_assign_tasks") or 0),
        "can_confirm_task_completion": int(r.get("can_confirm_task_completion") or 0),
        "can_upload_project_docs": int(r.get("can_upload_project_docs") or 0),
    }

def has_perm(flag:str)->bool:
    u = current_user()
    if not u:
        return False
    if is_admin():
        return True
    perms = _get_user_permissions(int(u.get("id")))
    return int(perms.get(flag) or 0)==1

def current_staff_section()->str|None:
    sid = current_staff_id()
    if sid is None:
        return None
    df = fetch_df("SELECT section FROM staff WHERE id=?", (int(sid),))
    if df.empty:
        return None
    sec = df["section"].iloc[0]
    return str(sec).strip() if sec is not None else None

# ---------- Auth ----------
def login_ui():
    st.markdown(f"<h2 style='text-align:center'>{APP_TITLE}</h2>", unsafe_allow_html=True)
    st.caption("Login with staff <b>email</b> (preferred) or <b>name</b>. Default password is <b>fcda</b>.", unsafe_allow_html=True)
    username=st.text_input("Username (email or name)", key="login_user")
    password=st.text_input("Password", type="password", key="login_pwd")
    remember_me = st.checkbox("Remember me on this device", value=True, key="remember_me")
    if st.button("Login", key="login_btn"):
        u_in = (username or "").strip()
        if not u_in:
            st.error("Enter your email or name.")
            return

        # Allow login using:
        #  - users.username (stored as email for most staff)
        #  - staff.email
        #  - staff.name
        u=fetch_df("""SELECT u.* FROM users u
                       LEFT JOIN staff s ON s.id=u.staff_id
                       WHERE LOWER(u.username)=LOWER(?) OR LOWER(COALESCE(s.email,''))=LOWER(?) OR LOWER(COALESCE(s.name,''))=LOWER(?)
                       LIMIT 1""", (u_in, u_in, u_in))

        if (not u.empty) and int(u["is_active"].iloc[0] if "is_active" in u.columns else 1)==1 and u["password_hash"].iloc[0]==hash_pwd(password):
            st.session_state["user"]=dict(u.iloc[0])
            # Optional persistent login (Remember me)
            if remember_me:
                try:
                    if cookies.ready():
                        raw = secrets.token_urlsafe(32)
                        token_hash = _hash_token(raw)
                        expires = (dt.datetime.utcnow() + dt.timedelta(days=30)).replace(microsecond=0).isoformat()
                        # Best-effort insert; token_hash is UNIQUE
                        execute("INSERT OR IGNORE INTO auth_tokens (user_id, token_hash, expires_at, created_at, last_used_at) VALUES (?,?,?,?,?)",
                                (int(st.session_state["user"]["id"]), token_hash, expires, _utcnow_iso(), _utcnow_iso()))
                        cookies["remember_token"] = raw
                        cookies.save()
                except Exception:
                    pass

            try:
                if int(st.session_state["user"].get("must_change_password") or 0)==1:
                    st.session_state["force_pw_change"]=True
                    st.session_state["nav_radio"]="⚙️ Account"
            except Exception:
                pass
            st.rerun()
        else:
            st.error("Wrong password. Default is 'fcda' unless changed.")

def logout_button():
    if st.sidebar.button("🚪 Logout", key="logout_btn"):
        clear_remember_cookie_and_token()
        st.session_state.pop("user", None); st.rerun()


def sidebar_nav():
    u=current_user()
    st.sidebar.title("📚 Navigation")
    if u: st.sidebar.markdown(f"**User:** {u['username']}  \n**Role:** {user_role()}")
    logout_button()

    # ---- Navigation control (avoid modifying widget state after creation) ----
    # If another page requested a redirect, apply it *before* the radio widget is instantiated.
    _pending_nav = st.session_state.pop("_pending_nav", None)
    if _pending_nav:
        st.session_state["nav_radio"] = _pending_nav

    # If user must change password, force navigation to Account and limit pages.
    try:
        if u and int(u.get("must_change_password") or 0) == 1:
            st.session_state["nav_radio"] = "⚙️ Account"
            forced_pages=["⚙️ Account","❓ Help"]
            return st.sidebar.radio("Go to", forced_pages, key="nav_radio")
    except Exception:
        pass

    base_pages = BASE_PAGES
    admin_pages = ADMIN_PAGES
    pages = base_pages + (admin_pages if is_admin() else [])

    return st.sidebar.radio("Go to", pages, key="nav_radio")

# ---------- Helpers ----------

def is_assigned_to_task(task_id:int, staff_id:int|None=None)->bool:
    sid = staff_id if staff_id is not None else current_staff_id()
    if sid is None: return False
    df=fetch_df("SELECT 1 FROM task_assignments WHERE task_id=? AND staff_id=?", (int(task_id), int(sid)))
    return (not df.empty)



def is_assigned_to_project(project_id:int, staff_id:int|None=None)->bool:
    sid = staff_id if staff_id is not None else current_staff_id()
    if sid is None: return False
    df=fetch_df("SELECT 1 FROM project_staff WHERE project_id=? AND staff_id=?", (int(project_id), int(sid)))
    return (not df.empty)

def can_upload_project_outputs(project_id:int)->bool:
    # Reports + test results + other project outputs
    # Admin anywhere.
    if is_admin():
        return True
    # Sub-admin can upload project documents if explicitly permitted.
    if user_role()=='sub_admin' and has_perm('can_upload_project_docs'):
        return True
    # Otherwise, only staff posted to the project.
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
    sid=current_staff_id()
    admin=is_admin()
    selected = None  # ensure defined for all branches
    col1,col2,col3=st.columns(3)
    projects=fetch_df("SELECT * FROM projects")
    staff=fetch_df("SELECT * FROM staff")
    open_tasks=fetch_df("SELECT * FROM task_assignments WHERE status!='Completed'")
    col1.metric("Projects", len(projects))
    col2.metric("Staff", len(staff))
    col3.metric("Open Tasks", len(open_tasks))

    def project_core_docs_status(pid):
        df=fetch_df("SELECT DISTINCT category FROM documents WHERE project_id=?", (pid, current_staff_id() or -1,))
        present=set(df["category"]) if not df.empty else set()
        missing=[c for c in CORE_DOC_CATEGORIES if c not in present]
        return present, missing

    def project_next_due(pid, start_date, next_due_date=None):
        # If a next_due_date is set on the project, it becomes the authoritative schedule anchor.
        try:
            nd = None
            if next_due_date is not None and not (isinstance(next_due_date, float) and pd.isna(next_due_date)):
                s = str(next_due_date).strip()
                if s and s.lower() not in ("nan","none","null"):
                    nd = dtparser.parse(s).date()
            if nd is not None:
                # Still fetch last submitted report date for context
                last = fetch_df("SELECT MAX(COALESCE(uploaded_at, report_date)) d FROM biweekly_reports WHERE project_id=?", (pid,))
                last_d = None
                try:
                    if (not last.empty) and ("d" in last.columns):
                        raw = last["d"].iloc[0]
                        if raw is not None and str(raw).strip().lower() not in ("", "nan", "none", "null"):
                            last_d = dtparser.parse(str(raw)).date()
                except Exception:
                    last_d = None
                return (date.today() > nd, last_d, nd, None)
        except Exception:
            pass

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

        last = fetch_df("SELECT MAX(COALESCE(uploaded_at, report_date)) d FROM biweekly_reports WHERE project_id=?", (pid,))
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


    # Action items: due/overdue project reports + tasks (no birthdays here 🙏)
    st.markdown("### ✅ Action Items (Due / Overdue)")
    items=[]
    today=date.today()
    horizon=today+timedelta(days=7)

    # 1) Bi-weekly reports / project outputs due for projects you are posted to
    if admin:
        proj_due=fetch_df("SELECT id,code,name,start_date,next_due_date FROM projects ORDER BY code")
    else:
        proj_due=fetch_df("SELECT P.id,P.code,P.name,P.start_date,P.next_due_date FROM projects P JOIN project_staff PS ON PS.project_id=P.id WHERE PS.staff_id=? ORDER BY P.code", (sid,))

    for _,p in proj_due.iterrows():
        pid=int(p["id"])
        overdue,last_d,exp,reason = project_next_due(pid, p.get("start_date"), p.get("next_due_date"))
        if exp is None: continue
        if overdue or (exp - today).days <= 7:
            status = "Overdue" if overdue else "Due soon"
            items.append({
                "type":"Project report",
                "item":f"{p.get('code','')} — {p.get('name','')}",
                "due":exp.isoformat(),
                "status":status,
                "details":reason
            })

    # 2) Tasks due / overdue assigned to you
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
                status = "Overdue" if due < today else "Due soon"
                pfx = (t.get("project_code") or "").strip()
                items.append({
                    "type":"Task",
                    "item":f"{pfx} — {t.get('title','')}" if pfx else str(t.get('title') or ""),
                    "due":due.isoformat(),
                    "status":status,
                    "details":f"Task ID {t.get('id')}"
                })

    if items:
        df_items=pd.DataFrame(items)
        df_items["__due_sort"]=pd.to_datetime(df_items["due"], errors="coerce")
        df_items["__status_sort"]=df_items["status"].map(lambda s: 0 if s=="Overdue" else 1)
        df_items=df_items.sort_values(["__status_sort","__due_sort","type","item"]).drop(columns=["__due_sort","__status_sort"])
        st.dataframe(df_items, hide_index=True, use_container_width=True)
    else:
        st.success("No due/overdue items in the next 7 days.")

    # --- Project Quick Edit (Admin only) ---
    st.markdown("### Project Quick Edit")
    pdf = fetch_df("SELECT id,code,name,client,location,start_date,end_date,supervisor_staff_id FROM projects ORDER BY code")
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
    sup_options = [""] + sorted([n for n in staff["name"].dropna().tolist() if str(n).strip()])

    default_sup = ""
    if selected is not None and selected.get("supervisor_staff_id"):
        try:
            default_sup = sup_name_by_id.get(int(selected["supervisor_staff_id"]), "") or ""
        except Exception:
            default_sup = ""

    if can_manage_projects:
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
                            execute("""INSERT INTO test_results (project_id,building_id,stage,test_type,batch_id,file_path,uploaded_at,uploader_staff_id,status)
                                       VALUES (?,?,?,?,?,?,?,?,?)""",(pid, bid, stage, ttype, batch_id, path, datetime.now().isoformat(timespec="seconds"), current_staff_id(), "PENDING"))
                            st.success("Test uploaded.")
                        else:
                            st.error("Select a file first.")

            # List
            tdf=fetch_df("""
                SELECT tr.id, b.name AS building, tr.stage, tr.test_type, tr.batch_id, tr.file_path, tr.uploaded_at, COALESCE(tr.status,'APPROVED') AS status
                FROM test_results tr
                LEFT JOIN buildings b ON b.id=tr.building_id
                WHERE tr.project_id=?
                  AND (COALESCE(tr.status,'APPROVED')='APPROVED' OR tr.uploader_staff_id=?)
                ORDER BY tr.uploaded_at DESC
            """,(pid, current_staff_id() or -1,))
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
                        st.markdown(f"<span class='pill'>{r['status']}</span>", unsafe_allow_html=True)
                        if is_admin():
                            a1,a2,a3 = st.columns([1,1,1])
                            with a1:
                                if r['status']!='APPROVED' and st.button("✅ Approve", key=f"tapp{r['id']}"):
                                    execute("UPDATE test_results SET status='APPROVED', reviewed_by_staff_id=?, reviewed_at=? WHERE id=?",
                                            (current_staff_id(), datetime.now().isoformat(timespec="seconds"), int(r["id"])))
                                    st.rerun()
                            with a2:
                                if r['status']!='REJECTED' and st.button("✖ Reject", key=f"trej{r['id']}"):
                                    execute("UPDATE test_results SET status='REJECTED', reviewed_by_staff_id=?, reviewed_at=? WHERE id=?",
                                            (current_staff_id(), datetime.now().isoformat(timespec="seconds"), int(r["id"])))
                                    st.rerun()
                            with a3:
                                if st.button("🗑️ Delete", key=f"tdel{r['id']}"):
                                    # remove file too
                                    try:
                                        fp=str(r.get("file_path") or "")
                                        if fp and os.path.exists(fp): os.remove(fp)
                                    except Exception:
                                        pass
                                    execute("DELETE FROM test_results WHERE id=?", (int(r["id"]),))
                                    st.rerun()

        # Biweekly Reports
        with tabs[3]:
            st.subheader("Biweekly Reports")
            allowed = can_upload_project_outputs(pid)
            st.markdown(f"Upload permission: <span class='pill'>{'Yes' if allowed else 'No'}</span>", unsafe_allow_html=True)
            rdate = st.date_input("Report Period Date", value=date.today(), key="bw_date")
            st.caption(f"Submitted at (auto): {datetime.now().strftime('%Y-%m-%d %H:%M')}")
            up = st.file_uploader("Upload biweekly report (PDF/Image)", type=["pdf","png","jpg","jpeg"], key="bw_file")
            if st.button("⬆️ Upload Report", key="bw_up"):
                if not allowed:
                    st.error("You don't have permission to upload to this project.")
                else:
                    path=save_uploaded_file(up, f"project_{pid}/reports")
                    if path:
                        rid = execute(
                            "INSERT INTO biweekly_reports (project_id,report_date,uploaded_at,file_path,uploader_staff_id,status) VALUES (?,?,?,?,?,?)",
                            (pid, str(rdate), datetime.now().isoformat(timespec="seconds"), path, current_staff_id(), "PENDING"),
                        )
                        st.success("Report uploaded and queued for Admin approval.")
                    else:
                        st.error("Select a file first.")
            # Staff should only see APPROVED reports (plus their own pending uploads).
            # Admin should see everything so they can approve/reject pending uploads.
            if is_admin():
                rdf=fetch_df(
                    "SELECT id,report_date,uploaded_at,file_path, COALESCE(status,'APPROVED') AS status, uploader_staff_id "
                    "FROM biweekly_reports WHERE project_id=? "
                    "ORDER BY date(COALESCE(uploaded_at,report_date)) DESC",
                    (pid,)
                )
            else:
                rdf=fetch_df(
                    "SELECT id,report_date,uploaded_at,file_path, COALESCE(status,'APPROVED') AS status, uploader_staff_id "
                    "FROM biweekly_reports WHERE project_id=? AND (COALESCE(status,'APPROVED')='APPROVED' OR uploader_staff_id=?) "
                    "ORDER BY date(COALESCE(uploaded_at,report_date)) DESC",
                    (pid, current_staff_id())
                )
            if rdf.empty:
                st.info("No reports yet.")
            else:
                for _,r in rdf.iterrows():
                    colA,colB,colC = st.columns([3,1,2])
                    sub = r['uploaded_at'] if pd.notna(r.get('uploaded_at')) and str(r.get('uploaded_at')).strip() else r['report_date']
                    with colA:
                        st.markdown(f"**Period:** {r['report_date']}  \n**Submitted:** {sub}  \n{os.path.basename(r['file_path'])}")
                    with colB:
                        file_download_button("⬇️ Download", r["file_path"], key=f"bw{r['id']}")
                    with colC:
                        st.markdown(f"<span class='pill'>{r['status']}</span>", unsafe_allow_html=True)
                        if is_admin():
                            c1,c2,c3 = st.columns([1,1,1])
                            with c1:
                                if r['status']!='APPROVED' and st.button("✅ Approve", key=f"bapp{r['id']}"):
                                    execute("UPDATE biweekly_reports SET status='APPROVED', reviewed_by_staff_id=?, reviewed_at=? WHERE id=?",
                                            (current_staff_id(), datetime.now().isoformat(timespec="seconds"), int(r['id'])))
                                    try:
                                        pdue = _parse_date_safe(r.get('report_date'))
                                        if pdue:
                                            execute("UPDATE projects SET next_due_date=? WHERE id=?", (str(pdue + timedelta(days=14)), int(pid)))
                                    except Exception:
                                        pass
                                    st.rerun()
                            with c2:
                                if r['status']!='REJECTED' and st.button("✖ Reject", key=f"brej{r['id']}"):
                                    execute("UPDATE biweekly_reports SET status='REJECTED', reviewed_by_staff_id=?, reviewed_at=? WHERE id=?",
                                            (current_staff_id(), datetime.now().isoformat(timespec="seconds"), int(r['id'])))
                                    st.rerun()
                            with c3:
                                if st.button("🗑️ Delete", key=f"bdel{r['id']}"):
                                    try:
                                        fp=str(r.get("file_path") or "")
                                        if fp and os.path.exists(fp): os.remove(fp)
                                    except Exception:
                                        pass
                                    execute("DELETE FROM biweekly_reports WHERE id=?", (int(r['id']),))
                                    st.rerun()

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


def page_chat():
    st.header('💬 General Chat')
    sid=current_staff_id()
    if sid is None:
        st.info('Please login to use chat.')
        return

    with st.form('chat_send', clear_on_submit=True):
        msg=st.text_area('Message', height=90, placeholder='Type your message…')
        img=st.file_uploader('Optional image', type=['png','jpg','jpeg','webp','gif'])
        sent=st.form_submit_button('Send')
        if sent:
            m=(msg or '').strip()
            if not m and img is None:
                st.warning('Type a message or attach an image.')
            else:
                image_path=None
                if img is not None:
                    ext=os.path.splitext(img.name)[1].lower()
                    chat_dir=os.path.join(UPLOAD_DIR,'chat')
                    os.makedirs(chat_dir, exist_ok=True)
                    fname=f"{uuid.uuid4().hex}{ext}"
                    disk_path=os.path.join(chat_dir,fname)
                    with open(disk_path,'wb') as f: f.write(img.getbuffer())
                    image_path=disk_path
                attachment_path=None
                attachment_name=None
                attachment_type=None
                execute('INSERT INTO chat_messages (staff_id,message,image_path,attachment_path,attachment_name,attachment_type) VALUES (?,?,?,?,?,?)', (sid, m if m else None, image_path, attachment_path, attachment_name, attachment_type))
                st.success('Sent')
                st.rerun()

    df=fetch_df("""
        SELECT C.id, C.message, C.image_path, C.created_at, COALESCE(S.name,'(Unknown)') AS staff_name
        FROM chat_messages C
        LEFT JOIN staff S ON S.id = C.staff_id
        ORDER BY C.created_at DESC
        LIMIT 80
    """)

    if df.empty:
        st.info('No messages yet. Say hi 👋')
        return

    # show oldest -> newest
    for r in (df.to_dict('records')):
        ts=str(r.get('created_at') or '')
        st.markdown(f"**{r.get('staff_name','(Unknown)')}**  \n{ts}")
        if r.get('message'):
            st.write(r['message'])
        if r.get('image_path') and os.path.exists(r['image_path']):
            st.image(r['image_path'])
        st.divider()

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
def page_projects():
    st.markdown("<div class='worknest-header'><h2>🏗️ Projects</h2></div>", unsafe_allow_html=True)
    # For Staff: show posted projects first, then the rest.
    if is_admin() or can_manage_projects():
        projects=fetch_df("""
            SELECT p.id, p.code, p.name, p.client, p.location, p.start_date, p.end_date, p.next_due_date, p.supervisor_staff_id,
                   (SELECT name FROM staff s WHERE s.id=p.supervisor_staff_id) supervisor
            FROM projects p
            ORDER BY p.code
        """)
    else:
        sid = current_staff_id()
        projects=fetch_df("""
            SELECT p.id, p.code, p.name, p.client, p.location, p.start_date, p.end_date, p.next_due_date, p.supervisor_staff_id,
                   (SELECT name FROM staff s WHERE s.id=p.supervisor_staff_id) supervisor,
                   CASE WHEN EXISTS (
                        SELECT 1 FROM project_staff ps
                        WHERE ps.project_id = p.id AND ps.staff_id = ?
                   ) THEN 0 ELSE 1 END AS _posted_sort
            FROM projects p
            ORDER BY _posted_sort, p.code
        """, (sid,))

    # Admin control: global reset of next bi-weekly due dates
    if is_admin():
        with st.expander("🛠️ Admin: Reset bi-weekly next due date (global)", expanded=False):
            st.caption("This overwrites **projects.next_due_date** for all projects. Use it when you want everyone aligned to the same reporting cycle.")
            reset_date = st.date_input("Set NEXT due date for all projects", value=date(2026,2,26), key="global_due_reset")
            if st.button("Apply global reset", key="btn_global_due_reset"):
                execute("UPDATE projects SET next_due_date=?", (str(reset_date),))
                execute("""INSERT INTO app_settings(key,value) VALUES(?,?)
                           ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value""", ("GLOBAL_BIWEEKLY_NEXT_DUE", str(reset_date)))
                st.success(f"Updated all projects: next due date set to {reset_date}.")
                st.rerun()

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

        # Streamlit widgets keep values by key; so ensure keys vary by selected project.
        # This makes the right-side panel instantly reflect the selected project.
        suffix = f"_{int(selected['id'])}" if selected is not None else "_new"

        staff=fetch_df("SELECT id,name,section FROM staff ORDER BY name")
        sup_names=["—"]+[s for s in staff["name"].tolist()] if not staff.empty else ["—"]
        code=st.text_input("Code", value=(selected["code"] if selected is not None else ""), key=f"proj_code{suffix}")
        name=st.text_input("Name", value=(selected["name"] if selected is not None else ""), key=f"proj_name{suffix}")
        client=st.text_input("Client", value=(selected["client"] if selected is not None and pd.notna(selected["client"]) else ""), key=f"proj_client{suffix}")
        location=st.text_input("Location", value=(selected["location"] if selected is not None and pd.notna(selected["location"]) else ""), key=f"proj_loc{suffix}")
        start=st.date_input("Start Date", value=(dtparser.parse(selected["start_date"]).date() if selected is not None and pd.notna(selected["start_date"]) else date.today()), key=f"proj_start{suffix}")
        end=st.date_input("End Date", value=(dtparser.parse(selected["end_date"]).date() if selected is not None and pd.notna(selected["end_date"]) else date.today()), key=f"proj_end{suffix}")
        sup_default = selected["supervisor"] if (selected is not None and pd.notna(selected["supervisor"])) else "—"
        sup_name=st.selectbox("Supervisor", sup_names, index=sup_names.index(sup_default) if sup_default in sup_names else 0, key=f"proj_sup{suffix}")
        colA,colB=st.columns(2)
        with colA:
            if can_manage_projects() and st.button("💾 Save / Update", key=f"proj_save{suffix}"):
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
            if (selected is not None) and st.button("🗑️ Delete", key=f"proj_del{suffix}"):
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
                            execute("""INSERT INTO test_results (project_id,building_id,stage,test_type,batch_id,file_path,uploaded_at,uploader_staff_id,status)
                                       VALUES (?,?,?,?,?,?,?,?,?)""",(pid, bid, stage, ttype, batch_id, path, datetime.now().isoformat(timespec="seconds"), current_staff_id(), "PENDING"))
                            st.success("Test uploaded.")
                        else:
                            st.error("Select a file first.")

            # List
            tdf=fetch_df("""
                SELECT tr.id, b.name AS building, tr.stage, tr.test_type, tr.batch_id, tr.file_path, tr.uploaded_at, COALESCE(tr.status,'APPROVED') AS status
                FROM test_results tr
                LEFT JOIN buildings b ON b.id=tr.building_id
                WHERE tr.project_id=?
                  AND (COALESCE(tr.status,'APPROVED')='APPROVED' OR tr.uploader_staff_id=?)
                ORDER BY tr.uploaded_at DESC
            """,(pid, current_staff_id() or -1,))
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
                        st.markdown(f"<span class='pill'>{r['status']}</span>", unsafe_allow_html=True)
                        if is_admin():
                            a1,a2,a3 = st.columns([1,1,1])
                            with a1:
                                if r['status']!='APPROVED' and st.button("✅ Approve", key=f"tapp{r['id']}"):
                                    execute("UPDATE test_results SET status='APPROVED', reviewed_by_staff_id=?, reviewed_at=? WHERE id=?",
                                            (current_staff_id(), datetime.now().isoformat(timespec="seconds"), int(r["id"])))
                                    st.rerun()
                            with a2:
                                if r['status']!='REJECTED' and st.button("✖ Reject", key=f"trej{r['id']}"):
                                    execute("UPDATE test_results SET status='REJECTED', reviewed_by_staff_id=?, reviewed_at=? WHERE id=?",
                                            (current_staff_id(), datetime.now().isoformat(timespec="seconds"), int(r["id"])))
                                    st.rerun()
                            with a3:
                                if st.button("🗑️ Delete", key=f"tdel{r['id']}"):
                                    # remove file too
                                    try:
                                        fp=str(r.get("file_path") or "")
                                        if fp and os.path.exists(fp): os.remove(fp)
                                    except Exception:
                                        pass
                                    execute("DELETE FROM test_results WHERE id=?", (int(r["id"]),))
                                    st.rerun()

        # Biweekly Reports
        with tabs[3]:
            st.subheader("Biweekly Reports")
            allowed = can_upload_project_outputs(pid)
            st.markdown(f"Upload permission: <span class='pill'>{'Yes' if allowed else 'No'}</span>", unsafe_allow_html=True)
            rdate = st.date_input("Report Period Date", value=date.today(), key="bw_date")
            st.caption(f"Submitted at (auto): {datetime.now().strftime('%Y-%m-%d %H:%M')}")
            up = st.file_uploader("Upload biweekly report (PDF/Image)", type=["pdf","png","jpg","jpeg"], key="bw_file")
            if st.button("⬆️ Upload Report", key="bw_up"):
                if not allowed:
                    st.error("You don't have permission to upload to this project.")
                else:
                    path=save_uploaded_file(up, f"project_{pid}/reports")
                    if path:
                        rid = execute("INSERT INTO biweekly_reports (project_id,report_date,file_path,uploader_staff_id) VALUES (?,?,?,?)",
                                      (pid, str(rdate), path, current_staff_id()))
                        try:
                            execute("UPDATE projects SET next_due_date=? WHERE id=?", (str(rdate + timedelta(days=14)), int(pid)))
                        except Exception:
                            pass
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
            rdf=fetch_df("SELECT id,report_date,uploaded_at,file_path, COALESCE(status,'APPROVED') AS status, uploader_staff_id FROM biweekly_reports WHERE project_id=? AND (COALESCE(status,'APPROVED')='APPROVED' OR uploader_staff_id=?) ORDER BY date(COALESCE(uploaded_at,report_date)) DESC",(pid, current_staff_id()))
            if rdf.empty:
                st.info("No reports yet.")
            else:
                for _,r in rdf.iterrows():
                    colA,colB,colC = st.columns([3,1,2])
                    sub = r['uploaded_at'] if pd.notna(r.get('uploaded_at')) and str(r.get('uploaded_at')).strip() else r['report_date']
                    with colA:
                        st.markdown(f"**Period:** {r['report_date']}  \n**Submitted:** {sub}  \n{os.path.basename(r['file_path'])}")
                    with colB:
                        file_download_button("⬇️ Download", r["file_path"], key=f"bw{r['id']}")
                    with colC:
                        st.markdown(f"<span class='pill'>{r['status']}</span>", unsafe_allow_html=True)
                        if is_admin():
                            c1,c2,c3 = st.columns([1,1,1])
                            with c1:
                                if r['status']!='APPROVED' and st.button("✅ Approve", key=f"bapp{r['id']}"):
                                    execute("UPDATE biweekly_reports SET status='APPROVED', reviewed_by_staff_id=?, reviewed_at=? WHERE id=?",
                                            (current_staff_id(), datetime.now().isoformat(timespec="seconds"), int(r['id'])))
                                    try:
                                        pdue = _parse_date_safe(r.get('report_date'))
                                        if pdue:
                                            execute("UPDATE projects SET next_due_date=? WHERE id=?", (str(pdue + timedelta(days=14)), int(pid)))
                                    except Exception:
                                        pass
                                    st.rerun()
                            with c2:
                                if r['status']!='REJECTED' and st.button("✖ Reject", key=f"brej{r['id']}"):
                                    execute("UPDATE biweekly_reports SET status='REJECTED', reviewed_by_staff_id=?, reviewed_at=? WHERE id=?",
                                            (current_staff_id(), datetime.now().isoformat(timespec="seconds"), int(r['id'])))
                                    st.rerun()
                            with c3:
                                if st.button("🗑️ Delete", key=f"bdel{r['id']}"):
                                    try:
                                        fp=str(r.get("file_path") or "")
                                        if fp and os.path.exists(fp): os.remove(fp)
                                    except Exception:
                                        pass
                                    execute("DELETE FROM biweekly_reports WHERE id=?", (int(r['id']),))
                                    st.rerun()
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
    staff=fetch_df("SELECT id,name,section FROM staff ORDER BY name")
    projects=fetch_df("SELECT id,code,name,start_date,next_due_date FROM projects ORDER BY code")
    st.subheader("Tasks")
    titles=fetch_df("SELECT id,title FROM tasks ORDER BY id DESC")
    mode_options = ["Edit existing"] if not can_assign_tasks() else ["Create new","Edit existing"]
    mode=st.radio("Mode", mode_options, horizontal=True, key="tsk_mode")

    if (not can_assign_tasks()) and mode=="Create new":
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
        # Ensure widget keys vary by selected task so the form reflects the selected task immediately.
        tkey = f"_{int(tid)}"
        can_edit = can_assign_tasks()
        # Section Heads are restricted to their section only
        if (not is_admin()) and user_role()=='section_head':
            sec = current_staff_section()
            if sec:
                staff_allowed = staff[staff["section"].fillna("").str.strip()==sec].copy()
            else:
                staff_allowed = staff.iloc[0:0].copy()
        else:
            staff_allowed = staff.copy()

        # If this task already includes assignees outside section, section head cannot edit it.
        if (not is_admin()) and user_role()=='section_head':
            existing_ass = fetch_df("SELECT s.section FROM task_assignments ta JOIN staff s ON s.id=ta.staff_id WHERE ta.task_id=?", (tid,))
            if (not existing_ass.empty):
                sec = current_staff_section() or ""
                bad = existing_ass["section"].fillna("").str.strip().apply(lambda x: x!=sec).any()
                if bad:
                    can_edit = False
        title=st.text_input("Title", value=trow["title"], key=f"tsk_title{tkey}", disabled=not can_edit)
        desc=st.text_area("Description", value=trow["description"] or "", key=f"tsk_desc{tkey}", disabled=not can_edit)
        date_assigned=st.date_input("Date assigned", value=dtparser.parse(trow["date_assigned"]).date(), key=f"tsk_da{tkey}", disabled=not can_edit)
        due=st.date_input("Due date", value=dtparser.parse(trow["due_date"]).date(), key=f"tsk_due{tkey}", disabled=not can_edit)
        da=int(max((due - date_assigned).days + 1, 1))
        st.write(f"Days allotted (auto): **{da}**")
        proj_opt=["—"]+[f"{r['code']} — {r['name']}" for _,r in projects.iterrows()]
        proj_value="—"
        if pd.notna(trow["project_id"]):
            pr=projects[projects["id"]==int(trow["project_id"])]
            if not pr.empty: proj_value=f"{pr['code'].iloc[0]} — {pr['name'].iloc[0]}"
        proj=st.selectbox("Project (optional)", proj_opt, index=proj_opt.index(proj_value) if proj_value in proj_opt else 0, key=f"tsk_proj{tkey}", disabled=not can_edit)
        assignees=st.multiselect(
            "Assignees",
            staff_allowed["name"].tolist(),
            key=f"tsk_asg{tkey}",
            default=fetch_df("SELECT name FROM task_assignments ta JOIN staff s ON s.id=ta.staff_id WHERE ta.task_id=?", (tid,))["name"].tolist(),
            disabled=not can_edit,
        )
        colA,colB,colC=st.columns(3)
        with colA:
            if st.button("💾 Save", key=f"tsk_save{tkey}", disabled=not can_edit):
                if not can_edit:
                    st.warning("You don't have permission to edit this task.")
                else:
                    execute("UPDATE tasks SET title=?,description=?,date_assigned=?,days_allotted=?,due_date=?,project_id=? WHERE id=?",
                        (title, desc or None, str(date_assigned), int(da), str(due),
                         int(projects[projects['code']==proj.split(' — ')[0]]['id'].iloc[0]) if proj!="—" else None, tid))
                    execute("DELETE FROM task_assignments WHERE task_id=?", (tid,))
                    for nm in assignees:
                        sid=int(staff[staff["name"]==nm]["id"].iloc[0])
                        execute("INSERT INTO task_assignments (task_id,staff_id,status) VALUES (?,?,?)",(tid,sid,"In progress"))
                    st.success("Task updated."); st.rerun()
        with colB:
            # Completion workflow: only Admin or permitted Section Heads can confirm completion.
            can_confirm = can_confirm_task_completion()
            btn_label = "✅ Confirm Completed (today)" if not is_admin() else "✅ Admin: Certify Completed (today)"
            if st.button(btn_label, key=f"tsk_done{tkey}", disabled=not can_confirm):
                if not can_confirm:
                    st.warning("You don't have permission to confirm completion.")
                else:
                    # Section heads may only confirm tasks within their section
                    if (not is_admin()) and user_role()=='section_head':
                        sec = current_staff_section() or ""
                        chk = fetch_df("""SELECT s.section FROM task_assignments ta
                                           JOIN staff s ON s.id=ta.staff_id
                                           WHERE ta.task_id=?""", (tid,))
                        if (not chk.empty) and chk["section"].fillna("").str.strip().apply(lambda x: x!=sec).any():
                            st.error("You can only confirm completion for tasks assigned within your section.")
                            st.stop()

                    today_d=date.today()
                    today=str(today_d)
                    da=_parse_date_safe(trow["date_assigned"]) or today_d
                    ass=fetch_df("SELECT id, staff_id FROM task_assignments WHERE task_id=?", (tid,))
                    for _,ar in ass.iterrows():
                        days_taken=int((today_d - da).days)
                        execute("UPDATE task_assignments SET status='Completed', completed_date=?, days_taken=? WHERE id=?",
                                (today, days_taken, int(ar["id"])))
                        # Legacy points table (kept for backward compatibility)
                        try:
                            execute("INSERT OR IGNORE INTO points (staff_id, source, source_id, points, awarded_at) VALUES (?,?,?,?,?)",
                                    (int(ar["staff_id"]), "task", int(ar["id"]), 5, datetime.now().isoformat(timespec="seconds")))
                        except Exception:
                            pass
                    st.success("Completion confirmed for all assignees."); st.rerun()

            # Delete remains Admin-only
            if is_admin():
                if st.button("🗑️ Admin: Delete Task", key=f"tsk_del{tkey}"):
                    execute("DELETE FROM task_assignments WHERE task_id=?", (tid,))
                    execute("DELETE FROM tasks WHERE id=?", (tid,))
                    st.success("Task deleted."); st.rerun()
            else:
                st.caption("Delete is Admin-only.")
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
        # Create new task (Admin or permitted Section Heads). Section Heads are restricted to their section only.
        if (not is_admin()) and user_role()=='section_head':
            sec = current_staff_section()
            if sec:
                staff_allowed_new = staff[staff["section"].fillna("").str.strip()==sec].copy()
            else:
                staff_allowed_new = staff.iloc[0:0].copy()
        else:
            staff_allowed_new = staff.copy()

        title=st.text_input("Title", key="tsk_title_new")
        desc=st.text_area("Description", key="tsk_desc_new")
        date_assigned=st.date_input("Date assigned", value=date.today(), key="tsk_da_new")
        due=st.date_input("Due date", value=date.today()+timedelta(days=7), key="tsk_due_new")
        da=int(max((due - date_assigned).days + 1, 1))
        st.write(f"Days allotted (auto): **{da}**")
        proj_opt=["—"]+[f"{r['code']} — {r['name']}" for _,r in projects.iterrows()]
        proj=st.selectbox("Project (optional)", proj_opt, key="tsk_proj_new")
        assignees=st.multiselect("Assignees", staff_allowed_new["name"].tolist(), key="tsk_asg_new")
        if can_assign_tasks() and st.button("➕ Create Task", key="tsk_create"):
            pid = int(projects[projects['code']==proj.split(' — ')[0]]['id'].iloc[0]) if proj!="—" else None
            tid=execute("INSERT INTO tasks (title,description,date_assigned,days_allotted,due_date,project_id,created_by_staff_id) VALUES (?,?,?,?,?,?,?)",
                        (title, desc or None, str(date_assigned), int(da), str(due), pid, current_staff_id()))
            for nm in assignees:
                sid=int(staff[staff["name"]==nm]["id"].iloc[0])
                execute("INSERT INTO task_assignments (task_id,staff_id,status) VALUES (?,?,?)",(tid,sid,"In progress"))
            # Push notifications to assignees (best-effort)
            try:
                if "email" in staff.columns:
                    emails = staff[staff["name"].isin(assignees)]["email"].dropna().astype(str).tolist()
                else:
                    emails = []
                if emails:
                    send_push(emails, "WorkNest: New Task", f"You have been assigned: {title[:80]}")
            except Exception:
                pass
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

    st.divider()
    st.subheader("📊 Cumulative Performance Scoreboard")
    st.caption("Scores are aggregated directly from the points ledger (single source of truth).")

    perf = fetch_df("""
        SELECT
            s.id,
            s.name,
            s.rank,
            s.section,
            COALESCE(SUM(CASE WHEN p.source = 'task' THEN p.points END), 0) AS task_points,
            COALESCE(SUM(CASE WHEN p.source IN ('biweekly','report') THEN p.points END), 0) AS report_points,
            COALESCE(SUM(CASE WHEN p.source = 'test' THEN p.points END), 0) AS test_points,
            COALESCE(SUM(p.points), 0) AS total_score
        FROM staff s
        LEFT JOIN points p ON p.staff_id = s.id
        GROUP BY s.id, s.name, s.rank, s.section
        ORDER BY total_score DESC, s.name ASC;
    """)

    if perf.empty:
        st.info("No points recorded yet.")
    else:
        st.dataframe(
            perf[[
                "name",
                "rank",
                "section",
                "task_points",
                "report_points",
                "test_points",
                "total_score",
            ]],
            use_container_width=True,
        )
        winner = perf.iloc[0]
        st.success(f"🏆 Top performer (cumulative): **{winner['name']}** — {int(winner['total_score'])} points")


def page_admin_inbox():
    """Central approval queue for uploads across all projects.

    This reduces the risk of staff uploading rubbish to farm points, because performance points
    are computed from APPROVED uploads only.
    """
    if not is_admin():
        st.warning("Admin only.")
        return

    st.title("📥 Admin Inbox")
    st.caption("Pending uploads awaiting approval. Approve to count for performance points.")

    # Quick reminder about persistence
    if (ENV_DATA_DIR or "").startswith("/tmp") and os.path.isdir(RENDER_DISK_DIR):
        st.warning(
            "Your WORKNEST_DATA_DIR is pointing to /tmp (ephemeral). "
            "Set it to /var/data/worknest_data in Render to persist uploads across redeploys."
        )

    tab_reports, tab_tests = st.tabs(["Biweekly Reports","Test Results"])

    def _render_queue(df, kind: str):
        if df is None or df.empty:
            st.success("No pending items.")
            return

        for _, r in df.iterrows():
            rid = int(r.get("id") or 0)
            pid = int(r.get("project_id") or 0)
            code = r.get("project_code") or ""
            pname = r.get("project_name") or ""
            uploader_email = (r.get("uploader_email") or "").strip()
            uploader = uploader_email or (r.get("uploader_name") or "")
            status = r.get("status") or "PENDING"
            uploaded_at = r.get("uploaded_at") or ""
            period_dt = r.get("report_date") or r.get("test_date") or ""
            period_str = str(period_dt)[:10] if period_dt else ""
            file_path = r.get("file_path") or ""

            with st.container(border=True):
                st.markdown(f"**{code}** — {pname}")
                st.write(f"**Uploader:** {uploader} · **Status:** {status} · **Period/Test date:** {period_dt} · **Uploaded:** {uploaded_at}")

                if file_path and not os.path.exists(file_path):
                    st.warning(f"Missing file on disk: {file_path}")

                c1, c2, c3, c4 = st.columns([1, 1, 1, 3])
                with c1:
                    if st.button("✅ Approve", key=f"inbox_{kind}_approve_{rid}"):
                        ts = dt.datetime.utcnow().isoformat(sep=' ', timespec='seconds')
                        if kind == "report":
                            execute_sql(
                                "UPDATE biweekly_reports SET status='APPROVED', reviewed_at=?, reviewed_by_staff_id=? WHERE id=?",
                                # reviewed_by_staff_id expects the Staff.id (not Users.id)
                                (ts, current_staff_id(), rid),
                            )
                        elif kind == "test":
                            execute_sql(
                                "UPDATE test_results SET status='APPROVED', reviewed_at=?, reviewed_by_staff_id=? WHERE id=?",
                                (ts, current_staff_id(), rid),
                            )
                        # Push notifications (best-effort)
                        try:
                            notify = []
                            if uploader_email:
                                notify.append(uploader_email)
                            if pid:
                                notify += _staff_emails_for_project(pid)
                            send_push(notify, "WorkNest: Approved", f"{kind.title()} approved for {code} ({period_str}).")
                        except Exception:
                            pass

                        # Keep performance table aligned (best-effort)
                        try:
                            if period_str:
                                ms = dt.datetime.strptime(period_str, "%Y-%m-%d").date().replace(day=1)
                                compute_and_store_monthly_performance(ms)
                            # For tests, scores are based on submitted_at; also recompute that month if available.
                            if kind == "test" and row.get("submitted_at"):
                                try:
                                    sub_date = _parse_date(row.get("submitted_at"))
                                    if sub_date:
                                        compute_and_store_monthly_performance(sub_date.replace(day=1))
                                except Exception:
                                    pass
                        except Exception:
                            pass
                        st.success("Approved.")
                        st.rerun()
                with c2:
                    if st.button("⛔ Reject", key=f"inbox_{kind}_reject_{rid}"):
                        ts = dt.datetime.utcnow().isoformat(sep=' ', timespec='seconds')
                        if kind == "report":
                            execute_sql(
                                "UPDATE biweekly_reports SET status='REJECTED', reviewed_at=?, reviewed_by_staff_id=? WHERE id=?",
                                (ts, current_staff_id(), rid),
                            )
                        elif kind == "test":
                            execute_sql(
                                "UPDATE test_results SET status='REJECTED', reviewed_at=?, reviewed_by_staff_id=? WHERE id=?",
                                (ts, current_staff_id(), rid),
                            )
                        try:
                            notify = []
                            if uploader_email:
                                notify.append(uploader_email)
                            if pid:
                                notify += _staff_emails_for_project(pid)
                            send_push(notify, "WorkNest: Rejected", f"{kind.title()} rejected for {code} ({period_str}).")
                        except Exception:
                            pass
                        st.warning("Rejected.")
                        st.rerun()
                with c3:
                    if st.button("🗑️ Delete", key=f"inbox_{kind}_delete_{rid}"):
                        if kind == "report":
                            execute_sql("DELETE FROM biweekly_reports WHERE id=?", (rid,))
                        elif kind == "test":
                            execute_sql("DELETE FROM test_results WHERE id=?", (rid,))
                        st.info("Deleted.")
                        st.rerun()
                with c4:
                    if file_path and os.path.exists(file_path):
                        try:
                            with open(file_path, "rb") as f:
                                st.download_button(
                                    "⬇️ Download",
                                    f,
                                    file_name=os.path.basename(file_path),
                                    key=f"inbox_{kind}_dl_{rid}",
                                )
                        except Exception:
                            st.caption("Download unavailable.")

    with tab_reports:
        df = fetch_df(
            """
            SELECT r.id, r.project_id, p.code AS project_code, p.name AS project_name,
                   r.report_date, r.uploaded_at, r.file_path, COALESCE(r.status,'PENDING') AS status,
                   s.email AS uploader_email, s.name AS uploader_name
            FROM biweekly_reports r
            LEFT JOIN projects p ON p.id=r.project_id
            LEFT JOIN staff s ON s.id=r.uploader_staff_id
            WHERE COALESCE(r.status,'PENDING')='PENDING'
            ORDER BY COALESCE(r.uploaded_at, r.report_date) DESC
            """
        )
        _render_queue(df, "report")

    with tab_tests:
        df = fetch_df(
            """
            SELECT t.id, t.project_id, p.code AS project_code, p.name AS project_name,
                   NULL AS test_date, t.uploaded_at, t.file_path, COALESCE(t.status,'PENDING') AS status,
                   s.email AS uploader_email, s.name AS uploader_name
            FROM test_results t
            LEFT JOIN projects p ON p.id=t.project_id
            LEFT JOIN staff s ON s.id=t.uploader_staff_id
            WHERE COALESCE(t.status,'PENDING')='PENDING'
            ORDER BY COALESCE(t.uploaded_at) DESC
            """
        )
        _render_queue(df, "test")



def page_import():
    if not is_admin():
        st.markdown("<div class='worknest-header'><h2>⬆️ Import CSVs</h2></div>", unsafe_allow_html=True)
        st.warning("Only **Admin** can import CSV files.")
        return
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
            created_users=0
            updated_users=0
            for _,r in df.iterrows():
                if pd.isna(r.get("name")) or pd.isna(r.get("rank")): 
                    continue
                name = str(r.get("name")).strip()
                email = r.get("email") if pd.notna(r.get("email")) and str(r.get("email")).strip() else None
                email = str(email).strip().lower() if email else None

                ex = fetch_df("SELECT id FROM staff WHERE (email IS NOT NULL AND email=? ) OR LOWER(name)=LOWER(?)", (email, name))
                if ex.empty:
                    execute("""INSERT INTO staff (name,rank,email,phone,section,role,grade,join_date)
                               VALUES (?,?,?,?,?,?,?,?)""",
                            (name, r.get("rank"), email, r.get("phone"), r.get("section"), r.get("role"), r.get("grade"), r.get("join_date")))
                    ex = fetch_df("SELECT id FROM staff WHERE (email IS NOT NULL AND email=? ) OR LOWER(name)=LOWER(?)", (email, name))
                else:
                    execute("""UPDATE staff SET rank=?, email=COALESCE(?,email), phone=?, section=?, role=?, grade=?, join_date=? WHERE id=?""",
                            (r.get("rank"), email, r.get("phone"), r.get("section"), r.get("role"), r.get("grade"), r.get("join_date"), int(ex["id"].iloc[0])))

                if ex.empty:
                    continue
                staff_id=int(ex["id"].iloc[0])

                # --- Ensure there is a login account for every staff ---
                # username priority: email (preferred) else name
                uname = email if email else name
                raw_role = str(r.get("role") or "staff").strip().lower()
                is_admin_flag = 1 if raw_role=="admin" else 0

                # normalize roles to what the app expects
                # (admin, section_head, sub_admin, staff)
                if raw_role in ("admin","section_head","sub_admin","staff"):
                    role_norm = raw_role
                elif raw_role in ("head","section head","section-head","sectionhead","supervisor"):
                    role_norm = "section_head"
                else:
                    role_norm = "staff"

                uex = fetch_df("SELECT id, password_hash FROM users WHERE staff_id=? OR LOWER(username)=LOWER(?)", (staff_id, uname))
                if uex.empty:
                    # Default password is fcda; force change on first login
                    try:
                        execute("""INSERT INTO users (staff_id,username,password_hash,is_admin,role,is_active,must_change_password)
                                   VALUES (?,?,?,?,?,?,?)""",
                                (staff_id, uname, hash_pwd("fcda"), is_admin_flag, role_norm, 1, 1))
                    except Exception:
                        # Backward compatibility if column doesn't exist yet
                        execute("""INSERT INTO users (staff_id,username,password_hash,is_admin,role,is_active)
                                   VALUES (?,?,?,?,?,?)""",
                                (staff_id, uname, hash_pwd("fcda"), is_admin_flag, role_norm, 1))
                    created_users += 1
                else:
                    # Keep existing password unless it's blank; update username/role linkage
                    pw = uex["password_hash"].iloc[0]
                    if pw is None or str(pw).strip()=="":
                        pw = hash_pwd("fcda")
                        try:
                            execute("""UPDATE users SET staff_id=?, username=?, password_hash=?, is_admin=?, role=?, is_active=1, must_change_password=1 WHERE id=?""",
                                    (staff_id, uname, pw, is_admin_flag, role_norm, int(uex["id"].iloc[0])))
                        except Exception:
                            execute("""UPDATE users SET staff_id=?, username=?, password_hash=?, is_admin=?, role=?, is_active=1 WHERE id=?""",
                                    (staff_id, uname, pw, is_admin_flag, role_norm, int(uex["id"].iloc[0])))
                    else:
                        try:
                            execute("""UPDATE users SET staff_id=?, username=?, is_admin=?, role=?, is_active=1 WHERE id=?""",
                                    (staff_id, uname, is_admin_flag, role_norm, int(uex["id"].iloc[0])))
                        except Exception:
                            pass
                    updated_users += 1

            st.success(f"Staff imported/updated. Users created: {created_users}, users updated: {updated_users}.")
        else:
            st.error("data/ staff_template.csv could not be read.")

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
    # Pick user by name/email (no more confusing numeric IDs)
    user_labels = []
    label_to_id = {}
    for _,r in df.iterrows():
        label = f"{r.get('name') or r.get('username')} — {r.get('username')} (role: {r.get('role')})"
        user_labels.append(label)
        label_to_id[label] = int(r["user_id"])

    pick = st.selectbox("Select user", user_labels, key="ac_pick_user")
    user_id = label_to_id[pick]
    cur_row = df[df["user_id"]==user_id].iloc[0].to_dict()

    # Load current toggles
    perms = _get_user_permissions(user_id)

    c1,c2,c3 = st.columns(3)
    with c1:
        new_role = st.selectbox("Role", ["staff","section_head","sub_admin","admin"],
                                index=["staff","section_head","sub_admin","admin"].index(str(cur_row.get("role") or "staff")))
    with c2:
        active = st.selectbox("Status", [1,0], index=0 if int(cur_row.get("is_active") or 1)==1 else 1,
                              format_func=lambda x: "Active" if x==1 else "Disabled")
    with c3:
        new_pwd = st.text_input("Reset password (optional)", type="password",
                                help="Leave blank to keep existing password.")

    st.markdown("#### Capability toggles (Admin-controlled)")
    t1,t2,t3 = st.columns(3)
    with t1:
        can_assign = st.checkbox("Can assign tasks (Section Head)", value=perms["can_assign_tasks"]==1, key="perm_assign")
    with t2:
        can_confirm = st.checkbox("Can confirm completion (Section Head)", value=perms["can_confirm_task_completion"]==1, key="perm_confirm")
    with t3:
        can_upload_docs = st.checkbox("Can upload project documents (Sub-admin)", value=perms["can_upload_project_docs"]==1, key="perm_upload_docs")

    if st.button("Apply changes", key="ac_apply"):
        role_norm = new_role
        isadm = 1 if role_norm=="admin" else 0

        if new_pwd.strip():
            execute("UPDATE users SET role=?, is_admin=?, is_active=?, password_hash=? WHERE id=?",
                    (role_norm, isadm, int(active), hash_pwd(new_pwd.strip()), int(user_id)))
        else:
            execute("UPDATE users SET role=?, is_admin=?, is_active=? WHERE id=?",
                    (role_norm, isadm, int(active), int(user_id)))

        # Upsert permissions
        execute("""INSERT INTO user_permissions (user_id, can_assign_tasks, can_confirm_task_completion, can_upload_project_docs)
                   VALUES (?,?,?,?)
                   ON CONFLICT (user_id) DO UPDATE SET
                     can_assign_tasks=EXCLUDED.can_assign_tasks,
                     can_confirm_task_completion=EXCLUDED.can_confirm_task_completion,
                     can_upload_project_docs=EXCLUDED.can_upload_project_docs
                """, (int(user_id), 1 if can_assign else 0, 1 if can_confirm else 0, 1 if can_upload_docs else 0))

        st.success("Updated.")
        st.rerun()

def page_staff_directory():
    st.title("👥 Staff Directory")
    st.caption("Read‑only directory. For edits, admins use **Staff Admin**.")
    q = st.text_input("Search (name / rank / section / email)", "")
    df = fetch_df("SELECT id, name, rank, section, email, phone, grade, join_date, dob FROM staff ORDER BY name")
    if q.strip():
        ql=q.strip().lower()
        mask = (
            df["name"].fillna("").str.lower().str.contains(ql) |
            df["rank"].fillna("").str.lower().str.contains(ql) |
            df["section"].fillna("").str.lower().str.contains(ql) |
            df["email"].fillna("").str.lower().str.contains(ql)
        )
        df = df[mask]
    st.dataframe(df.drop(columns=["id"]), use_container_width=True)

def page_account():
    st.title("⚙️ Account")
    u = st.session_state.get("user")
    if not u:
        st.info("Please log in.")
        return

    uid = int(u["id"])
    urec = fetch_df("SELECT id, username, role, is_admin, must_change_password FROM users WHERE id=?", (uid,))
    must = 0 if urec.empty else int(urec.iloc[0].get("must_change_password") or 0)

    if must == 1:
        st.warning("You must change your password before continuing.")

    st.subheader("Change password")
    st.caption("After a successful change, we’ll take you back to the dashboard.")

    # Use a form so Streamlit clears inputs on submit and prevents accidental double-clicks.
    with st.form("change_password_form", clear_on_submit=True):
        c1, c2 = st.columns(2)
        old = c1.text_input("Current password", type="password", key="pwd_old")
        new1 = c2.text_input("New password", type="password", key="pwd_new1")
        new2 = c2.text_input("Confirm new password", type="password", key="pwd_new2")
        submitted = st.form_submit_button("Update password", type="primary")

    if submitted:
        if not new1 or len(new1) < 4:
            st.error("Password is too short.")
        elif new1 != new2:
            st.error("Passwords do not match.")
        else:
            # verify old
            row = fetch_df("SELECT password_hash FROM users WHERE id=?", (uid,))
            if row.empty or (hash_pwd(old) != str(row.iloc[0]["password_hash"])):
                st.error("Current password is incorrect.")
            else:
                execute(
                    "UPDATE users SET password_hash=?, must_change_password=0 WHERE id=?",
                    (hash_pwd(new1), uid),
                )
                st.session_state["user"]["must_change_password"] = 0
                # One-time flash on next render
                st.session_state["flash_success"] = "Password updated successfully."
                # Request navigation away (handled before sidebar radio is created)
                st.session_state["_pending_nav"] = "🏠 Dashboard"
                st.rerun()


def _read_help_md(fname:str)->str:
    path=os.path.join(BASE_DIR, "help", fname)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    return "(Help file missing: help/%s)" % fname


def page_help():
    st.title("❓ Help")
    st.caption("Quick guides for staff and admins. If something is unclear, we’ll tighten it up as we go.")

    tabs = st.tabs(["Quick Start", "Tasks", "Leave", "Passwords", "Admin"])
    with tabs[0]:
        st.markdown(_read_help_md("quick_start.md"))
        st.download_button("Download Quick Start (MD)", _read_help_md("quick_start.md"), file_name="WorkNest_Quick_Start.md")
    with tabs[1]:
        st.markdown(_read_help_md("tasks.md"))
        st.download_button("Download Tasks Guide (MD)", _read_help_md("tasks.md"), file_name="WorkNest_Tasks_Guide.md")
    with tabs[2]:
        st.markdown(_read_help_md("leave.md"))
        st.download_button("Download Leave Guide (MD)", _read_help_md("leave.md"), file_name="WorkNest_Leave_Guide.md")
    with tabs[3]:
        st.markdown(_read_help_md("passwords.md"))
        st.download_button("Download Passwords Guide (MD)", _read_help_md("passwords.md"), file_name="WorkNest_Passwords_Guide.md")
    with tabs[4]:
        if is_admin() or is_sub_admin() or is_section_head():
            st.markdown(_read_help_md("admin.md"))
            st.download_button("Download Admin Guide (MD)", _read_help_md("admin.md"), file_name="WorkNest_Admin_Guide.md")
        else:
            st.info("Admin help is restricted.")




# =========================
# ML helpers (v0 demo)
# =========================
def _models_dir()->str:
    d = os.path.join(DATA_DIR, "models")
    try:
        os.makedirs(d, exist_ok=True)
    except Exception:
        pass
    return d

def _ml_enabled()->bool:
    return (Pipeline is not None) and (joblib is not None)

def _ml_fetch_training_df()->pd.DataFrame:
    """
    Build a training dataset from historical task assignments.
    Columns:
      - days_allotted
      - assignee_section, assignee_rank
      - title_len
      - label_overdue (0/1)
      - label_days_taken (float)
    """
    q = """
    SELECT
        A.id AS assignment_id,
        A.staff_id,
        T.id AS task_id,
        T.title,
        T.date_assigned,
        T.days_allotted,
        T.due_date,
        A.status,
        A.completed_date,
        S.section AS staff_section,
        S.rank AS staff_rank
    FROM task_assignments A
    JOIN tasks T ON T.id = A.task_id
    LEFT JOIN staff S ON S.id = A.staff_id
    WHERE A.staff_id IS NOT NULL
    """
    df = fetch_df(q)
    if df.empty:
        return df
    # Feature engineering
    def _safe_len(x):
        try: return len(str(x or "").strip())
        except Exception: return 0

    df["days_allotted"] = pd.to_numeric(df.get("days_allotted"), errors="coerce").fillna(0).astype(int)
    df["title_len"] = df["title"].apply(_safe_len)
    df["staff_section"] = df["staff_section"].fillna("unknown").astype(str)
    df["staff_rank"] = df["staff_rank"].fillna("unknown").astype(str)

    # Labels
    def _parse_dt(x):
        try:
            if x is None or (isinstance(x,float) and pd.isna(x)): return None
            s=str(x).strip()
            if s=="" or s.lower() in ("nan","none","null"): return None
            return dtparser.parse(s)
        except Exception:
            return None

    due = df["due_date"].apply(_parse_dt)
    comp = df["completed_date"].apply(_parse_dt)

    overdue = []
    days_taken = []
    for i in range(len(df)):
        d = due.iloc[i]
        c = comp.iloc[i]
        if c is None or d is None:
            overdue.append(np.nan)
        else:
            overdue.append(1 if c.date() > d.date() else 0)
        # duration label (only when completed)
        if c is None:
            days_taken.append(np.nan)
        else:
            a = _parse_dt(df.iloc[i].get("date_assigned"))
            if a is None:
                days_taken.append(np.nan)
            else:
                days_taken.append(max((c.date()-a.date()).days, 0))
    df["label_overdue"] = overdue
    df["label_days_taken"] = days_taken
    return df

def _ml_train_overdue_model(df:pd.DataFrame):
    d = df.dropna(subset=["label_overdue"]).copy()
    if d.empty:
        return None, {}
    X = d[["days_allotted","title_len","staff_section","staff_rank"]]
    y = d["label_overdue"].astype(int)
    cat = ["staff_section","staff_rank"]
    num = ["days_allotted","title_len"]
    pre = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), cat),
            ("num", "passthrough", num),
        ]
    )
    model = LogisticRegression(max_iter=1000)
    pipe = Pipeline(steps=[("pre", pre), ("model", model)])
    # Train/test split if possible
    metrics={}
    try:
        Xtr,Xte,ytr,yte = train_test_split(X,y,test_size=0.25,random_state=42,stratify=y if y.nunique()>1 else None)
        pipe.fit(Xtr,ytr)
        yp = pipe.predict(Xte)
        metrics["accuracy"] = float(accuracy_score(yte, yp))
        try:
            if hasattr(pipe, "predict_proba") and y.nunique()>1:
                pr = pipe.predict_proba(Xte)[:,1]
                metrics["auc"] = float(roc_auc_score(yte, pr))
        except Exception:
            pass
    except Exception:
        pipe.fit(X,y)
    return pipe, metrics

def _ml_train_duration_model(df:pd.DataFrame):
    d = df.dropna(subset=["label_days_taken"]).copy()
    if d.empty:
        return None, {}
    X = d[["days_allotted","title_len","staff_section","staff_rank"]]
    y = pd.to_numeric(d["label_days_taken"], errors="coerce").fillna(0.0)
    cat = ["staff_section","staff_rank"]
    num = ["days_allotted","title_len"]
    pre = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), cat),
            ("num", "passthrough", num),
        ]
    )
    model = RandomForestRegressor(n_estimators=200, random_state=42)
    pipe = Pipeline(steps=[("pre", pre), ("model", model)])
    metrics={}
    try:
        Xtr,Xte,ytr,yte = train_test_split(X,y,test_size=0.25,random_state=42)
        pipe.fit(Xtr,ytr)
        pred = pipe.predict(Xte)
        metrics["mae"] = float(mean_absolute_error(yte, pred))
    except Exception:
        pipe.fit(X,y)
    return pipe, metrics

def _ml_save_run(model_name:str, pipe, metrics:dict, model_path:str, train_rows:int):
    now = datetime.now().isoformat(timespec="seconds")
    try:
        execute(
            "INSERT INTO ml_runs (model_name, trained_at, train_rows, metrics_json, model_path) VALUES (?,?,?,?,?)",
            (model_name, now, int(train_rows), json.dumps(metrics or {}), model_path),
        )
    except Exception:
        pass

def _ml_log_prediction(model_name:str, task_id:int|None, assignment_id:int|None, staff_id:int|None, p_overdue:float|None, p_days:float|None, features:dict):
    try:
        execute(
            "INSERT INTO ml_predictions (created_at, model_name, task_id, assignment_id, staff_id, predicted_overdue_prob, predicted_days_taken, features_json) VALUES (?,?,?,?,?,?,?,?)",
            (datetime.now().isoformat(timespec="seconds"), model_name, task_id, assignment_id, staff_id, p_overdue, p_days, json.dumps(features or {})),
        )
    except Exception:
        pass

def _ml_load(model_name:str):
    try:
        path = os.path.join(_models_dir(), f"{model_name}.joblib")
        if os.path.exists(path) and joblib is not None:
            return joblib.load(path)
    except Exception:
        pass
    return None

def page_ml():
    st.header("🤖 ML / Insights")
    if not is_admin():
        st.info("Admin only.")
        return
    if not _ml_enabled():
        st.error("ML dependencies not available. Ensure scikit-learn and joblib are installed.")
        return

    df = _ml_fetch_training_df()
    st.caption(f"Training rows available: {len(df)}")
    with st.expander("Preview training data"):
        st.dataframe(df.head(50), use_container_width=True)

    col1,col2 = st.columns(2)
    with col1:
        if st.button("Train Overdue Risk Model"):
            pipe, metrics = _ml_train_overdue_model(df)
            if pipe is None:
                st.warning("Not enough labeled data to train overdue model.")
            else:
                path = os.path.join(_models_dir(), "overdue_risk_v0.joblib")
                joblib.dump(pipe, path)
                _ml_save_run("overdue_risk_v0", pipe, metrics, path, len(df))
                st.success(f"Trained and saved: {path}")
                st.json(metrics)
    with col2:
        if st.button("Train Duration Model"):
            pipe, metrics = _ml_train_duration_model(df)
            if pipe is None:
                st.warning("Not enough completed tasks to train duration model.")
            else:
                path = os.path.join(_models_dir(), "duration_v0.joblib")
                joblib.dump(pipe, path)
                _ml_save_run("duration_v0", pipe, metrics, path, len(df))
                st.success(f"Trained and saved: {path}")
                st.json(metrics)

def main():
    init_db(); apply_styles()
    # Restore login from remember-token cookie (if present)
    try_auto_login_from_cookie()
    if not current_user():
        login_ui(); return

    # Web push (OneSignal): if configured, bind this browser session to the logged-in user email
    try:
        u = current_user()
        if u and u.get("email"):
            render_push_opt_in(u["email"])
    except Exception:
        pass

    # One-time flash messages
    try:
        msg = st.session_state.pop("flash_success", None)
        if msg:
            st.success(msg)
    except Exception:
        pass

    # Run task reminder checks at most once per day per session (emails only if SMTP_* is configured)
    try:
        if st.session_state.get("reminder_ran_on") != str(date.today()):
            if smtp_configured():
                run_task_reminders()
            st.session_state["reminder_ran_on"] = str(date.today())
    except Exception:
        pass

    page = sidebar_nav() or "🏠 Dashboard"
    if page.startswith("🏠"): page_dashboard()
    elif page.startswith("🏗️"): page_projects()
    elif page.startswith("👥"): page_staff()
    elif page.startswith("🧳"): page_leave()
    elif page.startswith("💬"): page_chat()
    elif page.startswith("📇"): page_staff_directory()
    elif page.startswith("⚙️"): page_account()
    elif page.startswith("❓"): page_help()
    elif page.startswith("📄"): page_leave_table()
    elif page.startswith("🗂️"): page_tasks()
    elif page.startswith("⬆️"): page_import()
    elif page.startswith("🔐"): page_access_control()
    elif page.startswith("🤖"): page_ml()
    elif page.startswith("📥"): page_admin_inbox()
    else: page_dashboard()


if __name__=="__main__":
    main()
