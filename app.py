
# ===== FIX ATTACHMENT ID AUTO-INCREMENT (Postgres) =====
def _fix_attachment_id_sequences():
    try:
        execute("CREATE SEQUENCE IF NOT EXISTS biweekly_report_attachments_id_seq")
        execute("ALTER TABLE biweekly_report_attachments ALTER COLUMN id SET DEFAULT nextval('biweekly_report_attachments_id_seq')")
        execute("SELECT setval('biweekly_report_attachments_id_seq', COALESCE((SELECT MAX(id) FROM biweekly_report_attachments),0)+1, false)")

        execute("CREATE SEQUENCE IF NOT EXISTS test_result_attachments_id_seq")
        execute("ALTER TABLE test_result_attachments ALTER COLUMN id SET DEFAULT nextval('test_result_attachments_id_seq')")
        execute("SELECT setval('test_result_attachments_id_seq', COALESCE((SELECT MAX(id) FROM test_result_attachments),0)+1, false)")
    except Exception as e:
        print("SEQ FIX ERROR:", e)

# ===== SMS =====
import os, requests, base64
import traceback
import textwrap

SMS_ENABLED = os.getenv("SMS_ENABLED", "false").strip().lower() == "true"
TERMII_API_KEY = os.getenv("TERMII_API_KEY", "").strip()
TERMII_SENDER_ID = os.getenv("TERMII_SENDER_ID", "Worknest").strip()
TERMII_CHANNEL = os.getenv("TERMII_CHANNEL", "generic").strip()


def _termii_send_one(phone, message):
    url = "https://api.ng.termii.com/api/sms/send"
    payload = {
        "to": phone,
        "from": TERMII_SENDER_ID,
        "sms": message,
        "type": "plain",
        "channel": TERMII_CHANNEL,
        "api_key": TERMII_API_KEY,
    }
    headers = {"Content-Type": "application/json"}

    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=30)
        try:
            body = resp.json()
        except Exception:
            body = {"raw": resp.text[:500]}
        print("TERMII SMS RESPONSE:", resp.status_code, body)

        ok = False
        if resp.status_code == 200:
            code = str(body.get("code", "")).strip().lower()
            message_id = body.get("message_id")
            if code in {"ok", "success"} or bool(message_id):
                ok = True

        return {
            "ok": ok,
            "status_code": resp.status_code,
            "body": body,
            "phone": phone,
        }
    except Exception as e:
        print("TERMII SMS ERROR:", str(e))
        return {"ok": False, "reason": str(e), "phone": phone}


def send_sms(to_numbers, message):
    if not SMS_ENABLED:
        return {"ok": False, "reason": "sms_disabled"}
    if not TERMII_API_KEY:
        return {"ok": False, "reason": "termii_not_configured"}
    if not to_numbers:
        return {"ok": False, "reason": "no_numbers"}

    nums = []
    for n in to_numbers:
        nn = _normalize_ng(n)
        if nn:
            nums.append(nn)
    nums = list(dict.fromkeys(nums))

    if not nums:
        return {"ok": False, "reason": "no_valid_numbers"}

    results = [_termii_send_one(num, message) for num in nums]
    ok_count = sum(1 for r in results if r.get("ok"))
    return {
        "ok": ok_count > 0,
        "provider": "termii",
        "requested": len(to_numbers),
        "valid": len(nums),
        "sent": ok_count,
        "failed": len(nums) - ok_count,
        "results": results,
    }

def _all_staff_phones():
    try:
        df = fetch_df("SELECT phone FROM staff WHERE phone IS NOT NULL")
        return [r["phone"] for _, r in df.iterrows() if r.get("phone")]
    except Exception:
        return []

def navigate_to(page, project_id=None, tab=None):
    st.session_state["_pending_nav"] = page
    if project_id:
        st.session_state["selected_project_id"] = project_id
    if tab:
        st.session_state["selected_project_tab"] = tab

try:
    from streamlit_javascript import st_javascript
except Exception:
    st_javascript = None
import os, hashlib, secrets, html, json, hmac
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
from core.auth import current_staff_id, current_user, is_admin, user_role
from core.dashboard_service import (
    _dashboard_my_reports_df,
    _dashboard_personal_perf_snapshot,
    _dashboard_points_leaderboard_df,
    _dashboard_recent_reports_df,
    _dashboard_recent_tasks_df,
    _dashboard_staff_of_month_df,
    compliance_snapshot,
    current_cycle_compliance_snapshot,
    dashboard_summary_snapshot,
    historical_compliance_snapshot,
    my_current_cycle_compliance_snapshot,
    obligations_snapshot,
    unified_biweekly_snapshot,
)
from core.dates import (
    _is_last_day_of_month,
    _month_end,
    _month_start,
    _monthly_points_window,
    _next_tuesday_after,
    _parse_date,
    _parse_date_safe,
    _parse_iso,
    _to_date_or_none,
    _today,
    _utcnow_iso,
    safe_parse_date,
)
from core.db import (
    DB_IS_POSTGRES,
    DB_PATH,
    DB_URL,
    DATA_DIR,
    UPLOAD_DIR,
    USE_PG,
    _adapt_query,
    _fetch_df_cached,
    execute,
    fetch_df,
    get_conn,
)
from core.labels import (
    _biweekly_timing_status,
    _editable_status,
    _report_cycle_status_label,
    _timing_status_label,
    _timing_status_points,
)
from core.permissions import (
    can_approve_leave,
    can_assign_tasks,
    can_confirm_task_completion,
    can_import_csv,
    can_manage_projects,
    is_reviewer,
    is_section_head,
    is_sub_admin,
)
from core.projects_service import (
    _biweekly_backfill_cutoff_cycle,
    _biweekly_cycle_from_index,
    _biweekly_start_date,
    _current_biweekly_cycle,
    _current_biweekly_cycle_no,
    _project_current_due_cycle,
    _project_first_cycle_index,
    _project_is_dormant,
    _project_meta,
    _project_missing_historical_cycles,
    _project_nonrejected_cycle_map,
    _project_open_biweekly_cycle,
    _project_report_rows,
    can_upload_core_to_project,
    can_upload_project_outputs,
    file_download_button,
    get_setting,
    is_assigned_to_project,
    save_uploaded_file,
    set_setting,
)
from core.tasks_service import (
    _task_points,
    can_download_task_files,
    can_upload_task_files,
    current_staff_section,
    run_task_reminders,
    send_email,
    send_push,
    smtp_configured,
)
from core.utils import (
    RANK_ORDER,
    RANK_TO_INDEX,
    _is_supported_pdf_image,
    _normalize_handle_name,
    _normalize_ng,
    _pdf_error_value,
    _safe_pdf_text,
    has_non_compliance,
    normalize_rank,
    rank_index_safe,
    validate_concrete,
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

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

st.set_page_config(page_title="WorkNest Mini v3.2.4", layout="wide")
# --- Persistent login (Remember me) ---
cookies = CookieManager(prefix="worknest")
TOKEN_SALT = os.environ.get("WORKNEST_TOKEN_SALT") or os.environ.get("SECRET_KEY") or "worknest-mini"

def _hash_token(raw: str) -> str:
    return hashlib.sha256((raw + TOKEN_SALT).encode("utf-8")).hexdigest()

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
    try:
        log_login_event(st.session_state["user"], method="remember_me")
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
BASE_PAGES = ["🏠 Dashboard","🔎 Search","🏗️ Projects","🗂️ Tasks & Performance","🧳 Leave","📄 Leave Table","💬 Chat","⚙️ Account","❓ Help"]
ADMIN_PAGES = ["👥 Staff","📘 Office Diary","⬆️ Import CSVs","🔐 Access Control","🤖 ML / Insights","📥 Admin Inbox"]



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
        <div class="worknest-drawer-btn" id="wn_drawer_btn"></div>
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

APP_TITLE="WorkNest Mini v3.2.4"
CORE_DOC_CATEGORIES=["architectural","structural","electrical","mechanical","soil_investigation","boq","program_of_work"]
STAGES=["Substructure","Ground Floor","Typical Floor","Roof","External Works"]
TEST_TYPES_DISPLAY=[
    ("slump","Concrete Slump Test"),
    ("concube","Concrete Cube Test"),
    ("steel","Steel Test (Batch)"),
    ("reinforcement","Reinforcement Test (Batch)"),
]
@st.cache_resource(show_spinner=False)
def ensure_runtime_initialized():
    init_db()
    _fix_attachment_id_sequences()
    return True

def _exec_script(cur, sql_script: str):
    if not sql_script:
        return
    if not DB_IS_POSTGRES:
        cur.executescript(sql_script)
        return
    stmts=[s.strip() for s in sql_script.split(";") if s.strip()]
    for stmt in stmts:
        try:
            cur.execute(stmt)
        except Exception as e:
            msg = str(e).lower()
            # Be tolerant of duplicate-object races / legacy schema remnants on hosted Postgres.
            if (
                "already exists" in msg
                or "duplicate key value violates unique constraint \"pg_type_typname_nsp_index\"" in msg
                or "duplicate_table" in msg
                or "duplicate_object" in msg
            ):
                try:
                    cur.connection.rollback()
                except Exception:
                    pass
                continue
            raise

PBKDF2_ITERATIONS = 100_000
PASSWORD_SALT_BYTES = 16


def _legacy_hash_pwd(p):
    return hashlib.sha256(("worknest_salt_"+str(p)).encode("utf-8")).hexdigest()


def _generate_password_salt() -> str:
    return os.urandom(PASSWORD_SALT_BYTES).hex()


def hash_pwd(p, salt: str | None = None):
    salt_hex = str(salt or _generate_password_salt()).strip().lower()
    salt_bytes = bytes.fromhex(salt_hex)
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        str(p).encode("utf-8"),
        salt_bytes,
        PBKDF2_ITERATIONS,
    ).hex()
    return digest, salt_hex


def verify_pwd(password, stored_hash, stored_salt=None) -> bool:
    if stored_hash is None:
        return False
    stored_hash = str(stored_hash).strip()
    stored_salt = "" if stored_salt is None else str(stored_salt).strip().lower()
    if stored_salt:
        try:
            calc_hash, _ = hash_pwd(password, stored_salt)
            return hmac.compare_digest(calc_hash, stored_hash)
        except Exception:
            return False
    return hmac.compare_digest(_legacy_hash_pwd(password), stored_hash)


def _generate_temporary_password(length: int = 16) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789@#_-"
    return "".join(secrets.choice(alphabet) for _ in range(max(12, int(length))))

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
  password_salt TEXT,
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

CREATE TABLE IF NOT EXISTS chat_mentions (
  id SERIAL PRIMARY KEY,
  message_id INTEGER NOT NULL REFERENCES chat_messages(id) ON DELETE CASCADE,
  mentioned_staff_id INTEGER NOT NULL REFERENCES staff(id) ON DELETE CASCADE,
  mentioned_name TEXT,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  UNIQUE(message_id, mentioned_staff_id)
);

CREATE TABLE IF NOT EXISTS chat_reads (
  staff_id INTEGER PRIMARY KEY REFERENCES staff(id) ON DELETE CASCADE,
  last_seen_at TIMESTAMP NOT NULL DEFAULT NOW()
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

CREATE TABLE IF NOT EXISTS login_activity (
  id SERIAL PRIMARY KEY,
  user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
  staff_id INTEGER REFERENCES staff(id) ON DELETE SET NULL,
  username TEXT,
  login_at TEXT NOT NULL,
  login_method TEXT,
  session_key TEXT
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
        for col, ddl in [
            ('cycle_no', "ALTER TABLE biweekly_reports ADD COLUMN cycle_no INTEGER"),
            ('window_start', "ALTER TABLE biweekly_reports ADD COLUMN window_start TEXT"),
            ('window_end', "ALTER TABLE biweekly_reports ADD COLUMN window_end TEXT"),
            ('due_date', "ALTER TABLE biweekly_reports ADD COLUMN due_date TEXT"),
            ('timing_status', "ALTER TABLE biweekly_reports ADD COLUMN timing_status TEXT"),
        ]:
            if not _pg_has_column('biweekly_reports', col):
                _pg_add_column(ddl)
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
        if not _pg_has_column('users', 'password_salt'):
            _pg_add_column("ALTER TABLE users ADD COLUMN password_salt TEXT")

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
        if not _pg_has_column('chat_messages', 'created_at'):
            _pg_add_column("ALTER TABLE chat_messages ADD COLUMN created_at TIMESTAMP DEFAULT NOW()")
        cur.execute("""CREATE TABLE IF NOT EXISTS chat_mentions (
          id SERIAL PRIMARY KEY,
          message_id INTEGER NOT NULL REFERENCES chat_messages(id) ON DELETE CASCADE,
          mentioned_staff_id INTEGER NOT NULL REFERENCES staff(id) ON DELETE CASCADE,
          mentioned_name TEXT,
          created_at TIMESTAMP NOT NULL DEFAULT NOW(),
          UNIQUE(message_id, mentioned_staff_id)
        );""")
        if not _pg_has_column('chat_mentions', 'mentioned_name'):
            _pg_add_column("ALTER TABLE chat_mentions ADD COLUMN mentioned_name TEXT")
        if not _pg_has_column('chat_mentions', 'created_at'):
            _pg_add_column("ALTER TABLE chat_mentions ADD COLUMN created_at TIMESTAMP DEFAULT NOW()")
        cur.execute("""CREATE TABLE IF NOT EXISTS chat_reads (
          staff_id INTEGER PRIMARY KEY REFERENCES staff(id) ON DELETE CASCADE,
          last_seen_at TIMESTAMP NOT NULL DEFAULT NOW()
        );""")

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
CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, staff_id INTEGER, username TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL, password_salt TEXT, is_admin INTEGER DEFAULT 0, role TEXT DEFAULT 'staff', is_active INTEGER DEFAULT 1);
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
CREATE TABLE IF NOT EXISTS login_activity (id INTEGER PRIMARY KEY, user_id INTEGER, staff_id INTEGER, username TEXT, login_at TEXT NOT NULL, login_method TEXT, session_key TEXT);
CREATE TABLE IF NOT EXISTS performance_index (id INTEGER PRIMARY KEY, staff_id INTEGER NOT NULL, month TEXT NOT NULL, task_points INTEGER DEFAULT 0, report_points INTEGER DEFAULT 0, test_points INTEGER DEFAULT 0, reliability_score INTEGER DEFAULT 0, attention_to_detail_score INTEGER DEFAULT 0, UNIQUE(staff_id, month));
CREATE TABLE IF NOT EXISTS staff_of_month_posts (id INTEGER PRIMARY KEY, month TEXT NOT NULL UNIQUE, staff_id INTEGER, total_score INTEGER DEFAULT 0, posted_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS notices (id INTEGER PRIMARY KEY, staff_id INTEGER NOT NULL, title TEXT NOT NULL, message TEXT NOT NULL, image_path TEXT, posted_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS notice_comments (id INTEGER PRIMARY KEY, notice_id INTEGER NOT NULL, staff_id INTEGER NOT NULL, comment TEXT NOT NULL, posted_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS chat_messages (id INTEGER PRIMARY KEY, staff_id INTEGER NOT NULL, message TEXT, image_path TEXT, attachment_path TEXT, attachment_name TEXT, attachment_type TEXT, posted_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS chat_mentions (id INTEGER PRIMARY KEY AUTOINCREMENT, message_id INTEGER NOT NULL, mentioned_staff_id INTEGER NOT NULL, mentioned_name TEXT, created_at TEXT NOT NULL, UNIQUE(message_id, mentioned_staff_id));
CREATE TABLE IF NOT EXISTS chat_reads (staff_id INTEGER PRIMARY KEY, last_seen_at TEXT NOT NULL);
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
            for col, ddl in [
                ("cycle_no", "ALTER TABLE biweekly_reports ADD COLUMN cycle_no INTEGER"),
                ("window_start", "ALTER TABLE biweekly_reports ADD COLUMN window_start TEXT"),
                ("window_end", "ALTER TABLE biweekly_reports ADD COLUMN window_end TEXT"),
                ("due_date", "ALTER TABLE biweekly_reports ADD COLUMN due_date TEXT"),
                ("timing_status", "ALTER TABLE biweekly_reports ADD COLUMN timing_status TEXT"),
                ("submitted_on", "ALTER TABLE biweekly_reports ADD COLUMN submitted_on TEXT"),
            ]:
                if col not in cols:
                    try:
                        cur.execute(ddl)
                    except Exception:
                        pass
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
            if "password_salt" not in cols:
                cur.execute("ALTER TABLE users ADD COLUMN password_salt TEXT")
            cols = [r[1] for r in cur.execute("PRAGMA table_info(users)").fetchall()]
            if "must_change_password" not in cols:
                cur.execute("ALTER TABLE users ADD COLUMN must_change_password INTEGER DEFAULT 0")
        except Exception:
            pass


        # projects: next_due_date / dormant status
        try:
            cols = [r[1] for r in cur.execute("PRAGMA table_info(projects)").fetchall()]
            for col, ddl in [
                ("next_due_date", "ALTER TABLE projects ADD COLUMN next_due_date TEXT"),
                ("status", "ALTER TABLE projects ADD COLUMN status TEXT DEFAULT 'ACTIVE'"),
                ("dormant_since", "ALTER TABLE projects ADD COLUMN dormant_since TEXT"),
                ("dormant_reason", "ALTER TABLE projects ADD COLUMN dormant_reason TEXT"),
            ]:
                if col not in cols:
                    cur.execute(ddl)
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
                ("created_at", "ALTER TABLE chat_messages ADD COLUMN created_at TEXT"),
            ]:
                if col not in cols:
                    cur.execute(ddl)
            if "created_at" not in cols:
                try:
                    cur.execute("UPDATE chat_messages SET created_at = COALESCE(posted_at, datetime('now')) WHERE created_at IS NULL")
                except Exception:
                    pass
            cur.execute("CREATE TABLE IF NOT EXISTS chat_mentions (id INTEGER PRIMARY KEY AUTOINCREMENT, message_id INTEGER NOT NULL, mentioned_staff_id INTEGER NOT NULL, mentioned_name TEXT, created_at TEXT NOT NULL, UNIQUE(message_id, mentioned_staff_id))")
            mcols = [r[1] for r in cur.execute("PRAGMA table_info(chat_mentions)").fetchall()]
            for col, ddl in [
                ("mentioned_name", "ALTER TABLE chat_mentions ADD COLUMN mentioned_name TEXT"),
                ("created_at", "ALTER TABLE chat_mentions ADD COLUMN created_at TEXT"),
            ]:
                if col not in mcols:
                    cur.execute(ddl)
            cur.execute("CREATE TABLE IF NOT EXISTS chat_reads (staff_id INTEGER PRIMARY KEY, last_seen_at TEXT NOT NULL)")
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
            execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS password_salt TEXT")
            execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS must_change_password INTEGER DEFAULT 0")
            execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS password_changed_at TEXT")
            # Governance: approvals for uploads (reports/tests)
            execute("ALTER TABLE biweekly_reports ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'PENDING'")
            execute("ALTER TABLE biweekly_reports ADD COLUMN IF NOT EXISTS reviewed_by_staff_id INTEGER")
            execute("ALTER TABLE biweekly_reports ADD COLUMN IF NOT EXISTS reviewed_at TEXT")
            execute("ALTER TABLE biweekly_reports ADD COLUMN IF NOT EXISTS review_note TEXT")
            execute("ALTER TABLE biweekly_reports ADD COLUMN IF NOT EXISTS submitted_on TEXT")
            execute("ALTER TABLE biweekly_reports ADD COLUMN IF NOT EXISTS report_pdf_path TEXT")
            execute("ALTER TABLE projects ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'ACTIVE'")
            execute("ALTER TABLE projects ADD COLUMN IF NOT EXISTS dormant_since TEXT")
            execute("ALTER TABLE projects ADD COLUMN IF NOT EXISTS dormant_reason TEXT")
            execute("UPDATE projects SET status='ACTIVE' WHERE status IS NULL")
            execute("ALTER TABLE test_results ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'PENDING'")
            execute("ALTER TABLE test_results ADD COLUMN IF NOT EXISTS reviewed_by_staff_id INTEGER")
            execute("ALTER TABLE test_results ADD COLUMN IF NOT EXISTS reviewed_at TEXT")
            execute("ALTER TABLE test_results ADD COLUMN IF NOT EXISTS review_note TEXT")
            execute("ALTER TABLE test_results ADD COLUMN IF NOT EXISTS result_summary TEXT")
            execute("ALTER TABLE test_results ADD COLUMN IF NOT EXISTS notes TEXT")
            execute("ALTER TABLE test_results ADD COLUMN IF NOT EXISTS updated_at TEXT")
            # Backfill NULL statuses to APPROVED for legacy rows (so existing history doesn't vanish)
            execute("UPDATE biweekly_reports SET status='APPROVED' WHERE status IS NULL")
            execute("UPDATE test_results SET status='APPROVED' WHERE status IS NULL")

        else:
            # SQLite: IF NOT EXISTS for columns is not supported; ignore errors
            try: execute("ALTER TABLE users ADD COLUMN password_salt TEXT")
            except Exception: pass
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
                "ALTER TABLE biweekly_reports ADD COLUMN submitted_on TEXT",
                "ALTER TABLE biweekly_reports ADD COLUMN report_pdf_path TEXT",
                "ALTER TABLE projects ADD COLUMN status TEXT DEFAULT 'ACTIVE'",
                "ALTER TABLE projects ADD COLUMN dormant_since TEXT",
                "ALTER TABLE projects ADD COLUMN dormant_reason TEXT",
                "ALTER TABLE test_results ADD COLUMN status TEXT DEFAULT 'PENDING'",
                "ALTER TABLE test_results ADD COLUMN reviewed_by_staff_id INTEGER",
                "ALTER TABLE test_results ADD COLUMN reviewed_at TEXT",
                "ALTER TABLE test_results ADD COLUMN review_note TEXT",
                "ALTER TABLE test_results ADD COLUMN result_summary TEXT",
                "ALTER TABLE test_results ADD COLUMN notes TEXT",
                "ALTER TABLE test_results ADD COLUMN updated_at TEXT",
            ]:
                try: execute(q)
                except Exception: pass
            try: execute("UPDATE biweekly_reports SET status='APPROVED' WHERE status IS NULL")
            except Exception: pass
            try: execute("UPDATE projects SET status='ACTIVE' WHERE status IS NULL")
            except Exception: pass
            try: execute("UPDATE test_results SET status='APPROVED' WHERE status IS NULL")
            except Exception: pass

    except Exception:
        pass

    # Extra reporting/workflow tables and columns
    try:
        execute("""CREATE TABLE IF NOT EXISTS biweekly_report_attachments (
            id INTEGER PRIMARY KEY,
            report_id INTEGER NOT NULL,
            file_path TEXT NOT NULL,
            caption TEXT,
            uploaded_at TEXT NOT NULL,
            uploader_staff_id INTEGER
        )""")
        execute("""CREATE TABLE IF NOT EXISTS test_result_attachments (
            id INTEGER PRIMARY KEY,
            test_result_id INTEGER NOT NULL,
            file_path TEXT NOT NULL,
            caption TEXT,
            uploaded_at TEXT NOT NULL,
            uploader_staff_id INTEGER
        )""")
        extra_cols = [
            "ALTER TABLE biweekly_reports ADD COLUMN site_activities TEXT",
            "ALTER TABLE biweekly_reports ADD COLUMN reinforcement_observations TEXT",
            "ALTER TABLE biweekly_reports ADD COLUMN concrete_observations TEXT",
            "ALTER TABLE biweekly_reports ADD COLUMN hse_observations TEXT",
            "ALTER TABLE biweekly_reports ADD COLUMN rfi_notes TEXT",
            "ALTER TABLE biweekly_reports ADD COLUMN general_remarks TEXT",
            "ALTER TABLE biweekly_reports ADD COLUMN updated_at TEXT",
            "ALTER TABLE biweekly_reports ADD COLUMN selected_modules TEXT",
            "ALTER TABLE biweekly_reports ADD COLUMN structured_report_json TEXT",
            "ALTER TABLE test_results ADD COLUMN result_summary TEXT",
            "ALTER TABLE test_results ADD COLUMN notes TEXT",
            "ALTER TABLE test_results ADD COLUMN updated_at TEXT",
        ]
        for q in extra_cols:
            try:
                execute(q)
            except Exception:
                pass
        execute(
            """CREATE TABLE IF NOT EXISTS office_diary (
                id INTEGER PRIMARY KEY,
                entry_date TEXT NOT NULL,
                title TEXT NOT NULL,
                note TEXT NOT NULL,
                project_id INTEGER,
                created_by_staff_id INTEGER,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )"""
        )
    except Exception:
        pass

    # Bootstrap admin (only if users table is empty)
    try:
        ucnt = fetch_df("SELECT COUNT(1) AS n FROM users")
        n = int(ucnt.iloc[0]["n"]) if not ucnt.empty else 0
        if n == 0:
            bootstrap_password = (os.getenv("WORKNEST_BOOTSTRAP_PASSWORD") or "").strip() or _generate_temporary_password()
            bootstrap_hash, bootstrap_salt = hash_pwd(bootstrap_password)
            sid = execute(
                "INSERT INTO staff (name,rank,email,phone,section,role,grade,join_date) VALUES (?,?,?,?,?,?,?,?)",
                ("Admin", "Assistant Director", "", "", "", "admin", "", ""),
            )
            try:
                execute(
                    "INSERT INTO users (staff_id,username,password_hash,password_salt,is_admin,role,is_active,must_change_password) VALUES (?,?,?,?,?,?,?,?)",
                    (sid, "admin", bootstrap_hash, bootstrap_salt, 1, "admin", 1, 1),
                )
            except Exception:
                execute(
                    "INSERT INTO users (staff_id,username,password_hash,password_salt,is_admin,role,is_active) VALUES (?,?,?,?,?,?,?)",
                    (sid, "admin", bootstrap_hash, bootstrap_salt, 1, "admin", 1),
                )
            print(f"WORKNEST BOOTSTRAP ADMIN PASSWORD: {bootstrap_password}")
    except Exception:
        pass
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
    """Return OneSignal config dict.

    - ONESIGNAL_APP_ID is required for browser-side opt-in (Web SDK).
    - ONESIGNAL_REST_API_KEY is optional and only needed for server-side sending.
    """
    app_id = (os.getenv("ONESIGNAL_APP_ID") or "").strip()
    api_key = (os.getenv("ONESIGNAL_REST_API_KEY") or "").strip()
    return {
        "app_id": app_id or None,
        "api_key": api_key or None,
    }


def _onesignal_init_js(app_id: str, external_user_id: str | None = None) -> str:
    """JS snippet to init OneSignal Web SDK v16 and optionally login with external_user_id."""
    ext = (external_user_id or "").replace('"', '\"')
    login_line = f'try {{ OneSignal.login("{ext}"); }} catch(e) {{}}' if ext else ""
    return f"""
<script src=\"https://cdn.onesignal.com/sdks/web/v16/OneSignalSDK.page.js\" async></script>
<script>
  window.OneSignalDeferred = window.OneSignalDeferred || [];
  OneSignalDeferred.push(async function(OneSignal) {{
    try {{
      await OneSignal.init({{
        appId: \"{app_id}\",
        serviceWorkerPath: \"/app/static/OneSignalSDKWorker.js\",
        serviceWorkerUpdaterPath: \"/app/static/OneSignalSDKUpdaterWorker.js\",
        serviceWorkerParam: {{ scope: \"/\" }},
      }});
      {login_line}
    }} catch (e) {{
      // ignore init errors here; status probe will expose them when needed
    }}
  }});
</script>
"""


def render_push_bind(external_user_id: str):
    """Inject OneSignal init and bind the browser to external_user_id (email recommended)."""
    cfg = _onesignal_cfg()
    app_id = cfg.get("app_id")
    if not app_id:
        return
    key = f"onesignal_bound::{external_user_id}"
    if st.session_state.get(key):
        return
    st.session_state[key] = True
    components.html(_onesignal_init_js(app_id, external_user_id), height=0)


def onesignal_get_status(app_id: str, external_user_id: str | None = None):
    """Return dict with permission/subscription status from the browser.

    Requires streamlit-javascript; if unavailable returns None.
    """
    if st_javascript is None:
        return None

    ext = (external_user_id or "").replace('"', '\"')
    login_snip = f'try {{ OneSignal.login("{ext}"); }} catch(e) {{}}' if ext else ""
    js = f"""
async () => {{
  return new Promise((resolve) => {{
    const ensureScript = () => {{
      if (document.querySelector('script[src*="OneSignalSDK.page.js"]')) return;
      const s = document.createElement("script");
      s.src = "https://cdn.onesignal.com/sdks/web/v16/OneSignalSDK.page.js";
      s.async = true;
      document.head.appendChild(s);
    }};
    ensureScript();

    window.OneSignalDeferred = window.OneSignalDeferred || [];
    OneSignalDeferred.push(async function(OneSignal) {{
      try {{
        await OneSignal.init({{
          appId: "{app_id}",
          serviceWorkerPath: "/app/static/OneSignalSDKWorker.js",
          serviceWorkerUpdaterPath: "/app/static/OneSignalSDKUpdaterWorker.js",
          serviceWorkerParam: {{ scope: "/" }},
        }});
        {login_snip}
        const perm = Notification.permission; // granted | denied | default
        let supported = true;
        try {{
          supported = !!(window.Notification && navigator.serviceWorker);
        }} catch(e) {{}}
        let optedIn = false;
        try {{
          optedIn = !!(OneSignal?.User?.PushSubscription?.optedIn);
        }} catch(e) {{}}
        resolve({{ perm, subscribed: optedIn, supported }});
      }} catch (e) {{
        resolve({{ error: String(e) }});
      }}
    }});
  }});
}}
"""
    return st_javascript(js)


def onesignal_prompt_opt_in(app_id: str, external_user_id: str | None = None):
    """Trigger the OneSignal permission prompt (best called from a button click)."""
    ext = (external_user_id or "").replace('"', '\"')
    login_line = f'try {{ OneSignal.login("{ext}"); }} catch(e) {{}}' if ext else ""
    components.html(
        _onesignal_init_js(app_id, external_user_id)
        + f"""
<script>
  window.OneSignalDeferred = window.OneSignalDeferred || [];
  OneSignalDeferred.push(async function(OneSignal) {{
    try {{
      {login_line}
      // Show the prompt
      if (OneSignal?.Slidedown?.promptPush) {{
        OneSignal.Slidedown.promptPush({{ force: true }});
      }} else if (OneSignal?.Notifications?.requestPermission) {{
        OneSignal.Notifications.requestPermission();
      }}
    }} catch(e) {{}}
  }});
</script>
""",
        height=0,
    )


def onesignal_opt_out(app_id: str, external_user_id: str | None = None):
    """Opt-out on this device."""
    ext = (external_user_id or "").replace('"', '\"')
    login_line = f'try {{ OneSignal.login("{ext}"); }} catch(e) {{}}' if ext else ""
    components.html(
        _onesignal_init_js(app_id, external_user_id)
        + f"""
<script>
  window.OneSignalDeferred = window.OneSignalDeferred || [];
  OneSignalDeferred.push(async function(OneSignal) {{
    try {{
      {login_line}
      if (OneSignal?.User?.PushSubscription?.optOut) {{
        await OneSignal.User.PushSubscription.optOut();
      }}
    }} catch(e) {{}}
  }});
</script>
""",
        height=0,
    )


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


def _staff_phone(staff_id: int | None):
    try:
        if not staff_id:
            return None
        df = fetch_df("SELECT phone FROM staff WHERE id=? LIMIT 1", (int(staff_id),))
        if df.empty:
            return None
        phone = str(df.iloc[0].get("phone") or "").strip()
        return phone or None
    except Exception:
        return None


def _admin_phones():
    try:
        df = fetch_df(
            """
            SELECT DISTINCT s.phone
            FROM users u
            JOIN staff s ON s.id=u.staff_id
            WHERE COALESCE(s.phone,'')<>''
              AND (
                    COALESCE(u.is_active,1)=1
                    AND (
                        COALESCE(u.is_admin,0)=1
                        OR LOWER(COALESCE(u.role,'')) IN ('admin','sub_admin')
                    )
                  )
            """
        )
        vals = [str(x).strip() for x in df["phone"].tolist()] if not df.empty else []
        phones = []
        for x in vals:
            nx = _normalize_ng(x)
            if nx:
                phones.append(nx)
        phones = list(dict.fromkeys(phones))
        if phones:
            return phones
        fallback = _normalize_ng("2348112812709")
        if fallback:
            print("BIWEEKLY ADMIN SMS FALLBACK:", {"recipients": [fallback]})
            return [fallback]
        return []
    except Exception:
        fallback = _normalize_ng("2348112812709")
        if fallback:
            print("BIWEEKLY ADMIN SMS FALLBACK:", {"recipients": [fallback], "reason": "admin_phone_lookup_failed"})
            return [fallback]
        return []


def _notify_admins_biweekly_submission(project_code: str, cycle_no, event_label: str):
    admin_numbers = _admin_phones()
    if not admin_numbers:
        print("BIWEEKLY ADMIN SMS SKIP:", {"event": event_label, "reason": "no_admin_numbers"})
        return {"ok": False, "reason": "no_admin_numbers", "event": event_label}
    message = f"WorkNest: Biweekly report {event_label} for {project_code} - Report {int(cycle_no) if cycle_no is not None else 'N/A'}. Please review."
    print("BIWEEKLY ADMIN SMS ATTEMPT:", {"event": event_label, "project_code": project_code, "cycle_no": cycle_no, "recipients": len(admin_numbers)})
    result = _send_sms_notice(admin_numbers, message)
    print("BIWEEKLY ADMIN SMS RESULT:", {"event": event_label, "project_code": project_code, "cycle_no": cycle_no, "result": result})
    return result


def _send_sms_notice(to_numbers, message: str):
    try:
        result = send_sms(to_numbers, message)
        print("WORKNEST SMS NOTICE:", result)
        return result
    except Exception as e:
        print("WORKNEST SMS NOTICE ERROR:", str(e))
        return {"ok": False, "reason": str(e)}

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


def _canonical_approved_biweekly_report_id(project_id: int, cycle_no: int | None, preferred_report_id: int | None = None) -> int | None:
    if cycle_no is None:
        return int(preferred_report_id) if preferred_report_id is not None else None
    approved_df = fetch_df(
        """
        SELECT id
        FROM biweekly_reports
        WHERE project_id=?
          AND cycle_no=?
          AND COALESCE(status,'PENDING')='APPROVED'
        ORDER BY reviewed_at DESC, id DESC
        """,
        (int(project_id), int(cycle_no)),
    )
    if approved_df.empty:
        return None
    approved_ids = [int(x) for x in approved_df["id"].tolist() if pd.notna(x)]
    return approved_ids[0] if approved_ids else None


def _award_biweekly_points(report_id: int) -> None:
    df = fetch_df(
        "SELECT project_id, cycle_no, due_date, report_date, uploaded_at, submitted_on, timing_status FROM biweekly_reports WHERE id=?",
        (int(report_id),),
    )
    if df.empty:
        return
    r = df.iloc[0]
    pid = int(r["project_id"])
    cycle_no = int(r["cycle_no"]) if pd.notna(r.get("cycle_no")) else None
    if cycle_no is not None:
        canonical_report_id = _canonical_approved_biweekly_report_id(pid, cycle_no, preferred_report_id=int(report_id))
        if canonical_report_id != int(report_id):
            return
    due = _parse_date_safe(r.get("due_date") or r.get("report_date"))
    submitted = _parse_date_safe(r.get("submitted_on")) or _parse_date_safe(r.get("uploaded_at")) or due or _today()
    timing = str(r.get("timing_status") or "").upper() or _biweekly_timing_status(due or submitted, submitted)
    pts = _timing_status_points(timing)
    posted = fetch_df("SELECT staff_id FROM project_staff WHERE project_id=?", (pid,))
    awarded_at = dt.datetime.now().isoformat(timespec="seconds")
    for _, pr in posted.iterrows():
        try:
            sid = int(pr["staff_id"])
        except Exception:
            continue
        try:
            execute("DELETE FROM points WHERE staff_id=? AND source='biweekly' AND source_id=?", (sid, int(report_id)))
        except Exception:
            pass
        execute(
            "INSERT OR IGNORE INTO points (staff_id, source, source_id, points, awarded_at) VALUES (?,?,?,?,?)",
            (sid, "biweekly", int(report_id), int(pts), awarded_at),
        )


def _delete_biweekly_points(report_id: int) -> None:
    execute("DELETE FROM points WHERE source='biweekly' AND source_id=?", (int(report_id),))


def _sync_biweekly_points_for_report(report_id: int) -> None:
    df = fetch_df("SELECT id, project_id, cycle_no FROM biweekly_reports WHERE id=?", (int(report_id),))
    if df.empty:
        return
    r = df.iloc[0]
    pid = int(r["project_id"])
    cycle_no = int(r["cycle_no"]) if pd.notna(r.get("cycle_no")) else None
    if cycle_no is None:
        current_status = str(fetch_df("SELECT COALESCE(status,'PENDING') AS status FROM biweekly_reports WHERE id=?", (int(report_id),)).iloc[0]["status"] or "").upper()
        _delete_biweekly_points(int(report_id))
        if current_status == "APPROVED":
            _award_biweekly_points(int(report_id))
        return

    execute(
        """
        DELETE FROM points
        WHERE source='biweekly'
          AND source_id IN (
              SELECT id
              FROM biweekly_reports
              WHERE project_id=?
                AND cycle_no=?
          )
        """,
        (pid, cycle_no),
    )
    canonical_report_id = _canonical_approved_biweekly_report_id(pid, cycle_no, preferred_report_id=int(report_id))
    if canonical_report_id is not None:
        _award_biweekly_points(int(canonical_report_id))


def approve_biweekly_report(report_id: int, approver_staff_id: int | None) -> None:
    ts = dt.datetime.utcnow().isoformat(sep=' ', timespec='seconds')
    execute(
        "UPDATE biweekly_reports SET status='APPROVED', reviewed_at=?, reviewed_by_staff_id=? WHERE id=?",
        (ts, approver_staff_id, int(report_id)),
    )
    _sync_biweekly_points_for_report(int(report_id))

def _report_points(due:dt.date, submitted:dt.date)->int:
    return _timing_status_points(_biweekly_timing_status(due, submitted))

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
       Tasks (completed in the selected month):
          - within allotted days  => 3 pts
          - within 1.5 allotted  => 2 pts
          - beyond 1.5           => 1 pt
       Bi-weekly reports (APPROVED; due date (report_date) in the selected month):
          - uploaded_at <= report_date          => 3 pts
          - uploaded_at <= report_date + 7 days => 2 pts
          - otherwise                           => 1 pt
        Points are shared across all staff posted to that project.
       Test results (APPROVED; submitted_at in the selected month):
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

def apply_styles():
    st.markdown("""<style>
    .worknest-header{background:linear-gradient(90deg,#00B09B,#96C93D);color:#fff;padding:12px 16px;border-radius:14px}
    .pill{display:inline-block;padding:2px 10px;border-radius:999px;background:#eef}
    .wn-chat-card{padding:12px 14px;border:1px solid rgba(0,0,0,.08);border-radius:14px;background:#fff;margin-bottom:10px}
    .wn-chat-card.mentioned{background:#eef6ff;border-color:#8bbcff}
    .wn-chat-meta{font-size:.88rem;color:#5b6470;margin-bottom:4px}
    .wn-mention{color:#1565c0;font-weight:700}
    .wn-chat-badge{display:inline-block;padding:2px 8px;border-radius:999px;background:#eef6ff;color:#1565c0;font-size:.78rem;font-weight:700;margin-left:8px}
    </style>""", unsafe_allow_html=True)

def current_user_id():
    """Backward-compatible alias for older code paths."""
    return current_staff_id()

@st.cache_data(ttl=120, show_spinner=False)
def staff_lookup_options():
    df = fetch_df("SELECT id, name FROM staff WHERE COALESCE(name,'')<>'' ORDER BY name")
    if df.empty:
        return []
    out=[]
    for _, r in df.iterrows():
        try:
            out.append({"id": int(r["id"]), "name": str(r["name"]).strip()})
        except Exception:
            pass
    return out

def chat_name_to_id_map():
    return {_normalize_handle_name(x["name"]): x["id"] for x in staff_lookup_options()}

def compose_chat_message(raw_message:str, mentioned_names:list[str]):
    msg=(raw_message or '').strip()
    prefixes=[]
    lower_msg=msg.lower()
    for nm in mentioned_names or []:
        token=f"@{nm}".strip()
        if token.lower() not in lower_msg:
            prefixes.append(token)
    prefix_text = (" ".join(prefixes)).strip()
    if prefix_text and msg:
        return f"{prefix_text} {msg}".strip()
    return prefix_text or msg

def extract_mentions_from_message(message:str):
    msg=str(message or '')
    norm_msg=_normalize_handle_name(msg.replace('\n',' '))
    mapping=chat_name_to_id_map()
    found=[]
    options_by_id={x['id']: x['name'] for x in staff_lookup_options()}
    for norm_name, sid in mapping.items():
        token='@'+norm_name
        if token in norm_msg:
            found.append((sid, options_by_id.get(sid, norm_name.title())))
    seen=set(); out=[]
    for sid,name in found:
        if sid not in seen:
            seen.add(sid); out.append((sid,name))
    return out

def record_chat_mentions(message_id:int, mentions:list[tuple[int,str]]):
    for sid, name in mentions or []:
        try:
            execute("INSERT OR IGNORE INTO chat_mentions (message_id, mentioned_staff_id, mentioned_name, created_at) VALUES (?,?,?,?)",
                    (int(message_id), int(sid), str(name), _utcnow_iso()))
        except Exception:
            pass

def mark_chat_seen(staff_id:int|None):
    if staff_id is None:
        return
    now=_utcnow_iso()
    try:
        execute("INSERT OR IGNORE INTO chat_reads (staff_id, last_seen_at) VALUES (?,?)", (int(staff_id), now))
        execute("UPDATE chat_reads SET last_seen_at=? WHERE staff_id=?", (now, int(staff_id)))
    except Exception:
        pass

def _get_chat_last_seen(staff_id:int|None):
    if staff_id is None:
        return None
    df=fetch_df("SELECT last_seen_at FROM chat_reads WHERE staff_id=? LIMIT 1", (int(staff_id),))
    if df.empty:
        return None
    return str(df.iloc[0].get('last_seen_at') or '') or None

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

def chat_page_label():
    counts=chat_visibility_counts(current_staff_id())
    unread=max(0,int(counts.get('unread',0) or 0))
    mentions=max(0,int(counts.get('mentions',0) or 0))
    if unread or mentions:
        return f"💬 Chat ({unread} | @{mentions})"
    return "💬 Chat"

def render_chat_message_html(message:str, mention_names:list[str]):
    txt=html.escape(str(message or '')).replace('\n','<br>')
    for name in sorted(set([str(x).strip() for x in (mention_names or []) if str(x).strip()]), key=len, reverse=True):
        token=html.escape('@'+name)
        txt=txt.replace(token, f'<span class="wn-mention">{token}</span>')
    return txt

def chat_mentions_map(message_ids:list[int]):
    if not message_ids:
        return {}
    placeholders=','.join(['?']*len(message_ids))
    out={}
    try:
        df=fetch_df(f"SELECT message_id, mentioned_staff_id, mentioned_name FROM chat_mentions WHERE message_id IN ({placeholders})", tuple(message_ids))
    except Exception:
        try:
            df=fetch_df(f"SELECT message_id, mentioned_staff_id FROM chat_mentions WHERE message_id IN ({placeholders})", tuple(message_ids))
        except Exception:
            return out
    if df.empty:
        return out
    for _, r in df.iterrows():
        mid=int(r['message_id'])
        out.setdefault(mid, []).append({
            'staff_id': int(r['mentioned_staff_id']),
            'name': str(r.get('mentioned_name') or '').strip()
        })
    return out

# ---------- Auth ----------
def login_ui():
    st.markdown(f"<h2 style='text-align:center'>{APP_TITLE}</h2>", unsafe_allow_html=True)
    st.caption("Login with staff <b>email</b> (preferred) or <b>name</b>.", unsafe_allow_html=True)
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

        if (not u.empty) and int(u["is_active"].iloc[0] if "is_active" in u.columns else 1)==1 and verify_pwd(password, u["password_hash"].iloc[0], u["password_salt"].iloc[0] if "password_salt" in u.columns else None):
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
                log_login_event(st.session_state["user"], method="password")
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
            st.error("Wrong username or password.")

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

    base_pages = [chat_page_label() if p.startswith("💬") else p for p in BASE_PAGES]
    admin_pages = ADMIN_PAGES
    pages = base_pages + (admin_pages if is_admin() else [])

    return st.sidebar.radio("Go to", pages, key="nav_radio")

# ---------- Helpers ----------

def is_assigned_to_task(task_id:int, staff_id:int|None=None)->bool:
    sid = staff_id if staff_id is not None else current_staff_id()
    if sid is None: return False
    df=fetch_df("SELECT 1 FROM task_assignments WHERE task_id=? AND staff_id=?", (int(task_id), int(sid)))
    return (not df.empty)



_BIWEEKLY_STATUS_OPTIONS = ["Compliant", "Non-compliant", "Not checked", "Not applicable"]

_BIWEEKLY_MODULES = {
    "concrete": {
        "label": "Concrete Works",
        "checks": [
            ("slump_test", "Slump test carried out"),
            ("cube_samples", "Cube samples taken"),
            ("proper_vibration", "Proper vibration achieved"),
            ("no_segregation", "No segregation observed"),
            ("curing_started", "Proper curing initiated"),
        ],
        "extras": [("batching_method", "Method of batching"), ("mix_ratio", "Mix proportion used"), ("slump_result", "Slump result"), ("cube_count", "Number of cube samples")],
        "selects": {"batching_method": ["Not stated", "Manual", "Machine"]},
    },
    "formwork": {
        "label": "Formwork Inspection",
        "checks": [
            ("alignment", "Proper alignment"),
            ("bracing", "Adequate bracing"),
            ("tightness", "Formwork tightness / leakage prevention"),
            ("waterproofing", "Waterproofing installed where required"),
            ("release_agent", "Release agent applied"),
        ],
        "extras": [],
        "selects": {},
    },
    "reinforcement": {
        "label": "Reinforcement Works",
        "checks": [
            ("bar_sizes", "Bar sizes as per drawings"),
            ("spacing", "Spacing compliance"),
            ("cover", "Concrete cover achieved"),
            ("spacers", "Concrete spacers installed"),
            ("laps", "Laps and anchorage correct"),
        ],
        "extras": [],
        "selects": {},
    },
    "excavation": {
        "label": "Excavation / Earthworks",
        "checks": [
            ("depth", "Required excavation depth achieved"),
            ("soil_condition", "Soil condition consistent with expectation"),
            ("groundwater", "Groundwater observed / managed"),
        ],
        "extras": [],
        "selects": {},
    },
    "backfilling": {
        "label": "Backfilling & Compaction",
        "checks": [
            ("suitable_material", "Suitable backfill material used"),
            ("layers", "Backfilling carried out in layers"),
            ("compaction", "Compaction carried out to required standard"),
        ],
        "extras": [("compaction_method", "Method of compaction")],
        "selects": {"compaction_method": ["Not stated", "Manual", "Mechanical"]},
    },
    "steel_trusses": {
        "label": "Steel Trusses",
        "checks": [
            ("section_sizes", "Steel section sizes as specified"),
            ("material_tests", "Evidence of material testing available"),
            ("spacing", "Spacing of trusses as specified"),
            ("connections", "Connection details as specified"),
        ],
        "extras": [],
        "selects": {},
    },
    "external_works": {
        "label": "External Works (Roads & Drainage)",
        "checks": [
            ("subgrade", "Subgrade preparation adequate"),
            ("drainage_alignment", "Drainage alignment correct"),
            ("compaction", "Compaction adequate"),
        ],
        "extras": [],
        "selects": {},
    },
}

_BIWEEKLY_CHECK_EXTRA_RULES = {
    ("concrete", "slump_test"): {"extra_key": "slump_result", "label": "Slump value", "required_status": "Compliant"},
    ("concrete", "cube_samples"): {"extra_key": "cube_count", "label": "Number of cubes", "required_status": "Compliant"},
}

def _biweekly_check_extra_rule(module_key: str, field_key: str) -> dict | None:
    return _BIWEEKLY_CHECK_EXTRA_RULES.get((module_key, field_key))

def _biweekly_form_refresh():
    return None

def _biweekly_default_module_state(module_key: str) -> dict:
    meta = _BIWEEKLY_MODULES.get(module_key, {})
    data = {"checks": {}, "check_details": {}, "extras": {}, "remarks": ""}
    for field_key, _ in meta.get("checks", []):
        data["checks"][field_key] = "Not checked"
        data["check_details"][field_key] = ""
    for field_key, _ in meta.get("extras", []):
        options = meta.get("selects", {}).get(field_key)
        data["extras"][field_key] = options[0] if options else ""
    return data

def _normalize_biweekly_structured_payload(raw_payload) -> dict:
    payload = {}
    try:
        if raw_payload is None or (not isinstance(raw_payload, dict) and pd.isna(raw_payload)):
            raw_payload = None
    except Exception:
        pass
    if isinstance(raw_payload, dict):
        payload = raw_payload
    elif isinstance(raw_payload, (bytes, bytearray)):
        try:
            payload = json.loads(raw_payload.decode("utf-8", errors="ignore"))
        except Exception:
            payload = {}
    elif raw_payload not in (None, ""):
        try:
            payload = json.loads(str(raw_payload))
        except Exception:
            payload = {}
    selected = payload.get("selected_modules") or []
    if isinstance(selected, str):
        selected = [selected]
    selected = [m for m in selected if m in _BIWEEKLY_MODULES]
    modules = payload.get("modules") if isinstance(payload.get("modules"), dict) else {}
    normalized_modules = {}
    for module_key in _BIWEEKLY_MODULES:
        base = _biweekly_default_module_state(module_key)
        incoming = modules.get(module_key) if isinstance(modules.get(module_key), dict) else {}
        checks_in = incoming.get("checks") if isinstance(incoming.get("checks"), dict) else {}
        details_in = incoming.get("check_details") if isinstance(incoming.get("check_details"), dict) else {}
        extras_in = incoming.get("extras") if isinstance(incoming.get("extras"), dict) else {}
        for field_key in base["checks"]:
            val = checks_in.get(field_key, base["checks"][field_key])
            base["checks"][field_key] = val if val in _BIWEEKLY_STATUS_OPTIONS else "Not checked"
            base["check_details"][field_key] = str(details_in.get(field_key) or "")
        for field_key in base["extras"]:
            base["extras"][field_key] = str(extras_in.get(field_key, base["extras"][field_key]) or "")
        base["remarks"] = str(incoming.get("remarks") or "")
        normalized_modules[module_key] = base
    return {"selected_modules": selected, "modules": normalized_modules}

def _structured_payload_to_json(payload: dict) -> str:
    return json.dumps(_normalize_biweekly_structured_payload(payload), ensure_ascii=False)

def _module_summary_lines(module_key: str, module_state: dict) -> list[str]:
    meta = _BIWEEKLY_MODULES.get(module_key, {})
    lines = []
    rendered_extras = set()
    for field_key, label in meta.get("checks", []):
        val = str((module_state.get("checks") or {}).get(field_key) or "Not checked")
        lines.append(f"{label}: {val}")
        detail = str((module_state.get("check_details") or {}).get(field_key) or "").strip()
        if detail:
            lines.append(f"{label} reason/details: {detail}")
        rule = _biweekly_check_extra_rule(module_key, field_key)
        if rule:
            extra_key = rule["extra_key"]
            extra_val = str((module_state.get("extras") or {}).get(extra_key) or "").strip()
            if extra_val:
                lines.append(f"{rule['label']}: {extra_val}")
            rendered_extras.add(extra_key)
    for field_key, label in meta.get("extras", []):
        if field_key in rendered_extras:
            continue
        val = str((module_state.get("extras") or {}).get(field_key) or "").strip()
        if val:
            lines.append(f"{label}: {val}")
    remarks = str(module_state.get("remarks") or "").strip()
    if remarks:
        lines.append(f"Remarks: {remarks}")
    return lines



def _module_has_meaningful_content(module_key: str, module_state: dict) -> bool:
    meta = _BIWEEKLY_MODULES.get(module_key, {})
    checks = module_state.get("checks") if isinstance(module_state.get("checks"), dict) else {}
    extras = module_state.get("extras") if isinstance(module_state.get("extras"), dict) else {}
    for field_key, _ in meta.get("checks", []):
        val = str(checks.get(field_key) or "").strip()
        if val and val != "Not checked":
            return True
        if str((module_state.get("check_details") or {}).get(field_key) or "").strip():
            return True
    for field_key, _ in meta.get("extras", []):
        if str(extras.get(field_key) or "").strip():
            return True
    return bool(str(module_state.get("remarks") or "").strip())

def _structured_module_markdown(module_key: str, module_state: dict) -> str:
    lines = _module_summary_lines(module_key, module_state)
    if not lines:
        return ""
    return "\n".join([f"- {line}" for line in lines])

def _render_biweekly_structured_details(report_row: dict):
    payload = _normalize_biweekly_structured_payload(report_row.get("structured_report_json"))
    selected = payload.get("selected_modules") or []
    if not selected:
        return False
    st.markdown(f"**Observed Activities**\n\n{', '.join(_BIWEEKLY_MODULES[m]['label'] for m in selected)}")
    for module_key in selected:
        st.markdown(f"**{_BIWEEKLY_MODULES[module_key]['label']}**\n\n{_structured_module_markdown(module_key, payload['modules'][module_key])}")
    return True

def _legacy_sections_from_structured(payload: dict, hse_text: str = "", rfi_text: str = "", general_text: str = "") -> dict:
    payload = _normalize_biweekly_structured_payload(payload)
    selected = payload.get("selected_modules") or []
    module_labels = [_BIWEEKLY_MODULES[m]["label"] for m in selected if m in _BIWEEKLY_MODULES]
    site_activities = ", ".join(module_labels) if module_labels else ""
    module_blocks = {m: _structured_module_markdown(m, payload["modules"].get(m, {})) for m in selected}
    reinforcement_keys = ["reinforcement", "formwork", "excavation", "backfilling", "steel_trusses", "external_works"]
    reinforcement_text = "\n\n".join([module_blocks[k] for k in reinforcement_keys if module_blocks.get(k)])
    concrete_text = module_blocks.get("concrete") or ""
    if not reinforcement_text:
        reinforcement_text = "\n\n".join([module_blocks[k] for k in selected if k != "concrete" and module_blocks.get(k)])
    return {
        "site_activities": site_activities,
        "reinforcement_observations": reinforcement_text,
        "concrete_observations": concrete_text,
        "hse_observations": hse_text,
        "rfi_notes": rfi_text,
        "general_remarks": general_text,
    }


def _biweekly_reports_period_summary(cycle_no: int):
    active_projects = fetch_df("""
        SELECT id, code, name
        FROM projects
        WHERE COALESCE(status,'ACTIVE')!='DORMANT'
        ORDER BY code
    """)
    expected_rows = []
    for _, pr in active_projects.iterrows():
        try:
            pid = int(pr.get("id") or 0)
        except Exception:
            continue
        first_idx = _project_first_cycle_index(pid)
        target = None
        for idx in range(first_idx, first_idx + 500):
            cyc = _biweekly_cycle_from_index(idx)
            if int(cyc.get("cycle_no") or 0) == int(cycle_no):
                target = cyc
                break
        if target is None:
            continue
        expected_rows.append({
            "project_id": pid,
            "project_code": str(pr.get("code") or ""),
            "project_name": str(pr.get("name") or ""),
            "due_date": target.get("due_date").isoformat() if target.get("due_date") else "",
            "window_start": target.get("window_start").isoformat() if target.get("window_start") else "",
            "window_end": target.get("window_end").isoformat() if target.get("window_end") else "",
        })
    expected_df = pd.DataFrame(expected_rows)
    reports_df = fetch_df(
        """
        SELECT r.*, p.code AS project_code, p.name AS project_name,
               s.name AS uploader_name, s.email AS uploader_email
        FROM biweekly_reports r
        LEFT JOIN projects p ON p.id=r.project_id
        LEFT JOIN staff s ON s.id=r.uploader_staff_id
        WHERE COALESCE(r.status,'PENDING') != 'REJECTED'
          AND r.cycle_no=?
        ORDER BY COALESCE(r.updated_at, r.uploaded_at, r.report_date) DESC, r.id DESC
        """,
        (int(cycle_no),)
    )
    latest_rows = []
    if reports_df is not None and not reports_df.empty:
        reports_df = reports_df.sort_values(by=["project_id", "id"], ascending=[True, False])
        latest_rows = list(reports_df.drop_duplicates(subset=["project_id"], keep="first").to_dict("records"))
    reports_by_project = {int(r.get("project_id") or 0): r for r in latest_rows}

    covered_projects = []
    missing_projects = []
    activity_counts = {meta["label"]: 0 for meta in _BIWEEKLY_MODULES.values()}
    non_compliance_rows = []
    status_counter = {}
    timing_counter = {}
    total_non_compliance = 0
    total_hse_flagged = 0
    total_rfi_flagged = 0

    for _, exp in expected_df.iterrows():
        pid = int(exp.get("project_id") or 0)
        row = reports_by_project.get(pid)
        if not row:
            missing_projects.append({
                "Project Code": exp.get("project_code") or "",
                "Project Name": exp.get("project_name") or "",
                "Window": f"{exp.get('window_start') or ''}  {exp.get('window_end') or ''}",
                "Due Date": exp.get("due_date") or "",
            })
            continue
        payload = _normalize_biweekly_structured_payload(row.get("structured_report_json"))
        selected = payload.get("selected_modules") or []
        nc_count = 0
        nc_items = []
        for module_key in selected:
            label = _BIWEEKLY_MODULES[module_key]["label"]
            activity_counts[label] = activity_counts.get(label, 0) + 1
            module_state = payload["modules"].get(module_key, {})
            for field_key, check_label in _BIWEEKLY_MODULES[module_key].get("checks", []):
                val = str((module_state.get("checks") or {}).get(field_key) or "")
                if val == "Non-compliant":
                    nc_count += 1
                    total_non_compliance += 1
                    nc_items.append(f"{label} - {check_label}")
                    non_compliance_rows.append({
                        "Project": f"{row.get('project_code') or ''} - {row.get('project_name') or ''}".strip(' -'),
                        "Module": label,
                        "Checkpoint": check_label,
                        "Status": val,
                        "Report Status": row.get("status") or "",
                    })
        hse_text = str(row.get("hse_observations") or "").strip()
        rfi_text = str(row.get("rfi_notes") or "").strip()
        if hse_text:
            total_hse_flagged += 1
        if rfi_text:
            total_rfi_flagged += 1
        report_status = str(row.get("status") or "PENDING")
        timing_status = _timing_status_label(row.get("timing_status") or "")
        status_counter[report_status] = status_counter.get(report_status, 0) + 1
        timing_counter[timing_status] = timing_counter.get(timing_status, 0) + 1
        covered_projects.append({
            "Project Code": row.get("project_code") or "",
            "Project Name": row.get("project_name") or "",
            "Submitted By": row.get("uploader_name") or row.get("uploader_email") or "",
            "Submitted": row.get("uploaded_at") or row.get("submitted_on") or row.get("report_date") or "",
            "Status": report_status,
            "Timing": timing_status,
            "Activities": ", ".join(_BIWEEKLY_MODULES[m]["label"] for m in selected) if selected else str(row.get("site_activities") or ""),
            "Non-compliance Count": nc_count,
            "HSE Flag": "Yes" if hse_text else "No",
            "RFI/EI Flag": "Yes" if rfi_text else "No",
            "Non-compliance Items": "; ".join(nc_items),
        })

    activity_df = pd.DataFrame([
        {"Activity": k, "Reports Covering Activity": v}
        for k, v in activity_counts.items() if v > 0
    ])
    if not activity_df.empty:
        activity_df = activity_df.sort_values(by=["Reports Covering Activity", "Activity"], ascending=[False, True])

    summary = {
        "cycle_no": int(cycle_no),
        "projects_expected": int(len(expected_df)),
        "reports_submitted": int(len(covered_projects)),
        "missing_reports": int(len(missing_projects)),
        "total_non_compliance": int(total_non_compliance),
        "hse_flags": int(total_hse_flagged),
        "rfi_flags": int(total_rfi_flagged),
        "status_breakdown": status_counter,
        "timing_breakdown": timing_counter,
        "window": "",
    }
    if not expected_df.empty:
        try:
            ws = sorted(set(str(v) for v in expected_df["window_start"].dropna().tolist()))
            we = sorted(set(str(v) for v in expected_df["window_end"].dropna().tolist()))
            dd = sorted(set(str(v) for v in expected_df["due_date"].dropna().tolist()))
            summary["window"] = f"{ws[0] if ws else ''}  {we[-1] if we else ''} | Due: {dd[-1] if dd else ''}"
        except Exception:
            pass
    return {
        "summary": summary,
        "covered_df": pd.DataFrame(covered_projects),
        "missing_df": pd.DataFrame(missing_projects),
        "activity_df": activity_df,
        "non_compliance_df": pd.DataFrame(non_compliance_rows),
    }

from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas
from PIL import Image, ImageOps, UnidentifiedImageError

def _merge_biweekly_pdf_images(report_row, attachments_df=None):
    rows = []
    seen = set()

    def add_row(path: str, caption: str = ""):
        p = str(path or "").strip()
        if not p or not _is_supported_pdf_image(p):
            return
        key = os.path.normcase(os.path.abspath(p))
        if key in seen:
            return
        seen.add(key)
        rows.append({"file_path": p, "caption": caption or ""})

    add_row(report_row.get("file_path"), "Main report image")

    if attachments_df is not None and not getattr(attachments_df, "empty", True):
        try:
            for row in attachments_df.to_dict("records"):
                add_row(row.get("file_path"), str(row.get("caption") or "").strip())
        except Exception:
            pass

    return rows

def generate_biweekly_report_pdf(report_row, attachments_df=None, project_name=""):
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4
    left = 15 * mm
    right = 15 * mm
    top = 15 * mm
    bottom = 15 * mm
    y = height - top
    usable_width = width - left - right
    page_no = 0

    def draw_page_frame(title: str = "Biweekly Report"):
        c.setLineWidth(0.5)
        c.setFont("Times-Bold", 14)
        c.drawString(left, height - 12 * mm, _safe_pdf_text(title)[:120])
        c.setFont("Times-Roman", 9)
        c.drawRightString(width - right, height - 12 * mm, f"Page {page_no}")
        c.line(left, height - 14 * mm, width - right, height - 14 * mm)
        c.line(left, bottom - 2 * mm, width - right, bottom - 2 * mm)
        footer = f"Project: {_safe_pdf_text(project_name)[:85]}"
        c.drawString(left, bottom - 6 * mm, footer)

    def new_page(title: str = "Biweekly Report"):
        nonlocal y, page_no
        if page_no > 0:
            c.showPage()
        page_no += 1
        draw_page_frame(title)
        y = height - 22 * mm

    def ensure_space(required_height_mm=0, title: str = "Biweekly Report"):
        nonlocal y
        if y < bottom + (required_height_mm * mm):
            new_page(title)

    def line(text="", size=10, gap=5, font="Times-Roman"):
        nonlocal y
        ensure_space(gap + 3)
        c.setFont(font, size)
        c.drawString(left, y, _safe_pdf_text(text)[:175])
        y -= gap * mm

    def draw_wrapped_text(title, body):
        nonlocal y
        body = _safe_pdf_text(body)
        ensure_space(14)
        c.setFont("Times-Bold", 11)
        c.drawString(left, y, _safe_pdf_text(title)[:100])
        y -= 5 * mm
        wrapped_lines = []
        for raw_line in body.splitlines() or [""]:
            raw_line = raw_line or ""
            segments = textwrap.wrap(raw_line, width=112, break_long_words=True, break_on_hyphens=False) or [""]
            wrapped_lines.extend(segments)
        if not wrapped_lines:
            wrapped_lines = [""]
        for item in wrapped_lines:
            ensure_space(6)
            c.setFont("Times-Roman", 10)
            c.drawString(left, y, item[:190])
            y -= 4.6 * mm
        y -= 1.5 * mm

    def draw_attachment_images(rows):
        nonlocal y
        if not rows:
            return
        slot_gap = 6 * mm
        slot_caption = 8 * mm
        slot_height = ((height - top - bottom - 18 * mm) / 2.0) - slot_gap
        max_img_height = slot_height - slot_caption
        max_img_width = usable_width
        current_slot = 0
        new_page("Biweekly Report - Attachments")
        c.setFont("Times-Bold", 12)
        c.drawString(left, y, "Attachments / Photos")
        y -= 8 * mm
        for idx, row in enumerate(rows, start=1):
            path = str(row.get("file_path") or "").strip()
            cap = str(row.get("caption") or "").strip()
            if current_slot == 2:
                current_slot = 0
                new_page("Biweekly Report - Attachments")
                c.setFont("Times-Bold", 12)
                c.drawString(left, y, "Attachments / Photos")
                y -= 8 * mm
            slot_top = y
            caption_text = f"Photo {idx}: {cap}" if cap else f"Photo {idx}"
            c.setFont("Times-Roman", 9)
            c.drawString(left, slot_top, _safe_pdf_text(caption_text)[:170])
            img_bottom = slot_top - slot_caption - max_img_height
            if not os.path.exists(path):
                c.setFont("Times-Italic", 9)
                c.drawString(left, slot_top - 14 * mm, f"File unavailable: {os.path.basename(path) if path else 'missing file'}")
            else:
                try:
                    with Image.open(path) as img:
                        img = ImageOps.exif_transpose(img)
                        if img.mode not in ("RGB", "L"):
                            img = img.convert("RGB")
                        img_w, img_h = img.size
                        if not img_w or not img_h:
                            raise ValueError("Invalid image size")
                        scale = min(float(max_img_width) / float(img_w), float(max_img_height) / float(img_h))
                        draw_w = img_w * scale
                        draw_h = img_h * scale
                        img_x = left + ((max_img_width - draw_w) / 2.0)
                        img_y = img_bottom + ((max_img_height - draw_h) / 2.0)
                        img_reader = ImageReader(img)
                        c.rect(left, img_bottom, max_img_width, max_img_height)
                        c.drawImage(img_reader, img_x, img_y, width=draw_w, height=draw_h, preserveAspectRatio=True, mask='auto')
                except Exception:
                    c.setFont("Times-Italic", 9)
                    c.drawString(left, slot_top - 14 * mm, f"Image could not be rendered: {os.path.basename(path)}")
            y -= slot_height + slot_gap
            current_slot += 1

    new_page("Biweekly Report")

    fields = [
        ("Project", project_name),
        ("Report Date", report_row.get("report_date", "")),
        ("Status", report_row.get("status", "")),
        ("Cycle No", report_row.get("cycle_no", "")),
        ("Window Start", report_row.get("window_start", "")),
        ("Window End", report_row.get("window_end", "")),
        ("Due Date", report_row.get("due_date", "")),
        ("Timing", _timing_status_label(report_row.get("timing_status", ""))),
    ]
    for k, v in fields:
        line(f"{k}: {v}", 10, 4.5)

    y -= 2 * mm
    structured_payload = _normalize_biweekly_structured_payload(report_row.get("structured_report_json"))
    selected_modules = structured_payload.get("selected_modules") or []
    sections = []
    if selected_modules:
        sections.append(("Observed Activities", ", ".join(_BIWEEKLY_MODULES[m]["label"] for m in selected_modules)))
        for module_key in selected_modules:
            module_meta = _BIWEEKLY_MODULES.get(module_key, {})
            module_state = (structured_payload.get("modules") or {}).get(module_key) or {}
            module_lines = _module_summary_lines(module_key, module_state)
            sections.append((module_meta.get("label", module_key), "\n".join(module_lines) if module_lines else ""))
    else:
        sections.extend([
            ("Site Activities", report_row.get("site_activities", "")),
            ("Reinforcement Observations", report_row.get("reinforcement_observations", "")),
            ("Concrete / Test Observations", report_row.get("concrete_observations", "")),
        ])
    sections.extend([
        ("HSE Observations", report_row.get("hse_observations", "")),
        ("RFI / EI Notes", report_row.get("rfi_notes", "")),
        ("General Remarks", report_row.get("general_remarks", "")),
    ])

    for title, body in sections:
        draw_wrapped_text(title, body)

    draw_attachment_images(_merge_biweekly_pdf_images(report_row, attachments_df))

    c.save()
    pdf_bytes = buf.getvalue()
    buf.close()
    return pdf_bytes

def _build_biweekly_pdf_file(report_row, project_name="", attachments_df=None):
    try:
        try:
            project_id = int(report_row.get("project_id") or 0)
            report_id = int(report_row.get("id") or 0)
        except Exception:
            project_id = 0
            report_id = 0
        folder = os.path.join(UPLOAD_DIR, f"project_{project_id}/reports") if project_id else os.path.join(UPLOAD_DIR, "reports")
        os.makedirs(folder, exist_ok=True)
        fname = f"biweekly_report_{report_id or datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        out_path = os.path.join(folder, fname)
        pdf_bytes = generate_biweekly_report_pdf(report_row, attachments_df=attachments_df, project_name=project_name)
        with open(out_path, "wb") as f:
            f.write(pdf_bytes)
        return out_path
    except Exception as e:
        err_value = _pdf_error_value(e)
        print("BIWEEKLY PDF BUILD ERROR:", err_value)
        print(traceback.format_exc())
        return err_value

def _ensure_biweekly_pdf(report_row, project_name="", attachments_df=None):
    existing = str(report_row.get("report_pdf_path") or "").strip()
    if existing.startswith(_PDF_ERROR_PREFIX):
        existing = ""
    force_rebuild = attachments_df is not None
    if existing and os.path.exists(existing) and not force_rebuild:
        return existing
    try:
        out_path = _build_biweekly_pdf_file(report_row, project_name=project_name, attachments_df=attachments_df)
        if report_row.get("id") is not None:
            execute("UPDATE biweekly_reports SET report_pdf_path=? WHERE id=?", (out_path, int(report_row.get("id"))))
        return out_path
    except Exception as e:
        err_value = _pdf_error_value(e)
        print("BIWEEKLY PDF BUILD ERROR:", err_value)
        print(traceback.format_exc())
        if report_row.get("id") is not None:
            try:
                execute("UPDATE biweekly_reports SET report_pdf_path=? WHERE id=?", (err_value, int(report_row.get("id"))))
            except Exception:
                pass
        return err_value

def render_pdf_preview_and_download(label_prefix: str, pdf_path: str):
    p = str(pdf_path or "").strip()
    if not p:
        st.caption("PDF not available yet.")
        return
    if p.startswith(_PDF_ERROR_PREFIX):
        st.error(f"PDF generation failed. {p[len(_PDF_ERROR_PREFIX):]}")
        return
    if not os.path.exists(p):
        st.caption("PDF file could not be found.")
        return
    try:
        with open(p, "rb") as f:
            pdf_bytes = f.read()
    except Exception:
        st.caption("PDF file could not be opened.")
        return
    st.download_button("⬇️ Download PDF", data=pdf_bytes, file_name=os.path.basename(p), key=f"{label_prefix}_pdf_download")
    with st.expander("👁️ View PDF", expanded=False):
        b64 = base64.b64encode(pdf_bytes).decode("utf-8")
        st.markdown(
            f'<a href="data:application/pdf;base64,{b64}" target="_blank" rel="noopener noreferrer">Open PDF</a>',
            unsafe_allow_html=True,
        )
        st.caption("Use Open PDF on mobile devices. The direct open link is more reliable than inline preview.")

def _save_uploaded_bytes(uploaded_file, subfolder="", forced_name=None):
    if uploaded_file is None:
        return None
    ext = os.path.splitext(getattr(uploaded_file, 'name', '') or '')[1] or '.bin'
    fname = forced_name or f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}{ext}"
    folder = os.path.join(UPLOAD_DIR, subfolder) if subfolder else UPLOAD_DIR
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, fname)
    data = uploaded_file.getbuffer() if hasattr(uploaded_file, 'getbuffer') else uploaded_file.read()
    with open(path, 'wb') as f:
        f.write(data)
    return path


def _save_biweekly_attachments(report_id: int, uploads=None, camera_file=None, captions_text: str = "", pid: int | None = None):
    uploads = uploads or []
    captions = [ln.strip() for ln in str(captions_text or '').splitlines()]
    saved = []
    for i, up in enumerate([u for u in uploads if u is not None]):
        path = save_uploaded_file(up, f"project_{pid}/reports") if pid is not None else save_uploaded_file(up, "reports")
        if path:
            cap = captions[i] if i < len(captions) else ''
            execute("INSERT INTO biweekly_report_attachments (report_id,file_path,caption,uploaded_at,uploader_staff_id) VALUES (?,?,?,?,?)",
                    (int(report_id), path, cap, datetime.now().isoformat(timespec='seconds'), current_staff_id()))
            saved.append(path)
    if camera_file is not None:
        path = _save_uploaded_bytes(camera_file, f"project_{pid}/reports" if pid is not None else "reports", forced_name=f"camera_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg")
        if path:
            cap = captions[len(saved)] if len(captions) > len(saved) else 'Camera capture'
            execute("INSERT INTO biweekly_report_attachments (report_id,file_path,caption,uploaded_at,uploader_staff_id) VALUES (?,?,?,?,?)",
                    (int(report_id), path, cap, datetime.now().isoformat(timespec='seconds'), current_staff_id()))
            saved.append(path)
    return saved

def _save_test_result_attachment(test_result_id: int, upload=None, caption: str = "", pid: int | None = None):
    if upload is None:
        return None
    path = save_uploaded_file(upload, f"project_{pid}/tests") if pid is not None else save_uploaded_file(upload, "tests")
    if path:
        execute("INSERT INTO test_result_attachments (test_result_id,file_path,caption,uploaded_at,uploader_staff_id) VALUES (?,?,?,?,?)",
                (int(test_result_id), path, caption or '', datetime.now().isoformat(timespec='seconds'), current_staff_id()))
    return path

@st.cache_data(ttl=45, show_spinner=False)
def _attachment_rows(kind: str, parent_id: int):
    table = 'biweekly_report_attachments' if kind == 'biweekly' else 'test_result_attachments'
    idcol = 'report_id' if kind == 'biweekly' else 'test_result_id'
    try:
        return fetch_df(f"SELECT id,file_path,caption,uploaded_at FROM {table} WHERE {idcol}=? ORDER BY id", (int(parent_id),))
    except Exception:
        return pd.DataFrame(columns=['id','file_path','caption','uploaded_at'])


@st.cache_data(ttl=45, show_spinner=False)
def _attachment_rows_bulk(kind: str, parent_ids: tuple[int, ...]):
    if not parent_ids:
        return pd.DataFrame(columns=['id', 'parent_id', 'file_path', 'caption', 'uploaded_at'])
    table = 'biweekly_report_attachments' if kind == 'biweekly' else 'test_result_attachments'
    idcol = 'report_id' if kind == 'biweekly' else 'test_result_id'
    qmarks = ",".join(["?"] * len(parent_ids))
    try:
        return fetch_df(
            f"SELECT id, {idcol} AS parent_id, file_path, caption, uploaded_at FROM {table} WHERE {idcol} IN ({qmarks}) ORDER BY {idcol}, id",
            tuple(int(x) for x in parent_ids),
        )
    except Exception:
        return pd.DataFrame(columns=['id', 'parent_id', 'file_path', 'caption', 'uploaded_at'])

def _render_attachment_list(kind: str, parent_id: int, key_prefix: str):
    adf = _attachment_rows(kind, parent_id)
    if adf.empty:
        return
    with st.expander('Attachments / Photos', expanded=False):
        for _, ar in adf.iterrows():
            c1, c2 = st.columns([4,1])
            with c1:
                nm = os.path.basename(str(ar.get('file_path') or ''))
                cap = str(ar.get('caption') or '').strip()
                stamp = str(ar.get('uploaded_at') or '')
                st.markdown(f"**{nm}**" + (f"  \n{cap}" if cap else '') + (f"  \n*{stamp}*" if stamp else ''))
            with c2:
                file_download_button('⬇️ File', str(ar.get('file_path') or ''), key=f"{key_prefix}_{int(ar['id'])}")

def log_login_event(user: dict | None, method: str = "password") -> None:
    """Best-effort login audit trail."""
    try:
        if not user:
            return
        uid = user.get("id")
        sid = user.get("staff_id")
        username = user.get("username") or user.get("email") or user.get("name") or ""
        if not st.session_state.get("session_key"):
            st.session_state["session_key"] = str(uuid.uuid4())
        execute(
            "INSERT INTO login_activity (user_id, staff_id, username, login_at, login_method, session_key) VALUES (?,?,?,?,?,?)",
            (
                int(uid) if uid is not None and str(uid) != "" else None,
                int(sid) if sid is not None and str(sid) != "" else None,
                str(username),
                datetime.now().isoformat(timespec='seconds'),
                str(method or "password"),
                str(st.session_state.get("session_key") or ""),
            ),
        )
    except Exception:
        pass


@st.cache_data(ttl=45, show_spinner=False)
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


def _refresh_biweekly_reporting_views():
    """Create lightweight views used to normalize submitted biweekly uploads for branch compliance."""
    try:
        execute("""
        CREATE OR REPLACE VIEW vw_submitted_biweekly_reports AS
        SELECT *
        FROM (
            SELECT br.*,
                   ROW_NUMBER() OVER (
                       PARTITION BY br.project_id, br.cycle_no
                       ORDER BY COALESCE(br.updated_at, br.uploaded_at, br.submitted_on, br.report_date) DESC, br.id DESC
                   ) AS rn
            FROM biweekly_reports br
            WHERE COALESCE(br.status,'PENDING') IN ('APPROVED','PENDING','SUBMITTED','NEEDS_REVISION')
              AND br.cycle_no IS NOT NULL
        ) t
        WHERE rn = 1
        """)
    except Exception:
        pass

@st.cache_data(ttl=30, show_spinner=False)
def _project_visible_biweekly_reports_df(pid: int, admin_flag: int, viewer_staff_id: int):
    return fetch_df("""SELECT id,project_id,report_date,uploaded_at,submitted_on,file_path, report_pdf_path, COALESCE(status,'APPROVED') AS status, uploader_staff_id, cycle_no,
                           window_start, window_end, due_date, timing_status, site_activities, reinforcement_observations, concrete_observations,
                           hse_observations, rfi_notes, general_remarks, selected_modules, structured_report_json, reviewed_at, reviewed_by_staff_id, review_note, rejected_reason
                    FROM biweekly_reports
                    WHERE project_id=?
                      AND (
                            ?=1
                            OR (
                                COALESCE(status,'APPROVED') <> 'REJECTED'
                                AND (COALESCE(status,'APPROVED')='APPROVED' OR uploader_staff_id=?)
                            )
                          )
                    ORDER BY date(COALESCE(due_date, report_date)) DESC, id DESC""",(int(pid), int(admin_flag), int(viewer_staff_id)))

def navigate_to_search_result(page, project_id=None, tab=None, tab_index=None):
    st.session_state["_pending_nav"] = page
    if project_id is not None:
        st.session_state["selected_project_id"] = int(project_id)
    if tab is not None:
        st.session_state["selected_project_tab"] = tab
    if tab_index is not None:
        st.session_state["selected_project_tab_index"] = int(tab_index)
    st.rerun()

    if project_id is not None:
        st.session_state["selected_project_id"] = int(project_id)
    if tab:
        st.session_state["selected_project_tab"] = tab
    st.rerun()


from views.dashboard import page_dashboard

@st.cache_data(ttl=20, show_spinner=False)
def worknest_search(query: str, staff_id=None, admin_flag: int = 0):
    q = str(query or '').strip()
    if len(q) < 2:
        return {
            'projects': pd.DataFrame(),
            'documents': pd.DataFrame(),
            'reports': pd.DataFrame(),
            'tests': pd.DataFrame(),
            'chat': pd.DataFrame(),
        }
    like = f"%{q.lower()}%"
    results = {}
    staff_id_val = None
    if staff_id is not None:
        try:
            staff_id_val = int(staff_id)
        except Exception:
            staff_id_val = None
    is_admin_search = bool(admin_flag)
    try:
        if is_admin_search:
            results['projects'] = fetch_df(
                """SELECT id, code, name, client, location, COALESCE(status,'ACTIVE') AS status
                       FROM projects
                       WHERE LOWER(COALESCE(code,'')) LIKE ?
                          OR LOWER(COALESCE(name,'')) LIKE ?
                          OR LOWER(COALESCE(client,'')) LIKE ?
                          OR LOWER(COALESCE(location,'')) LIKE ?
                       ORDER BY code, name
                       LIMIT 25""",
                (like, like, like, like)
            )
        elif staff_id_val is None:
            results['projects'] = pd.DataFrame()
        else:
            results['projects'] = fetch_df(
                """SELECT p.id, p.code, p.name, p.client, p.location, COALESCE(p.status,'ACTIVE') AS status
                       FROM projects p
                       JOIN project_staff ps ON ps.project_id=p.id
                       WHERE ps.staff_id=?
                         AND (
                            LOWER(COALESCE(p.code,'')) LIKE ?
                         OR LOWER(COALESCE(p.name,'')) LIKE ?
                         OR LOWER(COALESCE(p.client,'')) LIKE ?
                         OR LOWER(COALESCE(p.location,'')) LIKE ?
                         )
                       ORDER BY p.code, p.name
                       LIMIT 25""",
                (staff_id_val, like, like, like, like)
            )
    except Exception:
        results['projects'] = pd.DataFrame()
    try:
        if is_admin_search:
            results['documents'] = fetch_df(
                """SELECT d.id, p.code AS project_code, p.name AS project_name, d.category, d.file_path, d.uploaded_at
                       FROM documents d
                       LEFT JOIN projects p ON p.id=d.project_id
                       WHERE LOWER(COALESCE(d.category,'')) LIKE ?
                          OR LOWER(COALESCE(d.file_path,'')) LIKE ?
                          OR LOWER(COALESCE(p.code,'')) LIKE ?
                          OR LOWER(COALESCE(p.name,'')) LIKE ?
                       ORDER BY d.uploaded_at DESC, d.id DESC
                       LIMIT 40""",
                (like, like, like, like)
            )
        elif staff_id_val is None:
            results['documents'] = pd.DataFrame()
        else:
            results['documents'] = fetch_df(
                """SELECT d.id, p.code AS project_code, p.name AS project_name, d.category, d.file_path, d.uploaded_at
                       FROM documents d
                       LEFT JOIN projects p ON p.id=d.project_id
                       WHERE d.project_id IN (
                           SELECT project_id FROM project_staff WHERE staff_id=?
                       )
                         AND (
                            LOWER(COALESCE(d.category,'')) LIKE ?
                         OR LOWER(COALESCE(d.file_path,'')) LIKE ?
                         OR LOWER(COALESCE(p.code,'')) LIKE ?
                         OR LOWER(COALESCE(p.name,'')) LIKE ?
                         )
                       ORDER BY d.uploaded_at DESC, d.id DESC
                       LIMIT 40""",
                (staff_id_val, like, like, like, like)
            )
    except Exception:
        results['documents'] = pd.DataFrame()
    try:
        if is_admin_search:
            results['reports'] = fetch_df(
                """SELECT r.id, p.code AS project_code, p.name AS project_name,
                              COALESCE(r.cycle_no, 0) AS cycle_no,
                              COALESCE(r.status,'APPROVED') AS status,
                              COALESCE(r.submitted_on, r.uploaded_at, r.report_date) AS stamp,
                              LEFT(COALESCE(r.site_activities,'') || ' ' || COALESCE(r.reinforcement_observations,'') || ' ' ||
                                   COALESCE(r.concrete_observations,'') || ' ' || COALESCE(r.hse_observations,'') || ' ' ||
                                   COALESCE(r.rfi_notes,'') || ' ' || COALESCE(r.general_remarks,''), 220) AS snippet
                       FROM biweekly_reports r
                       LEFT JOIN projects p ON p.id=r.project_id
                       WHERE COALESCE(r.status,'PENDING') <> 'REJECTED'
                         AND (
                             LOWER(COALESCE(p.code,'')) LIKE ?
                          OR LOWER(COALESCE(p.name,'')) LIKE ?
                          OR LOWER(COALESCE(r.site_activities,'')) LIKE ?
                          OR LOWER(COALESCE(r.reinforcement_observations,'')) LIKE ?
                          OR LOWER(COALESCE(r.concrete_observations,'')) LIKE ?
                          OR LOWER(COALESCE(r.hse_observations,'')) LIKE ?
                          OR LOWER(COALESCE(r.rfi_notes,'')) LIKE ?
                          OR LOWER(COALESCE(r.general_remarks,'')) LIKE ?
                       )
                       ORDER BY stamp DESC, r.id DESC
                       LIMIT 25""",
                (like, like, like, like, like, like, like, like)
            )
        elif staff_id_val is None:
            results['reports'] = pd.DataFrame()
        else:
            results['reports'] = fetch_df(
                """SELECT r.id, p.code AS project_code, p.name AS project_name,
                              COALESCE(r.cycle_no, 0) AS cycle_no,
                              COALESCE(r.status,'APPROVED') AS status,
                              COALESCE(r.submitted_on, r.uploaded_at, r.report_date) AS stamp,
                              LEFT(COALESCE(r.site_activities,'') || ' ' || COALESCE(r.reinforcement_observations,'') || ' ' ||
                                   COALESCE(r.concrete_observations,'') || ' ' || COALESCE(r.hse_observations,'') || ' ' ||
                                   COALESCE(r.rfi_notes,'') || ' ' || COALESCE(r.general_remarks,''), 220) AS snippet
                       FROM biweekly_reports r
                       LEFT JOIN projects p ON p.id=r.project_id
                       WHERE COALESCE(r.status,'PENDING') <> 'REJECTED'
                         AND r.project_id IN (
                             SELECT project_id FROM project_staff WHERE staff_id=?
                         )
                         AND (
                             LOWER(COALESCE(p.code,'')) LIKE ?
                          OR LOWER(COALESCE(p.name,'')) LIKE ?
                          OR LOWER(COALESCE(r.site_activities,'')) LIKE ?
                          OR LOWER(COALESCE(r.reinforcement_observations,'')) LIKE ?
                          OR LOWER(COALESCE(r.concrete_observations,'')) LIKE ?
                          OR LOWER(COALESCE(r.hse_observations,'')) LIKE ?
                          OR LOWER(COALESCE(r.rfi_notes,'')) LIKE ?
                          OR LOWER(COALESCE(r.general_remarks,'')) LIKE ?
                       )
                       ORDER BY stamp DESC, r.id DESC
                       LIMIT 25""",
                (staff_id_val, like, like, like, like, like, like, like, like)
            )
    except Exception:
        results['reports'] = pd.DataFrame()
    try:
        if is_admin_search:
            results['tests'] = fetch_df(
                """SELECT tr.id, p.code AS project_code, p.name AS project_name, tr.test_type, tr.batch_id,
                              COALESCE(tr.status,'APPROVED') AS status, COALESCE(tr.test_date, tr.uploaded_at) AS stamp,
                              LEFT(COALESCE(tr.result_summary, tr.notes, ''), 220) AS snippet
                       FROM test_results tr
                       LEFT JOIN projects p ON p.id=tr.project_id
                       WHERE LOWER(COALESCE(p.code,'')) LIKE ?
                          OR LOWER(COALESCE(p.name,'')) LIKE ?
                          OR LOWER(COALESCE(tr.test_type,'')) LIKE ?
                          OR LOWER(COALESCE(tr.batch_id,'')) LIKE ?
                          OR LOWER(COALESCE(tr.result_summary, tr.notes, '')) LIKE ?
                       ORDER BY stamp DESC, tr.id DESC
                       LIMIT 25""",
                (like, like, like, like, like)
            )
        elif staff_id_val is None:
            results['tests'] = pd.DataFrame()
        else:
            results['tests'] = fetch_df(
                """SELECT tr.id, p.code AS project_code, p.name AS project_name, tr.test_type, tr.batch_id,
                              COALESCE(tr.status,'APPROVED') AS status, COALESCE(tr.test_date, tr.uploaded_at) AS stamp,
                              LEFT(COALESCE(tr.result_summary, tr.notes, ''), 220) AS snippet
                       FROM test_results tr
                       LEFT JOIN projects p ON p.id=tr.project_id
                       WHERE tr.project_id IN (
                           SELECT project_id FROM project_staff WHERE staff_id=?
                       )
                         AND (
                            LOWER(COALESCE(p.code,'')) LIKE ?
                         OR LOWER(COALESCE(p.name,'')) LIKE ?
                         OR LOWER(COALESCE(tr.test_type,'')) LIKE ?
                         OR LOWER(COALESCE(tr.batch_id,'')) LIKE ?
                         OR LOWER(COALESCE(tr.result_summary, tr.notes, '')) LIKE ?
                         )
                       ORDER BY stamp DESC, tr.id DESC
                       LIMIT 25""",
                (staff_id_val, like, like, like, like, like)
            )
    except Exception:
        results['tests'] = pd.DataFrame()
    try:
        if is_admin_search:
            results['chat'] = fetch_df(
                """SELECT c.id, COALESCE(s.name,'Unknown') AS staff_name, c.created_at, LEFT(COALESCE(c.message,''), 220) AS snippet
                       FROM chat_messages c
                       LEFT JOIN staff s ON s.id=c.staff_id
                       WHERE LOWER(COALESCE(c.message,'')) LIKE ?
                       ORDER BY c.created_at DESC, c.id DESC
                       LIMIT 25""",
                (like,)
            )
        else:
            results['chat'] = pd.DataFrame()
    except Exception:
        results['chat'] = pd.DataFrame()
    return results


def page_chat():
    st.header('💬 General Chat')
    sid=current_staff_id()
    if sid is None:
        st.info('Please login to use chat.')
        return

    counts=chat_visibility_counts(sid)
    if counts.get('unread') or counts.get('mentions'):
        msg=f"Unread messages: {int(counts.get('unread',0) or 0)}"
        if counts.get('mentions'):
            msg += f" | Mentions for you: {int(counts.get('mentions',0) or 0)}"
        st.caption(msg)

    staff_opts = staff_lookup_options()
    mention_names = [x['name'] for x in staff_opts if x['id'] != sid]
    mention_name_to_id = {x['name']: x['id'] for x in staff_opts}

    with st.form('chat_send', clear_on_submit=True):
        selected_mentions = st.multiselect(
            'Tag staff (optional  type to search, full name will be inserted automatically)',
            options=mention_names,
            default=[],
            key='chat_mentions_picker'
        )
        msg=st.text_area('Message', height=90, placeholder='Type your message')
        img=st.file_uploader('Optional image', type=['png','jpg','jpeg','webp','gif'])
        sent=st.form_submit_button('Send')
        if sent:
            chosen = [(int(mention_name_to_id[nm]), nm) for nm in selected_mentions if nm in mention_name_to_id]
            manual = extract_mentions_from_message(msg)
            merged = {sid_: nm for sid_, nm in (chosen + manual)}
            final_mentions=[(int(k), v) for k,v in merged.items() if int(k) != int(sid)]
            m=compose_chat_message(msg, [nm for _, nm in final_mentions])
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
                message_id = execute('INSERT INTO chat_messages (staff_id,message,image_path,attachment_path,attachment_name,attachment_type) VALUES (?,?,?,?,?,?)', (sid, m if m else None, image_path, None, None, None))
                if message_id is not None:
                    record_chat_mentions(int(message_id), final_mentions)
                mark_chat_seen(sid)
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
        mark_chat_seen(sid)
        return

    ids=[]
    for _, rr in df.iterrows():
        try:
            ids.append(int(rr['id']))
        except Exception:
            pass
    mention_map = chat_mentions_map(ids)

    for r in df.to_dict('records'):
        mid = int(r.get('id')) if r.get('id') is not None else None
        msg_mentions = mention_map.get(mid, [])
        mention_names_here = [m.get('name') for m in msg_mentions if m.get('name')]
        mentioned_here = any(int(m.get('staff_id')) == int(sid) for m in msg_mentions if m.get('staff_id') is not None)
        ts=str(r.get('created_at') or '')
        meta = f"<div class='wn-chat-meta'><strong>{html.escape(str(r.get('staff_name','(Unknown)')))}</strong> &nbsp; {html.escape(ts)}"
        if mentioned_here:
            meta += "<span class='wn-chat-badge'>mentioned you</span>"
        meta += "</div>"
        body = render_chat_message_html(r.get('message') or '', mention_names_here) if r.get('message') else ''
        classes = 'wn-chat-card mentioned' if mentioned_here else 'wn-chat-card'
        st.markdown(f"<div class='{classes}'>{meta}<div>{body}</div></div>", unsafe_allow_html=True)
        img_path = r.get('image_path')
        if isinstance(img_path, str) and img_path.strip() and os.path.exists(img_path):
            st.image(img_path)
        st.divider()

    mark_chat_seen(sid)


def page_search():
    st.markdown("<div class='worknest-header'><h2>🔎 Search</h2></div>", unsafe_allow_html=True)
    st.caption("Search across projects, uploaded drawings/documents, biweekly reports, test results, and chat. For full drawing sets like 'Jabi Magistrate Court Structural Drawings', search by project name or file name.")
    q = st.text_input('Search WorkNest', placeholder='Try: Jabi, magistrate, structural drawings, cube test, Ayuba')
    if len(str(q or '').strip()) < 2:
        st.info('Type at least 2 characters to search.')
        return
    res = worknest_search(q, current_staff_id(), 1 if is_admin() else 0)
    total = sum(0 if getattr(df, 'empty', True) else len(df) for df in res.values())
    st.caption(f"Results for: {q}  {total} match(es)")

    if not res['projects'].empty:
        st.markdown('### Projects')
        for _, r in res['projects'].iterrows():
            c1, c2 = st.columns([6,1])
            with c1:
                st.write(f"**{r.get('code','')}  {r.get('name','')}**")
                meta = " | ".join([x for x in [str(r.get('client') or '').strip(), str(r.get('location') or '').strip(), str(r.get('status') or '').strip()] if x])
                if meta:
                    st.caption(meta)
            with c2:
                if st.button('Open', key=f"srch_proj_{int(r['id'])}"):
                    navigate_to_search_result('🏗️ Projects', project_id=int(r['id']))

    if not res['documents'].empty:
        st.markdown('### Drawings / Documents')
        ddf = res['documents'].copy()
        ddf['file_name'] = ddf['file_path'].apply(lambda x: os.path.basename(str(x)))
        show = ddf[['project_code','project_name','category','file_name','uploaded_at']].rename(columns={
            'project_code':'Project Code','project_name':'Project Name','category':'Category','file_name':'File','uploaded_at':'Uploaded'
        })
        for _, r in ddf.iterrows():
            c1, c2, c3 = st.columns([6,1,1])
            with c1:
                st.write(f"**{r.get('project_code','')}  {r.get('project_name','')}** | {r.get('category','')} | {os.path.basename(str(r.get('file_path') or ''))}")
                st.caption(str(r.get('uploaded_at') or ''))
            with c2:
                if st.button('Open', key=f"srch_doc_open_{int(r['id'])}"):
                    pid_df = fetch_df("SELECT project_id FROM documents WHERE id=? LIMIT 1", (int(r['id']),))
                    if not pid_df.empty and pd.notna(pid_df.iloc[0].get('project_id')):
                        navigate_to_search_result('🏗️ Projects', project_id=int(pid_df.iloc[0]['project_id']), tab_index=1)
            with c3:
                file_download_button('⬇️ File', str(r.get('file_path') or ''), key=f"srch_doc_dl_{int(r['id'])}")

                label = f"⬇️ {r.get('project_code') or ''} {os.path.basename(str(r.get('file_path') or ''))}".strip()
                file_download_button(label, str(r.get('file_path') or ''), key=f"search_doc_{int(r['id'])}")

    if not res['reports'].empty:
        st.markdown('### Biweekly Reports')
        rdf = res['reports'].rename(columns={
            'project_code':'Project Code','project_name':'Project Name','cycle_no':'Report No','status':'Status','stamp':'Submitted','snippet':'Excerpt'
        })
        st.dataframe(rdf[['Project Code','Project Name','Report No','Status','Submitted','Excerpt']], hide_index=True, width='stretch')

    if not res['tests'].empty:
        st.markdown('### Test Results')
        tdf = res['tests'].rename(columns={
            'project_code':'Project Code','project_name':'Project Name','test_type':'Test Type','batch_id':'Batch','status':'Status','stamp':'Date','snippet':'Excerpt'
        })
        st.dataframe(tdf[['Project Code','Project Name','Test Type','Batch','Status','Date','Excerpt']], hide_index=True, width='stretch')

    if not res['chat'].empty:
        st.markdown('### Chat')
        cdf = res['chat'].rename(columns={
            'staff_name':'Staff','created_at':'When','snippet':'Message'
        })
        st.dataframe(cdf[['Staff','When','Message']], hide_index=True, width='stretch')

    if all(getattr(df, 'empty', True) for df in res.values()):
        st.warning('No results found.')
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
from views.projects import page_projects

def page_staff():
    st.markdown("<div class='worknest-header'><h2>👥 Staff</h2></div>", unsafe_allow_html=True)
    staff=fetch_df("SELECT id,name,rank,email,section FROM staff ORDER BY name")
    if staff.empty:
        st.info("No staff yet. Import from CSVs or add directly via DB.")
        return
    names=[r["name"] for _,r in staff.iterrows()]
    sel=st.selectbox("Select staff", names, key="staff_pick")
    srow=staff[staff["name"]==sel].iloc[0]
    st.markdown(f"**Name:** {srow['name']}  \n**Rank:** {srow['rank']}  \n**Email:** {srow['email'] or ''}  \n**Section:** {srow['section'] or ''}")
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

        max_days=None; cap_dec31=True; force_days=None; default_days=5
        if ltype=="Annual":
            max_days=30; cap_dec31=True; default_days=30
        elif ltype=="Casual":
            max_days=casual_remaining; cap_dec31=True; default_days=casual_remaining
        elif ltype=="Paternity":
            force_days=14; cap_dec31=True
        elif ltype=="Maternity":
            force_days=112; cap_dec31=False
        elif ltype=="Sick":
            max_days=30; default_days=5
        else:
            max_days=30; default_days=3

        if force_days is not None:
            req=force_days; st.write(f"Working days (fixed): **{force_days}**")
        else:
            if ltype=="Casual" and max_days==0:
                st.warning("You have used all 14 casual leave days this year.")
            req=st.number_input("Requested working days", min_value=0 if ltype=="Casual" else 1,
                                max_value=max_days if (max_days and max_days>0) else 60,
                                value=default_days, key="lv_req",
                                disabled=(ltype=="Casual" and max_days==0))

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
                if (s<=Le and Ls<=e) and (str(L.get("status","")).upper() in ("APPROVED","RECORDED")):
                    return True
        return False

    def is_already_relieving(sid, s, e):
        if all_leaves.empty: return False
        for _,L in all_leaves.iterrows():
            if pd.notna(L["relieving_staff_id"]) and int(L["relieving_staff_id"])==int(sid):
                try:
                    Ls=dtparser.parse(L["start_date"]).date(); Le=dtparser.parse(L["end_date"]).date()
                except: continue
                if (s<=Le and Ls<=e) and (str(L.get("status","")).upper() in ("APPROVED","RECORDED")):
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

    if st.button("📝 Submit Leave Application", key="lv_submit", disabled=(ltype=="Casual" and casual_remaining==0)):
        if can_submit:
            overlap = fetch_df(
                """SELECT 1
                   FROM leaves
                   WHERE staff_id=?
                     AND UPPER(COALESCE(status,'')) IN ('APPROVED','RECORDED')
                     AND date(?) <= date(end_date)
                     AND date(?) >= date(start_date)
                   LIMIT 1""",
                (int(srow["id"]), str(start), str(end)),
            )
            if not overlap.empty:
                st.error("You already have a leave during this period.")
                return
            reliever_id=int([p for p in pool if p[1]==reliever][0][0])
            leave_status = "APPROVED" if ltype=="Annual" else "RECORDED"
            leave_status = str(leave_status or "").strip().upper() or "RECORDED"
            execute("INSERT INTO leaves (staff_id,leave_type,start_date,end_date,working_days,relieving_staff_id,status,reason) VALUES (?,?,?,?,?,?,?,?)",
                    (int(srow["id"]),ltype,str(start),str(end),int(wd),reliever_id,leave_status,reason or None))
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

from views.tasks import page_tasks

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
                st.markdown(f"**{code}**  {pname}")
                st.write(f"**Uploader:** {uploader} · **Status:** {status} · **Due/Test date:** {period_dt} · **Uploaded:** {uploaded_at}")

                if file_path and not os.path.exists(file_path):
                    st.warning(f"Missing file on disk: {file_path}")

                c1, c2, c3, c4 = st.columns([1, 1, 1, 3])
                with c1:
                    if st.button("✅ Approve", key=f"inbox_{kind}_approve_{rid}"):
                        ts = dt.datetime.utcnow().isoformat(sep=' ', timespec='seconds')
                        if kind == "report":
                            approve_biweekly_report(rid, current_staff_id())
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
                        try:
                            uploader_phone = _staff_phone(r.get("uploader_staff_id"))
                            if uploader_phone:
                                _send_sms_notice([uploader_phone], f"WorkNest: Your {kind} for {code} ({period_str}) has been approved.")
                        except Exception:
                            pass

                        # Keep performance table aligned (best-effort)
                        try:
                            if kind == "test" and period_str:
                                ms = dt.datetime.strptime(period_str, "%Y-%m-%d").date().replace(day=1)
                                compute_and_store_monthly_performance(ms)
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
                            _sync_biweekly_points_for_report(int(rid))
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
                        try:
                            uploader_phone = _staff_phone(r.get("uploader_staff_id"))
                            if uploader_phone:
                                _send_sms_notice([uploader_phone], f"WorkNest: Your {kind} for {code} ({period_str}) was rejected. Please check WorkNest.")
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
        cyc_df = fetch_df("SELECT DISTINCT cycle_no FROM biweekly_reports WHERE cycle_no IS NOT NULL ORDER BY cycle_no DESC")
        cycle_options = [int(v) for v in cyc_df["cycle_no"].dropna().tolist()] if cyc_df is not None and not cyc_df.empty else []
        try:
            current_cutoff = _report_cutoff_cycle_no(dt.date.today())
            for extra in range(max(1, current_cutoff - 4), current_cutoff + 1):
                if extra not in cycle_options:
                    cycle_options.append(extra)
            cycle_options = sorted(set(cycle_options), reverse=True)
        except Exception:
            cycle_options = sorted(set(cycle_options), reverse=True)
        if cycle_options:
            with st.expander("Period summary", expanded=True):
                chosen_cycle = st.selectbox("Select biweekly report number", options=cycle_options, key="admin_biweekly_period_summary_cycle")
                period_pack = _biweekly_reports_period_summary(int(chosen_cycle))
                summary = period_pack["summary"]
                if summary.get("window"):
                    st.caption(summary["window"])
                s1, s2, s3, s4 = st.columns(4)
                s1.metric("Projects Covered", summary.get("reports_submitted", 0))
                s2.metric("Expected Projects", summary.get("projects_expected", 0))
                s3.metric("Missing Reports", summary.get("missing_reports", 0))
                s4.metric("Non-compliance Items", summary.get("total_non_compliance", 0))
                s5, s6 = st.columns(2)
                with s5:
                    st.markdown("**Activity breakdown**")
                    if period_pack["activity_df"] is not None and not period_pack["activity_df"].empty:
                        st.dataframe(period_pack["activity_df"], use_container_width=True, hide_index=True)
                    else:
                        st.info("No structured activity data yet for this period.")
                with s6:
                    status_bits = []
                    for k, v in (summary.get("status_breakdown") or {}).items():
                        status_bits.append(f"{k}: {v}")
                    for k, v in (summary.get("timing_breakdown") or {}).items():
                        status_bits.append(f"{k}: {v}")
                    st.markdown("**Submission profile**")
                    st.write(" | ".join(status_bits) if status_bits else "No submissions yet.")
                    st.write(f"HSE flags: {summary.get('hse_flags', 0)}")
                    st.write(f"RFI/EI flags: {summary.get('rfi_flags', 0)}")
                if period_pack["covered_df"] is not None and not period_pack["covered_df"].empty:
                    with st.expander("Submitted reports in this period", expanded=False):
                        st.dataframe(period_pack["covered_df"], use_container_width=True, hide_index=True)
                if period_pack["missing_df"] is not None and not period_pack["missing_df"].empty:
                    with st.expander("Missing reports", expanded=False):
                        st.dataframe(period_pack["missing_df"], use_container_width=True, hide_index=True)
                if period_pack["non_compliance_df"] is not None and not period_pack["non_compliance_df"].empty:
                    with st.expander("Non-compliance register", expanded=False):
                        st.dataframe(period_pack["non_compliance_df"], use_container_width=True, hide_index=True)

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
            temp_credentials = []
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

                uex = fetch_df("SELECT id, password_hash, password_salt FROM users WHERE staff_id=? OR LOWER(username)=LOWER(?)", (staff_id, uname))
                if uex.empty:
                    temp_password = _generate_temporary_password()
                    pw_hash, pw_salt = hash_pwd(temp_password)
                    try:
                        execute("""INSERT INTO users (staff_id,username,password_hash,password_salt,is_admin,role,is_active,must_change_password)
                                   VALUES (?,?,?,?,?,?,?,?)""",
                                (staff_id, uname, pw_hash, pw_salt, is_admin_flag, role_norm, 1, 1))
                    except Exception:
                        # Backward compatibility if column doesn't exist yet
                        execute("""INSERT INTO users (staff_id,username,password_hash,password_salt,is_admin,role,is_active)
                                   VALUES (?,?,?,?,?,?,?)""",
                                (staff_id, uname, pw_hash, pw_salt, is_admin_flag, role_norm, 1))
                    temp_credentials.append({"Username": uname, "Temporary Password": temp_password})
                    created_users += 1
                else:
                    # Keep existing password unless it's blank; update username/role linkage
                    pw = uex["password_hash"].iloc[0]
                    pw_salt = uex["password_salt"].iloc[0] if "password_salt" in uex.columns else None
                    if pw is None or str(pw).strip()=="":
                        temp_password = _generate_temporary_password()
                        pw, pw_salt = hash_pwd(temp_password)
                        try:
                            execute("""UPDATE users SET staff_id=?, username=?, password_hash=?, password_salt=?, is_admin=?, role=?, is_active=1, must_change_password=1 WHERE id=?""",
                                    (staff_id, uname, pw, pw_salt, is_admin_flag, role_norm, int(uex["id"].iloc[0])))
                        except Exception:
                            execute("""UPDATE users SET staff_id=?, username=?, password_hash=?, password_salt=?, is_admin=?, role=?, is_active=1 WHERE id=?""",
                                    (staff_id, uname, pw, pw_salt, is_admin_flag, role_norm, int(uex["id"].iloc[0])))
                        temp_credentials.append({"Username": uname, "Temporary Password": temp_password})
                    else:
                        try:
                            execute("""UPDATE users SET staff_id=?, username=?, is_admin=?, role=?, is_active=1 WHERE id=?""",
                                    (staff_id, uname, is_admin_flag, role_norm, int(uex["id"].iloc[0])))
                        except Exception:
                            pass
                    updated_users += 1

            st.success(f"Staff imported/updated. Users created: {created_users}, users updated: {updated_users}.")
            if temp_credentials:
                st.warning("Temporary passwords were generated for new or repaired accounts. Share them securely and require users to change them at first login.")
                st.dataframe(pd.DataFrame(temp_credentials), hide_index=True, width='stretch')
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
    st.caption("Admin can define what staff see by assigning roles and enabling/disabling accounts. New accounts receive temporary credentials and must change them at first login.")
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
        label = f"{r.get('name') or r.get('username')}  {r.get('username')} (role: {r.get('role')})"
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
            new_hash, new_salt = hash_pwd(new_pwd.strip())
            execute("UPDATE users SET role=?, is_admin=?, is_active=?, password_hash=?, password_salt=? WHERE id=?",
                    (role_norm, isadm, int(active), new_hash, new_salt, int(user_id)))
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
    st.title("📇 Staff Directory")
    st.caption("Read-only directory. For edits, admins use **Staff Admin**.")
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
    st.dataframe(df.drop(columns=["id"]), width='stretch')

def page_account():
    st.title("⚙️ Account")
    u = st.session_state.get("user")
    if not u:
        st.info("Please log in.")
        return

    uid = int(u["id"])
    urec = fetch_df("SELECT id, username, role, is_admin, must_change_password FROM users WHERE id=?", (uid,))
    must = 0 if urec.empty else int(urec.iloc[0].get("must_change_password") or 0)

    # Prefer email as external id for OneSignal targeting
    external_id = (u.get("email") or u.get("username") or str(uid)).strip()

    # If user is forced to change password, keep this page focused
    if must == 1:
        st.warning("You must change your password before continuing.")
        st.subheader("Change password")
        st.caption("After a successful change, we'll take you back to the dashboard.")

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
                row = fetch_df("SELECT password_hash, password_salt FROM users WHERE id=?", (uid,))
                if row.empty or (not verify_pwd(old, row.iloc[0]["password_hash"], row.iloc[0]["password_salt"] if "password_salt" in row.columns else None)):
                    st.error("Current password is incorrect.")
                else:
                    new_hash, new_salt = hash_pwd(new1)
                    execute_sql(
                        "UPDATE users SET password_hash=?, password_salt=?, must_change_password=0 WHERE id=?",
                        (new_hash, new_salt, uid),
                    )
                    st.session_state["user"]["must_change_password"] = 0
                    st.session_state["flash_success"] = "Password updated successfully."
                    st.session_state["_pending_nav"] = "🏠 Dashboard"
                    st.rerun()
        return

    # ------------------------
    # Push notifications UI
    # ------------------------
    cfg = _onesignal_cfg()
    app_id = cfg.get("app_id")

    st.markdown("### 🔔 Push Notifications")

    if not app_id:
        st.info("Push notifications are not configured on this server yet.")
    elif st_javascript is None:
        st.warning("Push notifications status checker is missing (streamlit-javascript). Please redeploy with updated requirements.")
    else:
        # Make sure OneSignal knows who this user is (External User ID)
        render_push_bind(external_id)

        status = onesignal_get_status(app_id, external_id) or {}
        if status.get("error"):
            st.warning("Push status not available yet. Refresh this page once.")
        else:
            perm = status.get("perm")
            subscribed = bool(status.get("subscribed"))
            supported = status.get("supported", True)

            if not supported:
                st.error("This browser does not support push notifications.")
            elif perm == "denied":
                st.error("Push notifications are blocked for this site in your browser settings.")
                st.caption("In Chrome: click the padlock icon → Site settings → Notifications → Allow.")
            elif subscribed:
                st.success("Push notifications are enabled on this device ✅")
                c1, c2 = st.columns([1, 2])
                if c1.button("Disable on this device", key="push_disable_btn"):
                    onesignal_opt_out(app_id, external_id)
                    st.toast("Disabling…", icon="🔕")
                    st.rerun()
                c2.caption("You'll keep receiving WorkNest notifications on this browser until you disable it.")
            else:
                if perm == "granted":
                    st.info("Permission is granted, but you are not subscribed yet.")
                else:
                    st.info("Push notifications are not enabled on this device.")

                if st.button("Enable push notifications on this device", key="push_enable_btn"):
                    onesignal_prompt_opt_in(app_id, external_id)
                    st.toast("Check your browser prompt…", icon="🔔")

    st.divider()

    # ------------------------
    # Password change (optional)
    # ------------------------
    st.subheader("Change password")
    st.caption("After a successful change, we'll take you back to the dashboard.")

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
            row = fetch_df("SELECT password_hash, password_salt FROM users WHERE id=?", (uid,))
            if row.empty or (not verify_pwd(old, row.iloc[0]["password_hash"], row.iloc[0]["password_salt"] if "password_salt" in row.columns else None)):
                st.error("Current password is incorrect.")
            else:
                new_hash, new_salt = hash_pwd(new1)
                execute_sql(
                    "UPDATE users SET password_hash=?, password_salt=? WHERE id=?",
                    (new_hash, new_salt, uid),
                )
                st.session_state["flash_success"] = "Password updated successfully."
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
    st.caption("Quick guides for staff and admins. If something is unclear, we'll tighten it up as we go.")

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




def page_office_diary():
    st.markdown("<div class='worknest-header'><h2>📘 Office Diary</h2></div>", unsafe_allow_html=True)
    if not is_admin():
        st.info("Admin only.")
        return

    sid = current_staff_id()
    today_value = date.today()
    today_iso = today_value.isoformat()
    projects_df = fetch_df("SELECT id, code, name FROM projects ORDER BY code, name")
    project_options = ["— No project linked —"]
    project_map = {"— No project linked —": None}
    if not projects_df.empty:
        for _, row in projects_df.iterrows():
            label = " — ".join([x for x in [str(row.get("code") or "").strip(), str(row.get("name") or "").strip()] if x])
            project_options.append(label)
            project_map[label] = int(row["id"])

    edit_id = st.session_state.get("office_diary_edit_id")
    edit_row = None
    if edit_id is not None:
        edit_df = fetch_df("SELECT * FROM office_diary WHERE id=?", (int(edit_id),))
        if not edit_df.empty:
            edit_row = edit_df.iloc[0]
        else:
            st.session_state.pop("office_diary_edit_id", None)

    st.markdown("### Entry Form")
    with st.form("office_diary_form", clear_on_submit=edit_row is None):
        entry_title = st.text_input("Title", value=str(edit_row.get("title") or "") if edit_row is not None else "")
        entry_note = st.text_area("Note", value=str(edit_row.get("note") or "") if edit_row is not None else "", height=160)
        selected_project_label = "— No project linked —"
        if edit_row is not None and pd.notna(edit_row.get("project_id")) and not projects_df.empty:
            match = projects_df[projects_df["id"] == int(edit_row["project_id"])]
            if not match.empty:
                selected_project_label = " — ".join([x for x in [str(match.iloc[0].get("code") or "").strip(), str(match.iloc[0].get("name") or "").strip()] if x]) or "— No project linked —"
        project_label = st.selectbox(
            "Project (optional)",
            project_options,
            index=project_options.index(selected_project_label) if selected_project_label in project_options else 0,
        )
        c1, c2 = st.columns(2)
        save_label = "Update Entry" if edit_row is not None else "Save Entry"
        save_entry = c1.form_submit_button(save_label)
        cancel_edit = c2.form_submit_button("Cancel Edit") if edit_row is not None else False

    if cancel_edit:
        st.session_state.pop("office_diary_edit_id", None)
        st.rerun()

    if save_entry:
        if not str(entry_title or "").strip():
            st.error("Title is required.")
        elif not str(entry_note or "").strip():
            st.error("Note is required.")
        else:
            now_iso = datetime.now().isoformat(timespec="seconds")
            project_id = project_map.get(project_label)
            if edit_row is not None:
                execute(
                    "UPDATE office_diary SET title=?, note=?, project_id=?, updated_at=? WHERE id=?",
                    (str(entry_title).strip(), str(entry_note).strip(), project_id, now_iso, int(edit_row["id"])),
                )
                st.session_state.pop("office_diary_edit_id", None)
                st.success("Office diary entry updated.")
            else:
                execute(
                    "INSERT INTO office_diary (entry_date, title, note, project_id, created_by_staff_id, created_at, updated_at) VALUES (?,?,?,?,?,?,?)",
                    (today_iso, str(entry_title).strip(), str(entry_note).strip(), project_id, sid, now_iso, now_iso),
                )
                st.success("Office diary entry saved.")
            st.rerun()

    st.markdown("### Entries")
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("From", value=today_value - timedelta(days=7), key="office_diary_from")
    with col2:
        end_date = st.date_input("To", value=today_value, key="office_diary_to")
    if start_date > end_date:
        st.error("The 'From' date cannot be after the 'To' date.")
        return
    search_term = st.text_input("Search", value="", placeholder="Search title or note", key="office_diary_search")
    diary_sql = """
        SELECT od.id, od.entry_date, od.title, od.note, od.project_id, od.created_at, od.updated_at,
               p.code AS project_code, p.name AS project_name,
               s.name AS created_by_name
        FROM office_diary od
        LEFT JOIN projects p ON p.id=od.project_id
        LEFT JOIN staff s ON s.id=od.created_by_staff_id
    """
    diary_sql += """
        WHERE date(od.entry_date) BETWEEN ? AND ?
    """
    diary_params = (start_date.isoformat(), end_date.isoformat())
    if str(search_term or "").strip():
        diary_sql += """
        AND (
            LOWER(COALESCE(od.title,'')) LIKE ?
            OR LOWER(COALESCE(od.note,'')) LIKE ?
        )
        """
        like_term = f"%{str(search_term).strip().lower()}%"
        diary_params = diary_params + (like_term, like_term)
    diary_sql += " ORDER BY date(od.entry_date) DESC, COALESCE(od.updated_at, od.created_at) DESC, od.id DESC"
    entries_df = fetch_df(diary_sql, diary_params)

    if entries_df.empty:
        st.info("No diary entries for selected date range.")
        return

    for _, row in entries_df.iterrows():
        project_label = " - ".join([x for x in [str(row.get("project_code") or "").strip(), str(row.get("project_name") or "").strip()] if x])
        st.markdown(f"**{row.get('title') or 'Untitled'}**")
        meta_bits = [str(row.get("entry_date") or "")]
        if project_label:
            meta_bits.append(project_label)
        if str(row.get("created_by_name") or "").strip():
            meta_bits.append(f"By {row.get('created_by_name')}")
        st.caption(" | ".join([x for x in meta_bits if x]))
        st.write(str(row.get("note") or ""))
        a1, a2 = st.columns([1, 1])
        if a1.button("Edit", key=f"office_diary_edit_{int(row['id'])}"):
            st.session_state["office_diary_edit_id"] = int(row["id"])
            st.rerun()
        if a2.button("Delete", key=f"office_diary_delete_{int(row['id'])}"):
            execute("DELETE FROM office_diary WHERE id=?", (int(row["id"]),))
            if st.session_state.get("office_diary_edit_id") == int(row["id"]):
                st.session_state.pop("office_diary_edit_id", None)
            st.success("Office diary entry deleted.")
            st.rerun()
        st.markdown("---")


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
        st.dataframe(df.head(50), width='stretch')

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
    ensure_runtime_initialized(); apply_styles()
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
    elif page.startswith("🔎"): page_search()
    elif page.startswith("🏗️"): page_projects()
    elif page.startswith("👥"): page_staff()
    elif page.startswith("📘"): page_office_diary()
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


