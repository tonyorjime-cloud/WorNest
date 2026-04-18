import os
import sqlite3

import pandas as pd
import streamlit as st

try:
    import psycopg2
    import psycopg2.extras
except Exception:
    psycopg2 = None  # type: ignore


def _first_writable_dir(candidates):
    for d in candidates:
        if not d:
            continue
        try:
            os.makedirs(d, exist_ok=True)
            test_path = os.path.join(d, ".worknest_write_test")
            with open(test_path, "w", encoding="utf-8") as f:
                f.write("ok")
            os.remove(test_path)
            return d
        except Exception:
            continue
    return ""


ENV_DATA_DIR = os.getenv("WORKNEST_DATA_DIR", "").strip()
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

DB_PATH = os.getenv("WORKNEST_DB_PATH", os.path.join(DATA_DIR, "worknest.db"))
UPLOAD_DIR = os.getenv("WORKNEST_UPLOAD_DIR", os.path.join(DATA_DIR, "uploads"))

# Ensure persistence paths exist
os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
os.makedirs(UPLOAD_DIR, exist_ok=True)

# Database backend selection
DB_URL = os.getenv('DATABASE_URL') or os.getenv('WORKNEST_DB_URL') or ''
DB_IS_POSTGRES = bool(DB_URL.strip().lower().startswith(('postgres://', 'postgresql://')))
# Backwards-compat alias used by older helper functions / branches
# (Some parts of the app still reference USE_PG; keep it in sync with DB_IS_POSTGRES.)
USE_PG = DB_IS_POSTGRES


def get_conn():
    if DB_IS_POSTGRES:
        if not psycopg2:
            raise RuntimeError("psycopg2 is not installed. Add psycopg2-binary to requirements.txt")
        if not DB_URL:
            raise RuntimeError("DATABASE_URL (or WORKNEST_DB_URL) is not set.")
        return psycopg2.connect(DB_URL)
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.execute("PRAGMA foreign_keys=ON")
    return c


def _adapt_query(q: str) -> str:
    if DB_IS_POSTGRES:
        return q.replace("?", "%s")
    return q


@st.cache_data(ttl=45, show_spinner=False)
def _fetch_df_cached(q: str, params: tuple = ()):
    q = _adapt_query(q)
    c = get_conn()
    try:
        if DB_IS_POSTGRES:
            with c.cursor() as cur:
                cur.execute(q, params or ())
                cols = [d[0] for d in (cur.description or [])]
                rows = cur.fetchall() if cur.description else []
            return pd.DataFrame(rows, columns=cols)
        return pd.read_sql_query(q, c, params=params)
    finally:
        try:
            c.close()
        except Exception:
            pass


def fetch_df(q, p=()):
    params = tuple(p) if isinstance(p, (list, tuple)) else ((p,) if p not in (None, ()) else ())
    return _fetch_df_cached(str(q), params)


def execute(q, p=()):
    q = _adapt_query(q).strip()
    c = get_conn()
    try:
        if DB_IS_POSTGRES:
            with c:
                with c.cursor() as cur:
                    # Postgres compatibility: SQLite uses INSERT OR IGNORE
                    if q.lower().startswith("insert or ignore"):
                        q = "INSERT" + q[len("INSERT OR IGNORE"):]
                        if " on conflict" not in q.lower():
                            q = q.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
                    low = q.lower()

                    # For INSERTs, we *optionally* try to fetch the new id (common for SERIAL PK tables).
                    # We must use a SAVEPOINT because a failed RETURNING attempt aborts the transaction in Postgres.
                    if low.startswith("insert") and "returning" not in low:
                        cur.execute("SAVEPOINT sp_worknest_insert")
                        q2 = q.rstrip().rstrip(";") + " RETURNING id"
                        try:
                            cur.execute(q2, p)
                            row = cur.fetchone()
                            cur.execute("RELEASE SAVEPOINT sp_worknest_insert")
                            return int(row[0]) if row else None
                        except Exception as e:
                            cur.execute("ROLLBACK TO SAVEPOINT sp_worknest_insert")
                            cur.execute("RELEASE SAVEPOINT sp_worknest_insert")
                            # Some tables (e.g., app_settings) don't have an 'id' column.
                            msg = str(e)
                            if ("does not exist" in msg.lower()) and ("id" in msg.lower()):
                                cur.execute(q, p)
                                return None
                            # Fall back to plain execute for other RETURNING failures too.
                            cur.execute(q, p)
                            return None

                    cur.execute(q, p)
                    if "returning" in low:
                        row = cur.fetchone()
                        return int(row[0]) if row else None
                    return None
        else:
            cur = c.cursor()
            cur.execute(q, p)
            c.commit()
            return cur.lastrowid
    finally:
        try:
            st.cache_data.clear()
        except Exception:
            pass
        try:
            c.close()
        except Exception:
            pass

