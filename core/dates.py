import datetime as dt

from dateutil import parser as dtparser


def _utcnow_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat()


def _parse_iso(s: str):
    try:
        return dtparser.parse(s) if s else None
    except Exception:
        return None


def safe_parse_date(val, default=None):
    """Parse a date-like value defensively (handles None/'NaN'/bad strings)."""
    from datetime import date as _date

    if default is None:
        default = _date.today()
    if val is None:
        return default
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return default
    try:
        return dtparser.parse(s).date()
    except Exception:
        return default


def _today() -> dt.date:
    return dt.date.today()


def _parse_date_safe(s) -> dt.date | None:
    if s is None:
        return None
    try:
        s = str(s).strip()
        if not s or s.lower() in ("nan", "none", "null"):
            return None
        return dtparser.parse(s).date()
    except Exception:
        return None


def _parse_date(v):
    """Backwards-compatible alias used by older perf code."""
    return _parse_date_safe(v)


def _next_tuesday_after(d: dt.date) -> dt.date:
    days_ahead = (1 - d.weekday()) % 7
    if days_ahead == 0:
        days_ahead = 7
    return d + dt.timedelta(days=days_ahead)


def _monthly_points_window(today: dt.date | None = None) -> tuple[dt.date, dt.date]:
    today = today or _today()
    start = today.replace(day=1)
    if start.month == 12:
        nxt = dt.date(start.year + 1, 1, 1)
    else:
        nxt = dt.date(start.year, start.month + 1, 1)
    return start, (nxt - dt.timedelta(days=1))


def _month_start(d: dt.date) -> dt.date:
    return dt.date(d.year, d.month, 1)


def _month_end(d: dt.date) -> dt.date:
    ms = _month_start(d)
    if ms.month == 12:
        nxt = dt.date(ms.year + 1, 1, 1)
    else:
        nxt = dt.date(ms.year, ms.month + 1, 1)
    return nxt - dt.timedelta(days=1)


def _is_last_day_of_month(d: dt.date) -> bool:
    return d == _month_end(d)


def _to_date_or_none(val):
    if val is None:
        return None
    if isinstance(val, dt.date):
        return val
    try:
        return dtparser.parse(str(val)).date()
    except Exception:
        return None
