import datetime as dt


def _biweekly_timing_status(due: dt.date | None, submitted: dt.date | None) -> str:
    """Classify report timing using the live 3/2/1 regime."""
    if due is None or submitted is None:
        return "ON_TIME"
    if submitted <= due:
        return "ON_TIME"
    if submitted <= (due + dt.timedelta(days=7)):
        return "LATE"
    return "VERY_LATE"


def _timing_status_points(timing_status: str | None) -> int:
    timing = str(timing_status or "").upper()
    if timing == "VERY_LATE":
        return 1
    if timing == "LATE":
        return 2
    return 3


def _timing_status_label(timing_status: str | None) -> str:
    timing = str(timing_status or "").upper()
    if timing == "VERY_LATE":
        return "Very late"
    if timing == "LATE":
        return "Late"
    if timing == "ON_TIME":
        return "On time"
    return ""


def _report_cycle_status_label(cycle: dict, submitted_on: dt.date | None = None) -> str:
    if submitted_on is None:
        return "Awaiting upload"
    return _timing_status_label(_biweekly_timing_status(cycle.get("due_date"), submitted_on))


def _editable_status(status: str | None) -> bool:
    return str(status or "PENDING").upper() in {"DRAFT", "PENDING", "SUBMITTED", "NEEDS_REVISION"}
