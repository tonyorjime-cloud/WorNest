import base64
import datetime as dt
import json
import os
import textwrap
import traceback
import uuid
from io import BytesIO

import pandas as pd
import requests
import streamlit as st
from PIL import Image, ImageOps
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas

from core.auth import current_staff_id, is_admin, user_role
from core.dates import _parse_date_safe, _today
from core.db import DB_IS_POSTGRES, UPLOAD_DIR, execute, fetch_df
from core.permissions import can_upload_core_docs, has_perm
from core.labels import _biweekly_timing_status, _timing_status_label, _timing_status_points
from core.tasks_service import file_download_button, save_uploaded_file
from core.utils import _PDF_ERROR_PREFIX, _is_supported_pdf_image, _normalize_ng, _pdf_error_value, _safe_pdf_text



CORE_DOC_CATEGORIES = ["architectural", "structural", "electrical", "mechanical", "soil_investigation", "boq", "program_of_work"]
STAGES = ["Substructure", "Ground Floor", "Typical Floor", "Roof", "External Works"]
TEST_TYPES_DISPLAY = [
    ("slump", "Concrete Slump Test"),
    ("concube", "Concrete Cube Test"),
    ("steel", "Steel Test (Batch)"),
    ("reinforcement", "Reinforcement Test (Batch)"),
]
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

_BIWEEKLY_CHECK_EXTRA_RULES = {
    ("concrete", "slump_test"): {"extra_key": "slump_result", "label": "Slump value", "required_status": "Compliant"},
    ("concrete", "cube_samples"): {"extra_key": "cube_count", "label": "Number of cubes", "required_status": "Compliant"},
}


def get_setting(key: str, default: str | None = None) -> str | None:
    """Read a setting from DB (app_settings)."""
    try:
        df = fetch_df("SELECT value FROM app_settings WHERE key=?", (key,))
        if not df.empty:
            v = df.iloc[0]["value"]
            return None if v is None else str(v)
    except Exception:
        pass
    return default


def set_setting(key: str, value: str | None) -> None:
    """Upsert a setting."""
    if value is None:
        execute("DELETE FROM app_settings WHERE key=?", (key,))
        return
    if DB_IS_POSTGRES:
        execute(
            """INSERT INTO app_settings(key,value) VALUES(?,?)
               ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value""",
            (key, value),
        )
    else:
        execute("""INSERT OR REPLACE INTO app_settings(key,value) VALUES(?,?)""", (key, value))


def _biweekly_start_date() -> dt.date:
    """Global reporting anchor.

    Prefer the explicit reset-aware BIWEEKLY_CYCLE_START_DATE setting. Fall back to
    legacy BIWEEKLY_START_DATE for older deployments.
    """
    for key, fallback in (("BIWEEKLY_CYCLE_START_DATE", "2026-04-14"), ("BIWEEKLY_START_DATE", "2025-10-13")):
        v = get_setting(key, fallback)
        d = _parse_date_safe(v)
        if d:
            return d
    return dt.date(2026, 4, 14)


def _biweekly_cycle_from_index(idx: int) -> dict:
    """Return the live biweekly cycle keyed by due-date anchor."""
    due = _biweekly_start_date() + dt.timedelta(days=14 * idx)
    end = due - dt.timedelta(days=1)
    start = due - dt.timedelta(days=14)
    return {
        "cycle_no": idx + 1,
        "window_start": start,
        "window_end": end,
        "due_date": due,
    }


def _biweekly_cycle_for_due(due_date: dt.date | None) -> dict | None:
    if due_date is None:
        return None
    idx = 0
    while idx < 500:
        cyc = _biweekly_cycle_from_index(idx)
        if cyc["due_date"] == due_date:
            return cyc
        if cyc["due_date"] > due_date + dt.timedelta(days=21):
            return None
        idx += 1
    return None


def _biweekly_backfill_cutoff_cycle() -> int:
    """First live cycle to enforce in the current regime."""
    try:
        return int(get_setting("BIWEEKLY_LIVE_FROM_CYCLE", "1") or 1)
    except Exception:
        return 1


@st.cache_data(ttl=45, show_spinner=False)
def _project_meta(pid: int) -> dict:
    df = fetch_df(
        "SELECT id, start_date, COALESCE(status,'ACTIVE') AS status, dormant_since, dormant_reason FROM projects WHERE id=?",
        (int(pid),),
    )
    if df.empty:
        return {"id": int(pid), "start_date": None, "status": "ACTIVE", "dormant_since": None, "dormant_reason": None}
    return df.iloc[0].to_dict()


def _project_is_dormant(pid: int) -> bool:
    try:
        return str(_project_meta(int(pid)).get("status") or "ACTIVE").upper() == "DORMANT"
    except Exception:
        return False


def _project_first_cycle_index(pid: int) -> int:
    meta = _project_meta(int(pid))
    first_live_cycle = _biweekly_cycle_from_index(max(0, _biweekly_backfill_cutoff_cycle() - 1))
    fallback_start = first_live_cycle["window_start"]
    sd = _parse_date_safe(meta.get("start_date")) or fallback_start
    idx = 0
    while idx < 500:
        cyc = _biweekly_cycle_from_index(idx)
        if cyc["window_end"] >= sd:
            return idx
        idx += 1
    return 0


@st.cache_data(ttl=45, show_spinner=False)
def _project_report_rows(pid: int) -> pd.DataFrame:
    return fetch_df(
        """SELECT id, project_id, report_date, uploaded_at, submitted_on, COALESCE(status,'PENDING') AS status,
                  cycle_no, window_start, window_end, due_date, timing_status
           FROM biweekly_reports
           WHERE project_id=?
           ORDER BY date(COALESCE(due_date, report_date)) ASC, id ASC""",
        (int(pid),),
    )


def _project_nonrejected_cycle_map(pid: int) -> dict:
    rdf = _project_report_rows(int(pid))
    existing = {}
    if not rdf.empty:
        for _, rr in rdf.iterrows():
            cyc_no = rr.get("cycle_no")
            due = _parse_date_safe(rr.get("due_date") or rr.get("report_date"))
            if cyc_no is None:
                cyc = _biweekly_cycle_for_due(due)
                cyc_no = cyc["cycle_no"] if cyc else None
            try:
                cyc_no = int(cyc_no)
            except Exception:
                continue
            status = str(rr.get("status") or "PENDING").upper()
            if status in ("SUBMITTED", "APPROVED", "NEEDS_REVISION"):
                existing[cyc_no] = status
    return existing


@st.cache_data(ttl=45, show_spinner=False)
def _project_current_due_cycle(pid: int, today: dt.date | None = None) -> tuple[dict | None, str | None]:
    today = today or _today()
    if _project_is_dormant(int(pid)):
        return None, "Dormant project — reporting is ignored."
    first_idx = _project_first_cycle_index(int(pid))
    existing = _project_nonrejected_cycle_map(int(pid))
    live_cutoff = _biweekly_backfill_cutoff_cycle()

    upcoming_idx = None
    for idx in range(first_idx, 500):
        cyc = _biweekly_cycle_from_index(idx)
        if cyc["cycle_no"] < live_cutoff:
            continue
        if today <= cyc["due_date"]:
            upcoming_idx = idx
            break
    if upcoming_idx is None:
        return None, "No reporting cycle available."

    prev_idx = upcoming_idx - 1
    if prev_idx >= first_idx:
        prev = _biweekly_cycle_from_index(prev_idx)
        if prev["cycle_no"] >= live_cutoff and prev["due_date"] < today and prev["cycle_no"] not in existing:
            return prev, None

    cyc = _biweekly_cycle_from_index(upcoming_idx)
    if cyc["cycle_no"] in existing:
        return None, "Current cycle already submitted."
    return cyc, None


def _project_missing_historical_cycles(pid: int, today: dt.date | None = None) -> list[dict]:
    today = today or _today()
    if _project_is_dormant(int(pid)):
        return []
    existing = _project_nonrejected_cycle_map(int(pid))
    first_idx = _project_first_cycle_index(int(pid))
    out = []
    cutoff = _biweekly_backfill_cutoff_cycle()
    for idx in range(first_idx, 500):
        cyc = _biweekly_cycle_from_index(idx)
        if cyc["cycle_no"] >= cutoff:
            break
        if cyc["due_date"] > today:
            break
        if cyc["cycle_no"] not in existing:
            out.append(cyc)
    return out


def _project_open_biweekly_cycle(pid: int, today: dt.date | None = None) -> tuple[dict | None, str | None]:
    """Backward-compatible wrapper: return only the one live obligation the UI should care about."""
    return _project_current_due_cycle(pid, today or _today())


def _current_biweekly_cycle_no(today: dt.date | None = None) -> int:
    """Return the cycle that is currently due next in the live regime."""
    today = today or _today()
    cutoff = _biweekly_backfill_cutoff_cycle()
    for idx in range(max(0, cutoff - 1), 500):
        cyc = _biweekly_cycle_from_index(idx)
        if today <= cyc["due_date"]:
            return int(cyc["cycle_no"])
    return int(_biweekly_cycle_from_index(499)["cycle_no"])


def _current_biweekly_cycle(today: dt.date | None = None) -> dict:
    return _biweekly_cycle_from_index(max(0, _current_biweekly_cycle_no(today) - 1))


def is_assigned_to_project(project_id: int, staff_id: int | None = None) -> bool:
    sid = staff_id if staff_id is not None else current_staff_id()
    if sid is None:
        return False
    df = fetch_df("SELECT 1 FROM project_staff WHERE project_id=? AND staff_id=?", (int(project_id), int(sid)))
    return not df.empty


def can_upload_project_outputs(project_id: int) -> bool:
    if is_admin():
        return True
    if user_role() == "sub_admin" and has_perm("can_upload_project_docs"):
        return True
    return is_assigned_to_project(project_id)


def can_upload_core_to_project(project_id: int) -> bool:
    return can_upload_core_docs()


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


def _render_biweekly_structured_details(report_row: dict):
    payload = _normalize_biweekly_structured_payload(report_row.get("structured_report_json"))
    selected = payload.get("selected_modules") or []
    if not selected:
        return False
    st.markdown(f"**Observed Activities**\n\n{', '.join(_BIWEEKLY_MODULES[m]['label'] for m in selected)}")
    for module_key in selected:
        st.markdown(f"**{_BIWEEKLY_MODULES[module_key]['label']}**\n\n{_structured_module_markdown(module_key, payload['modules'][module_key])}")
    return True


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


def _attachment_rows(kind: str, parent_id: int):
    table = 'biweekly_report_attachments' if kind == 'biweekly' else 'test_result_attachments'
    idcol = 'report_id' if kind == 'biweekly' else 'test_result_id'
    try:
        return fetch_df(f"SELECT id,file_path,caption,uploaded_at FROM {table} WHERE {idcol}=? ORDER BY id", (int(parent_id),))
    except Exception:
        return pd.DataFrame(columns=['id','file_path','caption','uploaded_at'])


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


def approve_biweekly_report(report_id: int, approver_staff_id: int | None) -> None:
    ts = dt.datetime.utcnow().isoformat(sep=' ', timespec='seconds')
    execute(
        "UPDATE biweekly_reports SET status='APPROVED', reviewed_at=?, reviewed_by_staff_id=? WHERE id=?",
        (ts, approver_staff_id, int(report_id)),
    )
    _sync_biweekly_points_for_report(int(report_id))


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


def _send_sms_notice(to_numbers, message: str):
    try:
        result = send_sms(to_numbers, message)
        print("WORKNEST SMS NOTICE:", result)
        return result
    except Exception as e:
        print("WORKNEST SMS NOTICE ERROR:", str(e))
        return {"ok": False, "reason": str(e)}

