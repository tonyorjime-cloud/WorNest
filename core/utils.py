import os


def has_non_compliance(data):
    for activity, fields in data.items():
        if isinstance(fields, dict):
            for v in fields.values():
                if v == "Non-compliant":
                    return True
    return False


def validate_concrete(data):
    if "Concrete Works" not in data:
        return True, ""

    cw = data["Concrete Works"]

    if not cw.get("batching_method"):
        return False, "Concrete Works: Batching method is required."

    if not cw.get("mix_ratio"):
        return False, "Concrete Works: Mix ratio is required."

    if cw.get("slump_test") == "Yes" and not cw.get("slump_result"):
        return False, "Concrete Works: Slump result required."

    if cw.get("cube_samples") == "Yes" and not cw.get("cube_count"):
        return False, "Concrete Works: Cube count required."

    return True, ""


def _normalize_ng(phone):
    if not phone:
        return None
    p = str(phone).strip().replace(" ", "").replace("-", "")
    if p.startswith("+"):
        p = p[1:]
    if p.startswith("234") and p.isdigit():
        return p
    if p.startswith("0") and len(p) == 11 and p.isdigit():
        return "234" + p[1:]
    if len(p) == 10 and p.isdigit():
        return "234" + p
    return p if p.isdigit() else None


RANK_ORDER = [
    "Higher Technical Officer",
    "Senior Technical Officer",
    "Engineer II",
    "Engineer I",
    "Senior Engineer",
    "Principal Engineer",
    "Assistant Chief Engineer",
    "Chief Engineer",
    "Assistant Director",
]
RANK_TO_INDEX = {r: i for i, r in enumerate(RANK_ORDER)}


def normalize_rank(r):
    if not r:
        return None
    r = str(r).strip()
    aliases = {
        "Asst. Director": "Assistant Director",
        "Assistant Dir": "Assistant Director",
        "Engr I": "Engineer I",
        "Engr II": "Engineer II",
        "Engineer 1": "Engineer I",
        "Engineer 2": "Engineer II",
    }
    return aliases.get(r, r)


def rank_index_safe(r):
    rr = normalize_rank(r)
    return RANK_TO_INDEX.get(rr, None)


def _normalize_handle_name(name: str) -> str:
    return " ".join(str(name or "").strip().split()).lower()


def _is_supported_pdf_image(path: str) -> bool:
    ext = os.path.splitext(str(path or "").strip())[1].lower()
    return ext in {".png", ".jpg", ".jpeg", ".webp", ".bmp"}


_PDF_ERROR_PREFIX = "__PDF_BUILD_ERROR__:"


def _pdf_error_value(exc: Exception) -> str:
    return f"{_PDF_ERROR_PREFIX}{type(exc).__name__}: {str(exc)[:220]}"


def _safe_pdf_text(v):
    s = "" if v is None else str(v)
    return s.replace("", "-").replace("", "-").replace("", "'").replace("", '"').replace("", '"')
