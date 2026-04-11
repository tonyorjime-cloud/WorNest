import os
import requests

SMS_ENABLED = os.getenv("SMS_ENABLED", "true").strip().lower() == "true"
TERMII_API_KEY = os.getenv("TERMII_API_KEY", "").strip()
TERMII_SENDER_ID = os.getenv("TERMII_SENDER_ID", "Worknest").strip()
TERMII_CHANNEL = os.getenv("TERMII_CHANNEL", "generic").strip()


def normalize_phone_number(phone: str) -> str:
    """Normalize Nigerian phone numbers to 234XXXXXXXXXX format."""
    if not phone:
        return ""

    phone = str(phone).strip().replace(" ", "").replace("-", "")

    if phone.startswith("+"):
        phone = phone[1:]

    if phone.startswith("234") and phone.isdigit():
        return phone
    if phone.startswith("0") and len(phone) == 11 and phone.isdigit():
        return "234" + phone[1:]
    if len(phone) == 10 and phone.isdigit():
        return "234" + phone

    return phone if phone.isdigit() else ""


def _send_one(phone: str, message: str) -> dict:
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
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        try:
            data = response.json()
        except Exception:
            data = {"raw": response.text[:500]}

        print("TERMII SMS RESPONSE:", response.status_code, data)
        ok = response.status_code == 200 and (
            str(data.get("code", "")).strip().lower() in {"ok", "success"}
            or bool(data.get("message_id"))
        )
        return {
            "ok": ok,
            "status_code": response.status_code,
            "body": data,
            "phone": phone,
        }
    except Exception as e:
        print("TERMII SMS ERROR:", str(e))
        return {"ok": False, "reason": str(e), "phone": phone}


def send_sms(phone: str, message: str) -> bool:
    """Send a single SMS with Termii."""
    if not SMS_ENABLED:
        print("SMS is disabled.")
        return False

    if not TERMII_API_KEY:
        print("TERMII_API_KEY is missing.")
        return False

    phone = normalize_phone_number(phone)
    if not phone:
        print("Invalid phone number.")
        return False

    result = _send_one(phone, message)
    return bool(result.get("ok"))
