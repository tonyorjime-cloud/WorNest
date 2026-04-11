# sms.py
import os
import requests

SMS_ENABLED = os.getenv("SMS_ENABLED", "true").strip().lower() == "true"

TERMII_API_KEY = os.getenv("TERMII_API_KEY", "").strip()
TERMII_SENDER_ID = os.getenv("TERMII_SENDER_ID", "WorkNest").strip()
TERMII_CHANNEL = os.getenv("TERMII_CHANNEL", "generic").strip()


def normalize_phone_number(phone: str) -> str:
    """
    Normalize Nigerian phone numbers to 234XXXXXXXXXX format.
    Examples:
        09066454125    -> 2349066454125
        +2349066454125 -> 2349066454125
        2349066454125  -> 2349066454125
    """
    if not phone:
        return ""

    phone = str(phone).strip().replace(" ", "").replace("-", "")

    if phone.startswith("+"):
        phone = phone[1:]

    if phone.startswith("0") and len(phone) == 11:
        phone = "234" + phone[1:]
    elif phone.startswith("234"):
        pass

    return phone


def send_sms(phone: str, message: str) -> bool:
    """
    Send SMS using Termii.
    Returns True if accepted by Termii, else False.
    """
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

    url = "https://api.ng.termii.com/api/sms/send"
    payload = {
        "to": phone,
        "from": TERMII_SENDER_ID,
        "sms": message,
        "type": "plain",
        "channel": TERMII_CHANNEL,
        "api_key": TERMII_API_KEY,
    }

    try:
        response = requests.post(url, json=payload, timeout=30)
        data = response.json()
        print("TERMII SMS RESPONSE:", response.status_code, data)

        return response.status_code == 200 and (
            str(data.get("code", "")).lower() == "ok" or bool(data.get("message_id"))
        )

    except Exception as e:
        print("TERMII SMS ERROR:", str(e))
        return False