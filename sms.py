# sms.py
import os
import requests

SMS_ENABLED = os.getenv("SMS_ENABLED", "true").strip().lower() == "true"

# SendChamp config
SENDCHAMP_API_KEY = os.getenv("SENDCHAMP_API_KEY", "").strip()
SENDCHAMP_SENDER_ID = os.getenv("SENDCHAMP_SENDER_ID", "WorkNest").strip()

# Termii config
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


def send_sms_sendchamp(phone: str, message: str) -> bool:
    """
    Send SMS using SendChamp non-DND route.
    """
    if not SENDCHAMP_API_KEY:
        print("SENDCHAMP_API_KEY is missing.")
        return False

    phone = normalize_phone_number(phone)
    if not phone:
        print("Invalid phone number for SendChamp.")
        return False

    url = "https://api.sendchamp.com/api/v1/sms/send"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {SENDCHAMP_API_KEY}",
    }
    payload = {
        "to": [phone],
        "sender_name": SENDCHAMP_SENDER_ID,   # WorkNest
        "message": message,
        "route": "non_dnd",
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=30)
        data = response.json()
        print("SENDCHAMP SMS RESPONSE:", response.status_code, data)

        return response.status_code == 200 and str(data.get("status", "")).lower() == "success"
    except Exception as e:
        print("SENDCHAMP SMS ERROR:", str(e))
        return False


def send_sms_termii(phone: str, message: str) -> bool:
    """
    Send SMS using Termii.
    """
    if not TERMII_API_KEY:
        print("TERMII_API_KEY is missing.")
        return False

    phone = normalize_phone_number(phone)
    if not phone:
        print("Invalid phone number for Termii.")
        return False

    url = "https://api.ng.termii.com/api/sms/send"
    payload = {
        "api_key": TERMII_API_KEY,
        "to": phone,
        "from": TERMII_SENDER_ID,   # WorkNest
        "sms": message,
        "type": "plain",
        "channel": TERMII_CHANNEL,
    }

    try:
        response = requests.post(url, json=payload, timeout=30)
        data = response.json()
        print("TERMII SMS RESPONSE:", response.status_code, data)

        return response.status_code == 200 and bool(data.get("message_id"))
    except Exception as e:
        print("TERMII SMS ERROR:", str(e))
        return False


def send_sms(phone: str, message: str, provider: str = "sendchamp") -> bool:
    """
    Unified SMS sender.
    provider: 'sendchamp' or 'termii'
    Default is SendChamp.
    """
    if not SMS_ENABLED:
        print("SMS is disabled.")
        return False

    provider = (provider or "sendchamp").strip().lower()

    if provider == "termii":
        return send_sms_termii(phone, message)

    return send_sms_sendchamp(phone, message)