import os
import requests

def send_sms(to, message):
    url = "https://api.sendchamp.com/api/v1/sms/send"

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {os.getenv('SENDCHAMP_API_KEY')}",
        "Content-Type": "application/json",
    }

    payload = {
        "to": [str(to)],
        "message": str(message),
        "sender_name": "WorkNest",
        "route": "non_dnd",
    }

    response = requests.post(url, json=payload, headers=headers, timeout=30)

    try:
        body = response.json()
    except Exception:
        body = response.text

    print("SENDCHAMP SMS RESPONSE:", response.status_code, body)

    return {
        "ok": response.ok,
        "status_code": response.status_code,
        "body": body,
    }