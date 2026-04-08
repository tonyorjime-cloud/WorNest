import os
import requests

def send_sms(to, message):
    url = "https://api.sendchamp.com/api/v1/sms/send"

    api_key = os.getenv("SENDCHAMP_API_KEY")
    if not api_key:
        return {
            "ok": False,
            "error": "SENDCHAMP_API_KEY is not set"
        }

    headers = {
        "Accept": "application/json,text/plain,*/*",
        "Authorization": f"Bearer {api_key}",
    }

    data = {
        "message": message,
        "sender_name": "WorkNest",
        "type": "text",
        "phone_number": to,
        "route_id": "non_dnd",
        "route": "non_dnd",
        "to": to,
    }

    try:
        response = requests.post(url, headers=headers, data=data, timeout=30)
        response.raise_for_status()

        try:
            body = response.json()
        except ValueError:
            body = response.text

        return {
            "ok": True,
            "status_code": response.status_code,
            "response": body
        }

    except requests.RequestException as e:
        body = None
        if getattr(e, "response", None) is not None:
            try:
                body = e.response.json()
            except ValueError:
                body = e.response.text

        return {
            "ok": False,
            "error": str(e),
            "response": body
        }