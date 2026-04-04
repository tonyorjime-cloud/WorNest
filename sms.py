import os
import requests

def send_sms(to, message):
    url = "https://api.sendchamp.com/api/v1/sms/send"

    headers = {
        "Authorization": f"Bearer {os.getenv('SENDCHAMP_API_KEY')}",
        "Content-Type": "application/json"
    }

    payload = {
        "to": to,
        "message": message,
        "sender_name": "Sendchamp",
        "route": "non_dnd"
    }

    response = requests.post(url, json=payload, headers=headers)

    return response.json()