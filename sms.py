import os
import requests

def send_sms(to, message):
    url = "https://api.sendchamp.com/api/v1/sms/send"

    api_key = os.getenv("SENDCHAMP_API_KEY")

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

    response = requests.post(url, headers=headers, data=data, timeout=30)

    return {
        "status_code": response.status_code,
        "text": response.text,
    }


result = send_sms("2349066454125", "Test from WorkNest")
print("SMS RESULT:", result)


st.write(result)