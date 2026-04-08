import requests

url = "https://api.sendchamp.com/api/v1/sms/send"

payload = {
    "to": ["+2349066454125"],
    "message": "hello",
    "route": "non_dnd",
    "sender_name": "Sendchamp"
}
headers = {
    "accept": "application/json",
    "content-type": "application/json",
    "Authorization": "Bearer sendchamp_live_$2a$10$8uOTPekP2b0aGS/Gr0nNpuZqIZA2XF7pzkqmESZOloYPOmZfF5B16"
}

response = requests.post(url, json=payload, headers=headers)

print(response.text)