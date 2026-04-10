import requests

url = "https://api.sendchamp.com/api/v1/sms/send"

payload = "message=check&sender_name=WorkNest%20&type=text&phone_number=2349066454125&route_id=non_dnd&route=non_dnd&to=2349066454125"
headers = {
    "Accept": "application/json,text/plain,*/*",
    "Content-Type": "application/json",
    "Authorization": "Bearer sendchamp_live_$2a$10$8uOTPekP2b0aGS/Gr0nNpuZqIZA2XF7pzkqmESZOloYPOmZfF5B16"
}

response = requests.request("POST", url, data=payload, headers=headers)

print(response.text)

RESPONSE