import requests, json
from dataclasses import asdict
from datetime import datetime
from yaat.db import DB_Req

# URL of your Flask API
url = 'http://localhost:8080/'  # Change the port if needed

# Making a POST request with JSON data
req = DB_Req('password', ['tic1', 'tic2'], datetime.now(), datetime.now())
response_post = requests.post(url, json=json.dumps(asdict(req)))
print("Response from POST request:", response_post.text)