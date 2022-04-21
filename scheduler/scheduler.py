import schedule
import time
import json
from datetime import datetime
import requests

def call_api():
    response = requests.get('http://127.0.0.1:5000/launchrtwinback')
    print(response)

schedule.every().tuesday.at("00:00").do(call_api)
while True:
    n = schedule.idle_seconds()
    if n is None:
        break
    elif n > 0:
        time.sleep(n)
    schedule.run_pending()
