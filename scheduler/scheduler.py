import requests

requests.get('http://54.198.223.242/launchrtwinback')

# schedule.every().tuesday.at("00:00").do(call_api)
# while True:
#     n = schedule.idle_seconds()
#     if n is None:
#         break
#     elif n > 0:
#         time.sleep(n)
#     schedule.run_pending()
