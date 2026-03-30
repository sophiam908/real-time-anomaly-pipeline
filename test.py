import requests

API_KEY = "qJyMcVx2fuenLNK8DOshqezch1YoFVdY"

url = f"https://api.polygon.io/v2/aggs/ticker/AAPL/prev?apiKey=qJyMcVx2fuenLNK8DOshqezch1YoFVdY"

response = requests.get(url)

print("STATUS CODE:", response.status_code)
print("DATA:", response.json())