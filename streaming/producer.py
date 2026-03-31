import os
import json
import time
import requests
from datetime import datetime
from kafka import KafkaProducer

# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------

# Kafka connection info
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-data")

# Alpha Vantage API key
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")

# List of tickers to rotate through
TICKERS = ["AAPL", "MSFT", "GOOG", "AMZN", "TSLA"]

# Alpha Vantage endpoint
ALPHA_URL = "https://www.alphavantage.co/query"


# ---------------------------------------------------------
# KAFKA CONNECTION
# ---------------------------------------------------------

def create_producer():
    """
    Try to connect to Kafka.
    If Kafka isn't ready yet, retry every 3 seconds.
    This function prints CONSTANTLY so you always know what's happening.
    """
    print("[BOOT] Starting Kafka producer setup...")

    while True:
        try:
            print(f"[KAFKA] Attempting connection to {KAFKA_BOOTSTRAP_SERVERS}...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print("[KAFKA] Connected successfully!")
            return producer

        except Exception as e:
            print(f"[KAFKA] Connection failed: {e}")
            print("[KAFKA] Retrying in 3 seconds...")
            time.sleep(3)


# ---------------------------------------------------------
# FETCH PRICE FROM ALPHA VANTAGE
# ---------------------------------------------------------

def fetch_price(ticker):
    """
    Fetch the latest stock price for a ticker.
    This function prints BEFORE and AFTER the API call so you always know what's happening.
    """

    print(f"[API] Requesting price for {ticker}...")

    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": ticker,
        "apikey": ALPHA_VANTAGE_API_KEY,
    }

    try:
        response = requests.get(ALPHA_URL, params=params, timeout=10)
        print(f"[API] Response status: {response.status_code}")

        data = response.json()

        # If API limit is hit, Alpha Vantage returns a note
        if "Note" in data:
            print(f"[API] RATE LIMIT HIT: {data['Note']}")
            return None, None

        price = float(data["Global Quote"]["05. price"])
        timestamp = datetime.utcnow().timestamp()

        print(f"[API] Price for {ticker}: {price}")
        return price, timestamp

    except Exception as e:
        print(f"[API] ERROR fetching price for {ticker}: {e}")
        return None, None


# ---------------------------------------------------------
# MAIN LOOP
# ---------------------------------------------------------

def main():
    print("[BOOT] Producer starting...")

    # Check API key
    if not ALPHA_VANTAGE_API_KEY:
        print("[ERROR] Missing ALPHA_VANTAGE_API_KEY environment variable!")
        raise ValueError("Missing ALPHA_VANTAGE_API_KEY")

    print("[BOOT] API key loaded successfully.")
    print(f"[BOOT] Tickers loaded: {TICKERS}")

    # Connect to Kafka
    producer = create_producer()
    print(f"[BOOT] Producer connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")

    ticker_index = 0

    # Main loop
    while True:
        ticker = TICKERS[ticker_index]
        print(f"\n[LOOP] Fetching data for {ticker} at {datetime.utcnow()}")

        price, ts = fetch_price(ticker)

        if price is not None:
            message = {
                "ticker": ticker,
                "price": price,
                "timestamp": ts,
            }
            print(f"[SEND] Sending message to Kafka: {message}")
            producer.send(KAFKA_TOPIC, value=message)
            print("[SEND] Message sent successfully!")
        else:
            print(f"[SKIP] Skipping {ticker} due to API error or rate limit.")

        # Move to next ticker
        ticker_index = (ticker_index + 1) % len(TICKERS)

        print("[SLEEP] Sleeping for 60 seconds...\n")
        time.sleep(5)


# ---------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------

if __name__ == "__main__":
    main()
