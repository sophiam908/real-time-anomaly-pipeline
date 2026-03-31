import json
import os
import time

from kafka import KafkaProducer
import websocket

# -----------------------------
# Config
# -----------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-data")

# Coinbase product IDs (symbols)
PRODUCT_IDS = ["BTC-USD", "ETH-USD", "SOL-USD", "DOGE-USD", "ADA-USD"]

WS_URL = "wss://ws-feed.exchange.coinbase.com"


def create_kafka_producer():
    print("[BOOT] Creating Kafka producer...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=5,
    )
    print(f"[BOOT] Kafka producer connected to {KAFKA_BROKER}")
    return producer


producer = None  # global


# -----------------------------
# WebSocket callbacks
# -----------------------------
def on_message(ws, message):
    global producer
    try:
        data = json.loads(message)

        # We only care about ticker messages
        if data.get("type") != "ticker":
            return

        product_id = data.get("product_id")
        price_str = data.get("price")
        size_str = data.get("last_size")
        side = data.get("side")
        time_str = data.get("time")

        if not product_id or not price_str or not size_str or not side or not time_str:
            return

        price = float(price_str)
        volume = float(size_str)

        msg_value = {
            "symbol": product_id,
            "price": price,
            "volume": volume,
            "side": side,          # "buy" or "sell"
            "timestamp": time_str, # ISO 8601 string
        }

        print(f"[SEND] {product_id} {side} price={price} volume={volume} ts={time_str}")
        producer.send(KAFKA_TOPIC, value=msg_value)

    except Exception as e:
        print(f"[ERROR] Failed to process message: {e}")


def on_error(ws, error):
    print(f"[WS ERROR] {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"[WS CLOSE] code={close_status_code}, msg={close_msg}")


def on_open(ws):
    print(f"[WS OPEN] Connected to Coinbase WebSocket: {WS_URL}")
    sub_msg = {
        "type": "subscribe",
        "channels": [
            {
                "name": "ticker",
                "product_ids": PRODUCT_IDS,
            }
        ],
    }
    ws.send(json.dumps(sub_msg))
    print(f"[WS SUBSCRIBE] Subscribed to ticker for: {PRODUCT_IDS}")


def run_websocket():
    while True:
        try:
            print(f"[BOOT] Connecting to Coinbase WebSocket: {WS_URL}")
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever()
        except Exception as e:
            print(f"[WS FATAL] WebSocket crashed: {e}")
            print("[WS] Reconnecting in 5 seconds...")
            time.sleep(5)


def main():
    global producer
    print("Crypto producer starting...")
    print(f"[BOOT] Kafka broker: {KAFKA_BROKER}")
    print(f"[BOOT] Kafka topic: {KAFKA_TOPIC}")
    print(f"[BOOT] Symbols: {PRODUCT_IDS}")

    producer = create_kafka_producer()
    run_websocket()


if __name__ == "__main__":
    main()
