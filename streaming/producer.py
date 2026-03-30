from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:
    data = {
        "ticker": "AAPL",
        "price": 150,
        "timestamp": time.time()
    }

    producer.send("stock-data", data)
    print("Sent:", data)

    time.sleep(2)