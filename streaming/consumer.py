import json
import os
import time
from kafka import KafkaConsumer
import boto3

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "stock-data")

S3_BUCKET = os.getenv("S3_BUCKET", "my-anomaly-bucket")
S3_PREFIX = os.getenv("S3_PREFIX", "raw/stock-data")


def create_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="stock-consumer-group",
    )


def main():
    consumer = create_consumer()
    s3 = boto3.client("s3")

    print("Consumer running...")

    buffer = []
    batch_size = 100

    for msg in consumer:
        buffer.append(msg.value)

        if len(buffer) >= batch_size:
            timestamp = int(time.time())
            key = f"{S3_PREFIX}/batch_{timestamp}.json"

            body = "\n".join(json.dumps(r) for r in buffer)
            s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body.encode("utf-8"))

            print(f"Wrote {len(buffer)} records to s3://{S3_BUCKET}/{key}")
            buffer = []


if __name__ == "__main__":
    main()
