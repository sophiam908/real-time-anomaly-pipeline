import json
import time
import os
from kafka import KafkaConsumer
import boto3

TOPIC = "stock-data"
BOOTSTRAP_SERVERS = "localhost:29092"

BUCKET_NAME = "sophia-real-time-pipeline-123"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True
)

s3 = boto3.client("s3")

print("📥 Listening...")

for message in consumer:
    data = message.value

    print(f"📥 RECEIVED: {data}")

    filename = f"{data['ticker']}_{int(time.time())}.json"
    local_path = f"/tmp/{filename}"

    # ✅ WRITE FILE FIRST
    try:
        with open(local_path, "w") as f:
            json.dump(data, f)

        # ✅ THEN UPLOAD
        s3.upload_file(local_path, BUCKET_NAME, f"raw/{filename}")
        print(f"☁️ Uploaded to S3: raw/{filename}")

    except Exception as e:
        print("❌ Error:", e)

    # Optional cleanup
    if os.path.exists(local_path):
        os.remove(local_path)