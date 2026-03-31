import json
import os
import boto3
import pandas as pd
from sklearn.ensemble import IsolationForest

S3_BUCKET = os.getenv("S3_BUCKET", "my-anomaly-bucket")
INPUT_KEY = "processed/stock-data"
MODEL_KEY = "models/isolation_forest.json"


def load_data():
    s3 = boto3.client("s3")
    objs = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=INPUT_KEY).get("Contents", [])

    parquet_keys = [o["Key"] for o in objs if o["Key"].endswith(".parquet")]
    if not parquet_keys:
        return pd.DataFrame()

    latest = sorted(parquet_keys)[-1]
    return pd.read_parquet(f"s3://{S3_BUCKET}/{latest}")


def main():
    df = load_data()
    if df.empty:
        print("No data found.")
        return

    X = df[["avg_price"]].values

    model = IsolationForest(contamination=0.05, random_state=42)
    model.fit(X)

    payload = {
        "n_estimators": model.n_estimators,
        "contamination": model.contamination,
    }

    boto3.client("s3").put_object(
        Bucket=S3_BUCKET,
        Key=MODEL_KEY,
        Body=json.dumps(payload).encode("utf-8"),
    )

    print(f"Saved model to s3://{S3_BUCKET}/{MODEL_KEY}")


if __name__ == "__main__":
    main()
