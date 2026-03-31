import os
from fastapi import FastAPI
import boto3
import pandas as pd

S3_BUCKET = os.getenv("S3_BUCKET", "my-anomaly-bucket")
PROCESSED_PREFIX = "processed/stock-data"

app = FastAPI(title="Real-Time Anomaly Detection API")


def get_s3():
    return boto3.client("s3")


def list_parquet_files():
    s3 = get_s3()
    resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=PROCESSED_PREFIX)

    contents = resp.get("Contents", [])
    return [obj["Key"] for obj in contents if obj["Key"].endswith(".parquet")]


def load_latest_parquet():
    keys = list_parquet_files()
    if not keys:
        return pd.DataFrame()

    latest_key = sorted(keys)[-1]
    s3_path = f"s3://{S3_BUCKET}/{latest_key}"

    return pd.read_parquet(s3_path)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/tickers")
def get_tickers():
    df = load_latest_parquet()
    if df.empty:
        return []

    return sorted(df["ticker"].unique().tolist())


@app.get("/prices/{ticker}")
def get_prices(ticker: str):
    df = load_latest_parquet()
    if df.empty:
        return {"error": "No data available"}

    sub = df[df["ticker"] == ticker]
    return sub.to_dict(orient="records")
