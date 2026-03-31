import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

S3_BUCKET = os.getenv("S3_BUCKET", "my-anomaly-bucket")
RAW_PREFIX = "raw/stock-data"
OUT_PREFIX = "processed/stock-data"


def main():
    spark = SparkSession.builder.appName("BatchAggregation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    input_path = f"s3a://{S3_BUCKET}/{RAW_PREFIX}/"
    output_path = f"s3a://{S3_BUCKET}/{OUT_PREFIX}/"

    df = spark.read.json(input_path)

    agg = df.groupBy("ticker").agg(avg(col("price")).alias("avg_price"))

    agg.write.mode("overwrite").parquet(output_path)
    print(f"Wrote aggregated data to {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
