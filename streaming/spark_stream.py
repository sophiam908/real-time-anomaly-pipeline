from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, mean, stddev, abs, when, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# -----------------------
# 1. Spark Session
# -----------------------
spark = SparkSession.builder \
    .appName("RealTimeAnomalyDetection") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -----------------------
# 2. Read from Kafka
# -----------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "stock-data") \
    .option("startingOffsets", "latest") \
    .load()

# -----------------------
# 3. Define schema
# -----------------------
schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", DoubleType(), True)  # UNIX time
])

# -----------------------
# 4. Parse JSON
# -----------------------
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# -----------------------
# 5. Convert timestamp
# -----------------------
parsed_df = parsed_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"))
)

# -----------------------
# 6. Compute rolling stats
# -----------------------
windowed_stats = parsed_df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("ticker")
    ) \
    .agg(
        mean("price").alias("mean"),
        stddev("price").alias("std")
    )

# -----------------------
# 7. Join + anomaly detection
# -----------------------
joined = parsed_df.join(
    windowed_stats,
    on="ticker"
)

result = joined.withColumn(
    "z_score",
    (col("price") - col("mean")) / col("std")
).withColumn(
    "anomaly",
    when(abs(col("z_score")) > 3, 1).otherwise(0)
)

# -----------------------
# 8. Output to console
# -----------------------
query = result.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()