from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, mean, stddev, abs, when, to_timestamp, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark = SparkSession.builder.appName("RealTimeAnomalyDetection").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "stock-data")
    .option("startingOffsets", "latest")
    .load()
)

schema = StructType([
    StructField("ticker", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", DoubleType(), True),
])

parsed = (
    df.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
)

parsed = parsed.withColumn("timestamp", to_timestamp(col("timestamp")))

stats = (
    parsed.withWatermark("timestamp", "1 minute")
    .groupBy(window(col("timestamp"), "1 minute"), col("ticker"))
    .agg(mean("price").alias("mean"), stddev("price").alias("std"))
)

joined = parsed.join(stats, on="ticker")

result = joined.withColumn(
    "z_score", (col("price") - col("mean")) / col("std")
).withColumn(
    "anomaly", when(abs(col("z_score")) > 3, 1).otherwise(0)
)

query = (
    result.writeStream.outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
