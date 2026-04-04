import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    lit,
)

# Read job parameters
args = getResolvedOptions(sys.argv, ["RAW_PATH", "CLEAN_PATH"])
RAW_PATH = args["RAW_PATH"]
CLEAN_PATH = args["CLEAN_PATH"]

spark = SparkSession.builder.getOrCreate()

print("GLUE CLEANING JOB START")
print(f"RAW_PATH = {RAW_PATH}")
print(f"CLEAN_PATH = {CLEAN_PATH}")

# Load raw data
df = spark.read.json(RAW_PATH)
print("RAW SCHEMA")
df.printSchema()

# Expected fields:
# symbol: string
# price: string or double
# volume: string or double
# side: string
# timestamp: string

# Enforce schema + cast types
df_cast = (
    df
    .withColumn("price", col("price").cast("double"))
    .withColumn("volume", col("volume").cast("double"))
    .withColumn("side", col("side").cast("string"))
    .withColumn("symbol", col("symbol").cast("string"))
    .withColumn("timestamp", col("timestamp").cast("string"))
)

# Validate timestamp
df_cast = df_cast.withColumn("ts", to_timestamp("timestamp"))

# Drop rows where timestamp failed to parse
df_cast = df_cast.filter(col("ts").isNotNull())

# Validate numeric fields
df_cast = df_cast.filter(col("price").isNotNull() & (col("price") > 0))
df_cast = df_cast.filter(col("volume").isNotNull() & (col("volume") > 0))

# Validate categorical fields
valid_sides = ["buy", "sell"]
valid_symbols = ["BTC-USD", "ETH-USD", "SOL-USD", "DOGE-USD", "ADA-USD"]

df_cast = df_cast.filter(col("side").isin(valid_sides))
df_cast = df_cast.filter(col("symbol").isin(valid_symbols))

# Final cleaned dataset
df_clean = df_cast.select(
    "symbol",
    "price",
    "volume",
    "side",
    "timestamp",
    "ts"
)

print("CLEANED ROW COUNT")
print(df_clean.count())

# Write cleaned data to S3
df_clean.write.mode("overwrite").json(CLEAN_PATH)

print("GLUE CLEANING JOB DONE")
