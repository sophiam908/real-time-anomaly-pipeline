import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_timestamp,
    hour,
    dayofweek,
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
    VectorAssembler,
)
from pyspark.ml.iforest import IsolationForest

# -----------------------------
# Read job parameters
# -----------------------------
args = getResolvedOptions(sys.argv, ["CLEAN_PATH", "MODEL_PATH"])

CLEAN_PATH = args["CLEAN_PATH"]
MODEL_PATH = args["MODEL_PATH"]

spark = SparkSession.builder.getOrCreate()

print("=== GLUE ANOMALY DETECTION JOB START ===")
print(f"CLEAN_PATH = {CLEAN_PATH}")
print(f"MODEL_PATH = {MODEL_PATH}")

# -----------------------------
# Load cleaned data
# -----------------------------
df = spark.read.json(CLEAN_PATH)
print("=== INPUT SCHEMA ===")
df.printSchema()

# Expecting:
# price: double
# volume: double
# side: string
# symbol: string
# timestamp: string (ISO 8601)

# -----------------------------
# Time-based feature engineering
# -----------------------------
df_fe = df.withColumn("ts", to_timestamp("timestamp"))

df_fe = (
    df_fe
    .withColumn("hour_of_day", hour("ts"))
    .withColumn("day_of_week", dayofweek("ts"))
)

print("=== AFTER TIME FEATURE ENGINEERING ===")
df_fe.printSchema()

# -----------------------------
# Categorical feature engineering
# -----------------------------
side_indexer = StringIndexer(
    inputCol="side",
    outputCol="side_index",
    handleInvalid="keep",
)

symbol_indexer = StringIndexer(
    inputCol="symbol",
    outputCol="symbol_index",
    handleInvalid="keep",
)

side_encoder = OneHotEncoder(
    inputCols=["side_index"],
    outputCols=["side_ohe"],
)

symbol_encoder = OneHotEncoder(
    inputCols=["symbol_index"],
    outputCols=["symbol_ohe"],
)

# -----------------------------
# Assemble final feature vector
# -----------------------------
feature_cols = [
    "price",
    "volume",
    "hour_of_day",
    "day_of_week",
    "side_ohe",
    "symbol_ohe",
]

assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features",
)

# -----------------------------
# Isolation Forest (Anomaly Detection)
# -----------------------------
isof = IsolationForest(
    featuresCol="features",
    predictionCol="anomaly_prediction",
    anomalyScoreCol="anomaly_score",
    contamination=0.01,  # top 1% anomalies
)

# -----------------------------
# Build full pipeline
# -----------------------------
pipeline = Pipeline(stages=[
    side_indexer,
    symbol_indexer,
    side_encoder,
    symbol_encoder,
    assembler,
    isof,
])

# -----------------------------
# Train model
# -----------------------------
train_cols = [
    "price", "volume", "side", "symbol", "timestamp",
    "ts", "hour_of_day", "day_of_week"
]

df_train = df_fe.select(*train_cols).na.drop()

print(f"Training on {df_train.count()} rows...")
model = pipeline.fit(df_train)

# -----------------------------
# Save model to S3
# -----------------------------
print(f"Saving anomaly detection model to: {MODEL_PATH}")
model.write().overwrite().save(MODEL_PATH)

print("=== GLUE ANOMALY DETECTION JOB DONE ===")
