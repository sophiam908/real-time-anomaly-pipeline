import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_timestamp,
    hour,
    dayofweek,
    col,
)
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer,
    OneHotEncoder,
    VectorAssembler,
)
from pyspark.ml.clustering import KMeans
from pyspark.ml.functions import vector_to_array
from pyspark.sql.functions import udf

# Read job parameters
args = getResolvedOptions(sys.argv, ["CLEAN_PATH", "MODEL_PATH"])

CLEAN_PATH = args["CLEAN_PATH"]
MODEL_PATH = args["MODEL_PATH"]

spark = SparkSession.builder.getOrCreate()

print("GLUE ANOMALY DETECTION JOB START")
print(f"CLEAN_PATH = {CLEAN_PATH}")
print(f"MODEL_PATH = {MODEL_PATH}")

# Load cleaned data
df = spark.read.json(CLEAN_PATH)

print("INPUT SCHEMA")
df.printSchema()

# Time-based feature engineering
df_fe = df.withColumn("ts", to_timestamp("timestamp"))

df_fe = (
    df_fe
    .withColumn("hour_of_day", hour("ts"))
    .withColumn("day_of_week", dayofweek("ts"))
)

print("AFTER TIME FEATURE ENGINEERING")
df_fe.printSchema()

# Categorical feature engineering
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

# Assemble feature vector
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

# KMeans Model (Anomaly Detection)
kmeans = KMeans(
    featuresCol="features",
    predictionCol="cluster",
    k=5,
)

# Build pipeline
pipeline = Pipeline(stages=[
    side_indexer,
    symbol_indexer,
    side_encoder,
    symbol_encoder,
    assembler,
    kmeans,
])

# Prepare training data
train_cols = [
    "price",
    "volume",
    "side",
    "symbol",
    "timestamp",
    "hour_of_day",
    "day_of_week",
]

df_train = df_fe.select(*train_cols).na.drop()

print(f"Training on {df_train.count()} rows...")

# Train model
model = pipeline.fit(df_train)

# Transform data
predictions = model.transform(df_train)

# Compute anomaly scores
centers = model.stages[-1].clusterCenters()

# Convert vector to array
predictions = predictions.withColumn(
    "features_array",
    vector_to_array("features")
)

# Distance function
def compute_distance(features, cluster):
    center = centers[cluster]
    return float(sum((features[i] - center[i])**2 for i in range(len(features))) ** 0.5)

distance_udf = udf(compute_distance, DoubleType())

predictions = predictions.withColumn(
    "anomaly_score",
    distance_udf(col("features_array"), col("cluster"))
)

# Flag top 1% anomalies
threshold = predictions.approxQuantile(
    "anomaly_score", [0.99], 0.01
)[0]

print(f"Anomaly threshold (99th percentile): {threshold}")

predictions = predictions.withColumn(
    "anomaly_prediction",
    (col("anomaly_score") >= threshold).cast("int")
)

# Save model
print(f"Saving model to: {MODEL_PATH}")
model.write().overwrite().save(MODEL_PATH)

# Save scored data
output_path = MODEL_PATH + "/scored_data"

print(f"Saving scored data to: {output_path}")
predictions.write.mode("overwrite").parquet(output_path)

print("GLUE ANOMALY DETECTION JOB DONE")
