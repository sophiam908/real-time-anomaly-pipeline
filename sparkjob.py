from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("AnomalyDetection") \
    .getOrCreate()

df = spark.read.json("s3a://sophia-real-time-pipeline-123/raw/*")

df.show()