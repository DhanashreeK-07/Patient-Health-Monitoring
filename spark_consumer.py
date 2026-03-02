from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, IntegerType
import os

spark = SparkSession.builder \
    .appName("VitalSignsConsumer") \
    .getOrCreate()

schema = StructType().add("value", IntegerType())

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "vitals") \
    .load()

value_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.value")

query = value_df \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "spark_output") \
    .option("checkpointLocation", "checkpoint") \
    .start()

query.awaitTermination()
