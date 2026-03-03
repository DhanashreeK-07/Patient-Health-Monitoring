from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("PatientStreaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Schema (modify based on your dataset)
schema = StructType([
    StructField("Patient_ID", StringType()),
    StructField("Heart_Rate", IntegerType()),
    StructField("Blood_Pressure", StringType()),
    StructField("Temperature", DoubleType()),
    StructField("Timestamp", StringType())
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "patient_topic") \
    .load()

# Convert JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Data Cleaning
clean_df = json_df \
    .filter(col("Heart_Rate").isNotNull()) \
    .filter(col("Temperature") > 30)

# Aggregation Example
agg_df = clean_df.groupBy("Patient_ID") \
    .agg(
        avg("Heart_Rate").alias("avg_heart_rate"),
        avg("Temperature").alias("avg_temp")
    )
