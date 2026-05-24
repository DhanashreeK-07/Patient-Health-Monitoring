import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Spark Session
spark = SparkSession.builder.appName("AzurePatientStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Azure Credentials
EH_NAMESPACE = os.getenv("EVENT_HUB_NAMESPACE")
EH_CONN_STR = os.getenv("EVENT_HUB_CONNECTION_STRING")
EH_SASL = f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EH_CONN_STR}";'

MYSQL_URL = f"jdbc:mysql://{os.getenv('DB_HOST')}:3306/{os.getenv('DB_NAME')}"
MYSQL_PROPERTIES = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS"),
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Define Schema based on your dataset
schema = StructType([
    StructField("Patient_ID", StringType()),
    StructField("Heart_Rate", IntegerType()),
    StructField("Blood_Pressure", StringType()),
    StructField("Temperature", DoubleType()),
    StructField("Timestamp", StringType())
])

# 1. Read Stream from Event Hubs
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", EH_NAMESPACE) \
    .option("subscribe", "patient_topic") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.jaas.config", EH_SASL) \
    .load()

# 2. Parse JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 3. Clean & Aggregate
clean_df = json_df.filter(col("Heart_Rate").isNotNull()).filter(col("Temperature") > 30)

agg_df = clean_df.groupBy("Patient_ID").agg(
    avg("Heart_Rate").alias("avg_heart_rate"),
    avg("Temperature").alias("avg_temp")
)

# 4. Write Micro-batch to MySQL
def write_to_mysql(batch_df, batch_id):
    batch_df.write.jdbc(
        url=MYSQL_URL, 
        table="patient_aggregated", 
        mode="append", 
        properties=MYSQL_PROPERTIES
    )

query = agg_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_mysql) \
    .start()

query.awaitTermination()
