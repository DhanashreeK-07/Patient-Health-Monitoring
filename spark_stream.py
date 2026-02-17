from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from schema import schema
from alerts import apply_alerts

spark = SparkSession.builder.appName("HealthStream").getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "health_topic") \
    .load()

parsed = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

alerts = apply_alerts(parsed)

query = alerts.writeStream.outputMode("append").format("console").start()
query.awaitTermination()
