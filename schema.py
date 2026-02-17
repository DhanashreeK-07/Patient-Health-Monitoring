from pyspark.sql.types import *

schema = StructType([
    StructField("patient_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("heart_rate", IntegerType()),
    StructField("oxygen_level", IntegerType()),
    StructField("temperature", DoubleType())
])
