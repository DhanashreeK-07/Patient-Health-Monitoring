import pandas as pd
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load dataset
df = pd.read_csv("human_vital_signs_dataset_2024.csv")

for _, row in df.iterrows():
    producer.send("patient_topic", row.to_dict())
    print("Sent:", row.to_dict())
    time.sleep(0.5)   # 500 ms

producer.flush()