from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {
        "value": random.randint(60, 100)
    }
    producer.send("vitals", data)
    print("Sent:", data)
    time.sleep(2)
