from kafka import KafkaProducer
import json
import time
from simulator import generate_data
from config.kafka_config import KAFKA_BROKER, TOPIC_NAME

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:
    data = generate_data()
    producer.send(TOPIC_NAME, data)
    print("Sent:", data)
    time.sleep(2)
