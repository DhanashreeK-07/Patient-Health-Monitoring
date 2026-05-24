import pandas as pd
import json
import time
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

# Azure Event Hubs Configuration
EVENT_HUB_NAMESPACE = os.getenv("EVENT_HUB_NAMESPACE")
CONNECTION_STRING = os.getenv("EVENT_HUB_CONNECTION_STRING")
TOPIC_NAME = "patient_topic"

def get_producer():
    return KafkaProducer(
        bootstrap_servers=EVENT_HUB_NAMESPACE,
        security_protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        sasl_plain_username='$ConnectionString',
        sasl_plain_password=CONNECTION_STRING,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def stream_data():
    producer = get_producer()
    
    # Path relative to the src/ folder
    csv_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'human_vital_signs_dataset_2024.csv')
    
    print(f"Starting data stream to Azure Event Hubs: {EVENT_HUB_NAMESPACE}")
    df = pd.read_csv(csv_path)
    
    for _, row in df.iterrows():
        payload = row.to_dict()
        producer.send(TOPIC_NAME, payload)
        print(f"Sent: {payload}")
        time.sleep(0.5) # Simulate real-time streaming delay

    producer.flush()
    print("Streaming completed.")

if __name__ == "__main__":
    stream_data()
