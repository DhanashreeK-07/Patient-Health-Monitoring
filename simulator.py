import random
import datetime

def generate_data():
    return {
        "patient_id": "P101",
        "timestamp": str(datetime.datetime.now()),
        "heart_rate": random.randint(60,130),
        "oxygen_level": random.randint(85,100),
        "temperature": round(random.uniform(36,39),2)
    }
