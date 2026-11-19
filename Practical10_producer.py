import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

# Retry until Kafka broker is ready
producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=["kafka:9092"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        print("Producer connected to Kafka!")
    except Exception as e:
        print("Waiting for Kafka...", e)
        time.sleep(3)

cities = ["Mumbai", "Delhi", "Bengaluru", "Pune"]

while True:
    event = {
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "city": random.choice(cities),
        "temperature": round(random.uniform(22.0, 38.0), 2),
        "orders": random.randint(1, 15),
    }
    producer.send("live_metrics", value=event)
    producer.flush()
    print("Sent:", event)
    time.sleep(2)
