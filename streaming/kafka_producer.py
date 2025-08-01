
"""Simple Kafka producer that publishes random sales events to the 'sales_events' topic"""
import json, random, time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

products = [101, 102, 103, 104]
while True:
    event = {
        "event_id": random.randint(1, 1_000_000),
        "event_time": datetime.utcnow().isoformat(),
        "product_id": random.choice(products),
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(10, 100), 2)
    }
    producer.send("sales_events", event)
    print("Sent", event)
    time.sleep(1)
