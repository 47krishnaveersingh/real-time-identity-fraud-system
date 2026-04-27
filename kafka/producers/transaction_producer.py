import json
import time
import random
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

users = ["user1", "user2", "user3"]

while True:
    event = {
        "user_id": random.choice(users),
        "amount": random.randint(100, 5000),
        "location": random.choice(["IN", "US", "UK"]),
        "timestamp": time.time()
    }

    producer.send("transaction_events", event)
    print(f"Sent: {event}")

    time.sleep(2)