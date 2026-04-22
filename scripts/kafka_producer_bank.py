from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {
        "transaction_id": random.randint(1000, 9999),
        "amount": random.randint(100, 10000),
        "type": random.choice(["transfer", "payment", "withdraw"]),
        "status": random.choice(["NORMAL", "FRAUD"])
    }

    producer.send("bank_topic", data)
    print("Sent:", data)

    time.sleep(1)