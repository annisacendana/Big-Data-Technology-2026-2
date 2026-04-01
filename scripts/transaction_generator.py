import json
import time
import random
from datetime import datetime

products = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headset", "Webcam"]
cities = ["Jakarta", "Bandung", "Surabaya", "Medan", "Yogyakarta"]

while True:

    transaction = {
        "user_id": random.randint(100,200),
        "product": random.choice(products),
        "price": random.randint(100,2000),
        "city": random.choice(cities),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    filename = f"stream_data/transaction_{int(time.time())}.json"

    with open(filename,"w") as f:
        json.dump(transaction,f)

    print("Generated:", transaction)

    time.sleep(3)