import json
import time
import random
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from kafka import KafkaProducer
from utils.data_generator import (
    get_user_pool,
    gen_lifestyle_record
)

TOPIC = "sleep-lifestyle"
INTERVAL = 2  # seconds between messages

# Pool of synthetic users
USERS = get_user_pool(200)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

print(f"[lifestyle_producer] streaming to topic '{TOPIC}' every {INTERVAL}s ...")

try:
    while True:
        # Pick full user object
        user = random.choice(USERS)

        # Generate record using full user dict
        record = gen_lifestyle_record(user)

        # Send using user_id as Kafka key
        producer.send(
            TOPIC,
            key=user["user_id"],
            value=record
        )

        print(
            f" → sent: user={user['user_id'][:8]}... "
            f"sleep={record['sleep_duration_hrs']}h "
            f"quality={record['sleep_quality']}"
        )

        time.sleep(INTERVAL)

except KeyboardInterrupt:
    print("Stopped.")
    producer.close()