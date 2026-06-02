import json
import time
import random
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from kafka import KafkaProducer
from utils.data_generator import (
    get_user_pool,
    gen_profession_record
)

TOPIC = "profession"
INTERVAL = 8

# Pool of synthetic users
USERS = get_user_pool(200)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

print(f"[profession_producer] streaming to topic '{TOPIC}' every {INTERVAL}s ...")

try:
    while True:
        # Pick full user object
        user = random.choice(USERS)

        # Generate record using full user dict
        record = gen_profession_record(user)

        # Send using user_id as Kafka key
        producer.send(
            TOPIC,
            key=user["user_id"],
            value=record
        )

        print(
            f" → sent: user={user['user_id'][:8]}... "
            f"job={record['job_title']} "
            f"stress={record['work_stress_score']}"
        )

        time.sleep(INTERVAL)

except KeyboardInterrupt:
    print("Stopped.")
    producer.close()