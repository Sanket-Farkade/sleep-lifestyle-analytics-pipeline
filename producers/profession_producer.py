import json, time, random, sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from kafka import KafkaProducer
from utils.data_generator import gen_user_id, gen_profession_record

TOPIC    = "profession"
INTERVAL = 8
USERS    = [gen_user_id() for _ in range(50)]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
)

print(f"[profession_producer] streaming to topic '{TOPIC}' every {INTERVAL}s ...")
try:
    while True:
        user_id = random.choice(USERS)
        record  = gen_profession_record(user_id)
        producer.send(TOPIC, key=user_id, value=record)
        print(f"  → sent: user={user_id[:8]}... job={record['job_title']} stress={record['work_stress_score']}")
        time.sleep(INTERVAL)
except KeyboardInterrupt:
    print("Stopped.")
    producer.close()