# producers/profession_producer.py
"""
Streams profession/work records to the 'profession' Kafka topic.
Interval: every 8 seconds (one random user per tick).
"""

import json
import time
import logging
import sys
import os

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.data_generator import get_user_pool, gen_profession_record

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC             = "profession"
INTERVAL_SEC      = 8
MAX_RETRIES       = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [profession] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def make_producer(retries=MAX_RETRIES):
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=3,
                linger_ms=5,
            )
            log.info("Connected to Kafka at %s", BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            wait = 2 ** attempt
            log.warning("Kafka not reachable (attempt %d/%d). Retrying in %ds …",
                        attempt, retries, wait)
            time.sleep(wait)
    log.error("Could not connect to Kafka after %d attempts. Exiting.", retries)
    sys.exit(1)


def on_send_error(exc):
    log.error("Failed to send record: %s", exc)


def main():
    users    = get_user_pool()
    producer = make_producer()
    sent     = 0

    log.info("Profession producer started → topic: %s  interval: %ds", TOPIC, INTERVAL_SEC)

    try:
        while True:
            user   = users[sent % len(users)]
            record = gen_profession_record(user)

            producer.send(
                TOPIC,
                key=record["user_id"],
                value=record,
            ).add_errback(on_send_error)

            sent += 1
            if sent % 50 == 0:
                log.info("Sent %d profession records", sent)

            time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        log.info("Shutting down profession producer (sent %d records total).", sent)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
