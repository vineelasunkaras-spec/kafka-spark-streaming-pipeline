"""Publishes synthetic sensor events to Kafka."""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

SITES = ["NJ-01", "NJ-02", "TX-01", "AZ-01"]


def make_event(device_id: str | None = None) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "device_id": device_id or f"dev-{random.randint(1, 200):04d}",
        "site": random.choice(SITES),
        "temperature_c": round(random.gauss(65, 8), 2),
        "pressure_kpa": round(random.gauss(101, 3), 2),
        "event_ts": datetime.now(timezone.utc).isoformat(),
    }


def main():
    from kafka import KafkaProducer

    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="sensor-events")
    p.add_argument("--rate", type=int, default=20, help="events per second")
    a = p.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=a.bootstrap,
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v).encode(),
        acks="all",
        enable_idempotence=True,
        linger_ms=10,
    )
    while True:
        e = make_event()
        producer.send(a.topic, key=e["device_id"], value=e)
        time.sleep(1 / a.rate)


if __name__ == "__main__":
    main()
