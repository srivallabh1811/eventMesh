# collector/dlq_listener.py

import os
import json
import hashlib
import psycopg2
from dotenv import load_dotenv
from confluent_kafka import Consumer

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
DLQ_TOPIC = "payment.processed.dlq"
SOURCE_GROUP = "payment-service-group"  # the consumer group whose failures these are

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "dlq-listener",
    "auto.offset.reset": "earliest",
})

consumer.subscribe([DLQ_TOPIC])

db_conf = {
    "host": "localhost",
    "port": 5433,
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}


def compute_error_hash(source_group: str, error_type: str) -> str:
    raw = f"{source_group}:{error_type}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


print("[dlq-listener] Listening for dead-lettered messages...")

conn = psycopg2.connect(**db_conf)
cur = conn.cursor()

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print(msg.error())
            continue

        failed_payload = json.loads(msg.value().decode())
        error_type = failed_payload.get("error_type", "UNKNOWN")
        error_hash = compute_error_hash(SOURCE_GROUP, error_type)

        cur.execute("""
            INSERT INTO dlq_events
                (source_group, error_hash, error_type, count, oldest_ts)
            VALUES
                (%s, %s, %s, 1, now())
            ON CONFLICT (source_group, error_hash)
            DO UPDATE
            SET count = dlq_events.count + 1;
        """, (SOURCE_GROUP, error_hash, error_type))

        conn.commit()

        print(f"[dlq-listener] Recorded {error_type} for {SOURCE_GROUP} (hash: {error_hash})")

except KeyboardInterrupt:
    print("\nStopping DLQ listener...")

finally:
    cur.close()
    conn.close()
    consumer.close()