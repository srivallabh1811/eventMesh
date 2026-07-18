# collector/topology_listener.py

import os
import json
import psycopg2
from dotenv import load_dotenv
from confluent_kafka import Consumer

load_dotenv()

# -----------------------------------
# Kafka Configuration
# -----------------------------------
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
ANNOUNCEMENTS_TOPIC = "service.topology.announcements"

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "topology-listener",
    "auto.offset.reset": "earliest",
})

consumer.subscribe([ANNOUNCEMENTS_TOPIC])

# -----------------------------------
# PostgreSQL Configuration
# -----------------------------------
db_conf = {
    "host": "localhost",
    "port": 5433,
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

print("[topology-listener] Listening for producer announcements...")

# -----------------------------------
# Connect to PostgreSQL once
# -----------------------------------
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

        announcement = json.loads(msg.value().decode())

        producer_client = announcement["producer_client"]
        topic = announcement["topic"]

        cur.execute("""
            INSERT INTO producer_registrations
                (producer_client, topic, last_seen)
            VALUES
                (%s, %s, now())
            ON CONFLICT (producer_client, topic)
            DO UPDATE
            SET last_seen = now();
        """, (producer_client, topic))

        conn.commit()

        print(f"[topology-listener] Registered {producer_client} -> {topic}")

except KeyboardInterrupt:
    print("\nStopping topology listener...")

finally:
    cur.close()
    conn.close()
    consumer.close()