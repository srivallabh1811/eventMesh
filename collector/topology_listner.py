# collector/topology_listner.py

import os
import json
import psycopg2
from dotenv import load_dotenv
from confluent_kafka import Consumer

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
ANNOUNCEMENTS_TOPIC = "service.topology.announcements"

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "topology-listener",
    "auto.offset.reset": "earliest",
})

consumer.subscribe([ANNOUNCEMENTS_TOPIC])

db_conf = {
    "host": "localhost",
    "port": 5433,
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

print("[topology-listener] Listening for producer announcements...")

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
        consumer_group = announcement.get("consumer_group")

        cur.execute("""
            INSERT INTO producer_registrations
                (producer_client, topic, last_seen)
            VALUES
                (%s, %s, now())
            ON CONFLICT (producer_client, topic)
            DO UPDATE
            SET last_seen = now();
        """, (producer_client, topic))

        print(f"[topology-listener] Registered {producer_client} -> {topic}")

        if consumer_group:
            cur.execute("""
                INSERT INTO service_identities
                    (producer_client, consumer_group, last_seen)
                VALUES
                    (%s, %s, now())
                ON CONFLICT (producer_client, consumer_group)
                DO UPDATE
                SET last_seen = now();
            """, (producer_client, consumer_group))

            print(f"[topology-listener] Linked identity: {producer_client} <-> {consumer_group}")

        conn.commit()

except KeyboardInterrupt:
    print("\nStopping topology listener...")

finally:
    cur.close()
    conn.close()
    consumer.close()