# shared/registration.py

import os
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")
ANNOUNCEMENTS_TOPIC = "service.topology.announcements"

_registration_producer = Producer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "client.id": "topology-announcer",
})


def _delivery_report(err, msg):
    if err is not None:
        print(f"[registration] Announcement delivery failed: {err}")


def register_producer(producer_client: str, topic: str):
    """
    Announce that this producer writes to this topic.
    Publishes a small message to the topology announcements topic
    instead of writing to the database directly.
    """
    announcement = {
        "producer_client": producer_client,
        "topic": topic,
        "announced_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
        _registration_producer.produce(
            topic=ANNOUNCEMENTS_TOPIC,
            key=producer_client,
            value=json.dumps(announcement),
            callback=_delivery_report,
        )
        _registration_producer.flush(timeout=5)
        print(f"[registration] Announced: {producer_client} -> produces '{topic}'")
    except Exception as e:
        print(f"[registration] Failed to announce {producer_client}: {e}")