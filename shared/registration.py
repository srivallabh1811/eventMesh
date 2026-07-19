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


def register_producer(producer_client: str, topic: str, consumer_group: str = None):
    """
    Announce that this producer writes to this topic.
    If this service is also a consumer (e.g. PaymentService consumes
    order.created AND produces payment.processed), pass its own
    consumer_group too, so the topology can link both identities
    as the same physical service.
    """
    announcement = {
        "producer_client": producer_client,
        "topic": topic,
        "consumer_group": consumer_group,
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

        if consumer_group:
            print(f"[registration] Announced: {producer_client} -> produces '{topic}' (also consumes as '{consumer_group}')")
        else:
            print(f"[registration] Announced: {producer_client} -> produces '{topic}'")
    except Exception as e:
        print(f"[registration] Failed to announce {producer_client}: {e}")