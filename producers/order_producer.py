import json
import time
import uuid
import random
from datetime import datetime, timezone
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC = "order.created"
CLIENT_ID = "order-service"

producer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "client.id": CLIENT_ID,
}

producer = Producer(producer_conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"[order-service] Delivery failed: {err}")
    else:
        print(f"[order-service] Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def make_order_event():
    order_id = str(uuid.uuid4())
    trace_id = str(uuid.uuid4())
    origin_ts = datetime.now(timezone.utc).isoformat()

    payload = {
        "order_id": order_id,
        "customer_id": f"cust-{random.randint(1000, 9999)}",
        "amount": round(random.uniform(10, 500), 2),
        "created_at": origin_ts,
    }

    headers = [
        ("trace-id", trace_id.encode("utf-8")),
        ("origin-ts", origin_ts.encode("utf-8")),
        ("event-type", b"order.created"),
    ]

    return payload, headers


def run():
    print(f"[order-service] Producing to '{TOPIC}' every ~2s. Ctrl+C to stop.")
    try:
        while True:
            payload, headers = make_order_event()
            producer.produce(
                topic=TOPIC,
                key=payload["order_id"],
                value=json.dumps(payload),
                headers=headers,
                callback=delivery_report,
            )
            producer.poll(0)
            time.sleep(random.uniform(1.0, 2.5))
    except KeyboardInterrupt:
        print("\n[order-service] Shutting down...")
    finally:
        producer.flush()


if __name__ == "__main__":
    run()