import json
import time
import random
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer
from shared.registration import register_producer

BOOTSTRAP_SERVERS = "localhost:29092"
INPUT_TOPIC = "order.created"
OUTPUT_TOPIC = "inventory.updated"
CLIENT_ID = "inventory-service"
GROUP_ID = "inventory-service-group"

consumer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": GROUP_ID,
    "client.id": CLIENT_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
}

producer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "client.id": CLIENT_ID,
}

consumer = Consumer(consumer_conf)
producer = Producer(producer_conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"[inventory-service] Delivery failed: {err}")


def process_order(order_payload, headers_dict):
    time.sleep(random.uniform(3.0, 5.0))

    inventory_payload = {
        "order_id": order_payload["order_id"],
        "amount": order_payload["amount"],
        "stock_adjusted": True,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    out_headers = [
        ("trace-id", headers_dict.get("trace-id", b"")),
        ("origin-ts", headers_dict.get("origin-ts", b"")),
        ("event-type", b"inventory.updated"),
    ]

    return inventory_payload, out_headers


def run():
    register_producer(CLIENT_ID, OUTPUT_TOPIC, consumer_group=GROUP_ID)

    consumer.subscribe([INPUT_TOPIC])
    print(f"[inventory-service] Consuming from '{INPUT_TOPIC}' (SLOW mode)...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[inventory-service] Consumer error: {msg.error()}")
                continue

            order_payload = json.loads(msg.value())
            headers_dict = dict(msg.headers() or [])

            inventory_payload, out_headers = process_order(order_payload, headers_dict)

            producer.produce(
                topic=OUTPUT_TOPIC,
                key=inventory_payload["order_id"],
                value=json.dumps(inventory_payload),
                headers=out_headers,
                callback=delivery_report,
            )
            producer.poll(0)
            print(f"[inventory-service] Updated stock for order {inventory_payload['order_id']} (slow)")
    except KeyboardInterrupt:
        print("\n[inventory-service] Shutting down...")
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    run()