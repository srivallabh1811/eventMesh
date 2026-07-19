# consumers/shipping_consumer.py

import json
import time
from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = "localhost:29092"
INPUT_TOPIC = "inventory.updated"
CLIENT_ID = "shipping-service"
GROUP_ID = "shipping-service-group"

consumer_conf = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": GROUP_ID,
    "client.id": CLIENT_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
}

consumer = Consumer(consumer_conf)


def run():
    consumer.subscribe([INPUT_TOPIC])
    print(f"[shipping-service] Consuming from '{INPUT_TOPIC}'...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[shipping-service] Consumer error: {msg.error()}")
                continue

            payload = json.loads(msg.value())
            print(f"[shipping-service] Preparing shipment for order {payload['order_id']}")
            time.sleep(0.5)  # simulate quick processing

    except KeyboardInterrupt:
        print("\n[shipping-service] Shutting down...")
    finally:
        consumer.close()


if __name__ == "__main__":
    run()