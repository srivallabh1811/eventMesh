import json
import os
import time
import random
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Consumer, Producer
from shared.registration import register_producer

# Load environment variables
load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")

DLQ_TOPIC = "payment.processed.dlq"
INPUT_TOPIC = "order.created"
OUTPUT_TOPIC = "payment.processed"
CLIENT_ID = "payment-service"
GROUP_ID = "payment-service-group"

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
        print(f"[payment-service] Delivery failed: {err}")


FAILURE_REASONS = ["CARD_DECLINED", "INSUFFICIENT_FUNDS", "GATEWAY_TIMEOUT", "FRAUD_SUSPECTED"]


def process_order(order_payload, headers_dict):
    time.sleep(random.uniform(0.1, 0.3))

    is_failure = random.random() <= 0.09

    if is_failure:
        payment_payload = {
            "order_id": order_payload["order_id"],
            "amount": order_payload["amount"],
            "status": "failed",
            "error_type": random.choice(FAILURE_REASONS),
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }
    else:
        payment_payload = {
            "order_id": order_payload["order_id"],
            "amount": order_payload["amount"],
            "status": "success",
            "processed_at": datetime.now(timezone.utc).isoformat(),
        }

    out_headers = [
        ("trace-id", headers_dict.get("trace-id", b"")),
        ("origin-ts", headers_dict.get("origin-ts", b"")),
        ("event-type", b"payment.processed"),
    ]

    return payment_payload, out_headers, is_failure

def run():
    register_producer(CLIENT_ID, OUTPUT_TOPIC, consumer_group=GROUP_ID)
    register_producer(CLIENT_ID, DLQ_TOPIC, consumer_group=GROUP_ID)
    consumer.subscribe([INPUT_TOPIC])
    print(f"[payment-service] Consuming from '{INPUT_TOPIC}'...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[payment-service] Consumer error: {msg.error()}")
                continue

            order_payload = json.loads(msg.value())
            headers_dict = dict(msg.headers() or [])

            payment_payload, out_headers, is_failure = process_order(order_payload, headers_dict)

            if is_failure:
                producer.produce(
                    topic=DLQ_TOPIC,
                    key=payment_payload["order_id"],
                    value=json.dumps(payment_payload),
                    headers=out_headers,
                    callback=delivery_report,
                )
                producer.poll(0)
                print(f"[payment-service] DEAD-LETTERED order {payment_payload['order_id']} -> {payment_payload['error_type']}")
            else:
                producer.produce(
                    topic=OUTPUT_TOPIC,
                    key=payment_payload["order_id"],
                    value=json.dumps(payment_payload),
                    headers=out_headers,
                    callback=delivery_report,
                )
                producer.poll(0)
                print(f"[payment-service] Processed order {payment_payload['order_id']} -> {payment_payload['status']}")

    except KeyboardInterrupt:
        print("\n[payment-service] Shutting down...")
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    run()