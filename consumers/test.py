import sys
import traceback

print("Step 1: starting script", flush=True)

try:
    from confluent_kafka import Consumer
    print("Step 2: imported Consumer", flush=True)

    conf = {
        "bootstrap.servers": "localhost:29092",
        "group.id": "diagnostic-test-group",
        "client.id": "diagnostic-test",
        "auto.offset.reset": "earliest",
    }

    consumer = Consumer(conf)
    print("Step 3: Consumer object created", flush=True)

    consumer.subscribe(["order.created"])
    print("Step 4: subscribed successfully", flush=True)

    msg = consumer.poll(5.0)
    print(f"Step 5: poll returned: {msg}", flush=True)

    consumer.close()
    print("Step 6: closed cleanly", flush=True)

except Exception:
    print("EXCEPTION CAUGHT:", flush=True)
    traceback.print_exc()

print("Step 7: script reached the end", flush=True)