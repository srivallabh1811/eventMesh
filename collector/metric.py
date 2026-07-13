import time
from datetime import datetime, timezone
from confluent_kafka import ConsumerGroupTopicPartitions
from confluent_kafka.admin import AdminClient, OffsetSpec
import psycopg2

BOOTSTRAP_SERVERS = "localhost:29092"
POLL_INTERVAL_SECONDS = 7

import os
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")

PG_CONF = {
    "host": "localhost",
    "port": 5433,
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})


def get_db_conn():
    return psycopg2.connect(**PG_CONF)


def get_all_consumer_group_ids():
    future = admin.list_consumer_groups()
    result = future.result()
    return [g.group_id for g in result.valid]


def get_group_state(group_id):
    futures = admin.describe_consumer_groups([group_id])
    desc = futures[group_id].result()
    # Strip "ConsumerGroupState." prefix for cleaner display
    return str(desc.state).replace("ConsumerGroupState.", "")


def get_committed_offsets(group_id):
    request = ConsumerGroupTopicPartitions(group_id)
    futures = admin.list_consumer_group_offsets([request])
    result = futures[group_id].result()
    return result.topic_partitions


def get_end_offsets(topic_partitions):
    request = {tp: OffsetSpec.latest() for tp in topic_partitions}
    futures = admin.list_offsets(request)

    end_offsets = {}
    for tp, future in futures.items():
        result = future.result()
        end_offsets[(tp.topic, tp.partition)] = result.offset
    return end_offsets


def lag_indicator(lag):
    """Simple visual flag based on lag severity."""
    if lag == 0:
        return "OK"
    elif lag < 20:
        return "WARN"
    else:
        return "HIGH"


def print_table(rows):
    """Print a clean aligned table of collected rows."""
    if not rows:
        print("  (no consumer group data this cycle)")
        return

    headers = ["GROUP", "TOPIC", "PART", "LAG", "STATUS", "STATE"]
    col_widths = [
        max(len(headers[0]), max(len(r[0]) for r in rows)),
        max(len(headers[1]), max(len(r[1]) for r in rows)),
        max(len(headers[2]), max(len(str(r[2])) for r in rows)),
        max(len(headers[3]), max(len(str(r[3])) for r in rows)),
        max(len(headers[4]), max(len(r[4]) for r in rows)),
        max(len(headers[5]), max(len(r[5]) for r in rows)),
    ]

    def fmt_row(cols):
        return "  " + " | ".join(str(c).ljust(w) for c, w in zip(cols, col_widths))

    print(fmt_row(headers))
    print("  " + "-+-".join("-" * w for w in col_widths))
    for r in rows:
        print(fmt_row(r))


def poll_once():
    conn = get_db_conn()
    cur = conn.cursor()
    now = datetime.now(timezone.utc)

    try:
        group_ids = get_all_consumer_group_ids()
    except Exception as e:
        print(f"[collector] Failed to list consumer groups: {e}")
        conn.close()
        return

    display_rows = []

    for group_id in group_ids:
        try:
            state = get_group_state(group_id)
            committed = get_committed_offsets(group_id)

            valid_tps = [tp for tp in committed if tp.offset is not None and tp.offset >= 0]
            if not valid_tps:
                continue

            end_offsets = get_end_offsets(valid_tps)

            for tp in valid_tps:
                end_offset = end_offsets.get((tp.topic, tp.partition))
                if end_offset is None:
                    continue

                current_offset = tp.offset
                lag = max(end_offset - current_offset, 0)

                cur.execute(
                    """
                    INSERT INTO consumer_lag_samples
                        (time, consumer_group, topic, partition, current_offset, end_offset, lag)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (now, group_id, tp.topic, tp.partition, current_offset, end_offset, lag),
                )

                display_rows.append([
                    group_id, tp.topic, tp.partition, lag, lag_indicator(lag), state
                ])

        except Exception as e:
            print(f"[collector] Error processing group '{group_id}': {e}")

    conn.commit()
    cur.close()
    conn.close()

    # Sort worst lag first, easiest to spot problems
    display_rows.sort(key=lambda r: r[3], reverse=True)

    timestamp_str = now.strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"\n[collector] Poll @ {timestamp_str}")
    print_table(display_rows)


def run():
    print(f"[collector] Starting. Polling every {POLL_INTERVAL_SECONDS}s. Ctrl+C to stop.")
    try:
        while True:
            poll_once()
            time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\n[collector] Shutting down...")


if __name__ == "__main__":
    run()