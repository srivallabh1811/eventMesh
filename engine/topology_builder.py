import os
import psycopg2
from collections import defaultdict
from dotenv import load_dotenv
from confluent_kafka import ConsumerGroupTopicPartitions
from confluent_kafka.admin import AdminClient
load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS")

db_conf = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5433"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})


def get_all_consumer_group_ids():
    future = admin.list_consumer_groups()
    result = future.result()
    return [g.group_id for g in result.valid]


def get_topics_for_group(group_id):
    """Which topics does this consumer group actually have committed offsets for?"""
    request = ConsumerGroupTopicPartitions(group_id)
    futures = admin.list_consumer_group_offsets([request])
    result = futures[group_id].result()

    topics = set()
    for tp in result.topic_partitions:
        if tp.offset is not None and tp.offset >= 0:
            topics.add(tp.topic)
    return topics


def get_known_producers(cur):
    """producer_client, topic pairs we've heard announcements from."""
    cur.execute("SELECT producer_client, topic FROM producer_registrations")
    return cur.fetchall()


def build_topology():
    conn = psycopg2.connect(**db_conf)
    cur = conn.cursor()

    producers = get_known_producers(cur)
    group_ids = get_all_consumer_group_ids()

    edges_found = 0

    for group_id in group_ids:
        try:
            consumed_topics = get_topics_for_group(group_id)
        except Exception as e:
            print(f"[topology-builder] Skipping group '{group_id}': {e}")
            continue

        for producer_client, produced_topic in producers:
            if produced_topic in consumed_topics:
                cur.execute("""
                    INSERT INTO service_graph_edges
                        (producer_client, topic, consumer_group, discovered_at)
                    VALUES (%s, %s, %s, now())
                    ON CONFLICT (producer_client, topic, consumer_group)
                    DO UPDATE SET discovered_at = now()
                """, (producer_client, produced_topic, group_id))
                edges_found += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"[topology-builder] Discovered/refreshed {edges_found} edges.")

    from collections import defaultdict


def print_topology(cur):
    cur.execute("""
        SELECT producer_client, topic, consumer_group
        FROM service_graph_edges
        ORDER BY producer_client, topic, consumer_group
    """)
    rows = cur.fetchall()

    if not rows:
        print("(no topology data yet)")
        return

    graph = defaultdict(list)
    for producer, topic, consumer in rows:
        graph[(producer, topic)].append(consumer)

    print("\n[topology] Service Dependency Graph")
    print("=" * 50)

    for (producer, topic), consumers in graph.items():
        print(f"\n{producer}")
        print(f"  \u2514\u2500> {topic}")
        for consumer in consumers:
            marker = "  (test/diagnostic - not a real service)" if consumer == "diagnostic-test-group" else ""
            print(f"        \u2514\u2500> {consumer}{marker}")


if __name__ == "__main__":
    build_topology()

    conn = psycopg2.connect(**db_conf)
    cur = conn.cursor()
    print_topology(cur)
    cur.close()
    conn.close()