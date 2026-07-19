# engine/cascade_predictor.py

import os
import psycopg2
from collections import defaultdict
from dotenv import load_dotenv
from engine.velocity_engine import (
    get_active_partitions,
    get_velocity_for,
    get_thresholds,
    classify_velocity,
    project_breach,
    DEFAULT_WARN_LAG,
    DEFAULT_HIGH_LAG,
)

load_dotenv()

db_conf = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5433"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

AT_RISK_ETA_THRESHOLD_SECONDS = 600  # 10 minutes


def get_at_risk_services(cur):
    """Find every consumer group that's WORSENING with a short time-to-breach."""
    partitions = get_active_partitions(cur)
    thresholds = get_thresholds(cur)
    at_risk = []

    for consumer_group, topic, partition in partitions:
        v = get_velocity_for(cur, consumer_group, topic, partition)
        if not v:
            continue

        warn_lag, high_lag = thresholds.get(
            (consumer_group, topic),
            (DEFAULT_WARN_LAG, DEFAULT_HIGH_LAG)
        )

        trend = classify_velocity(v["velocity"])
        eta_seconds = project_breach(v["current_lag"], v["velocity"], high_lag, trend)

        if trend == "WORSENING" and eta_seconds is not None and eta_seconds < AT_RISK_ETA_THRESHOLD_SECONDS:
            at_risk.append({**v, "trend": trend, "eta_seconds": eta_seconds})

    return at_risk


def get_identity_map(cur):
    """consumer_group -> producer_client, so we can bridge 'this consumer is struggling'
    into 'this is the producer identity that will eventually be affected'."""
    cur.execute("SELECT producer_client, consumer_group FROM service_identities")
    return {consumer_group: producer_client for producer_client, consumer_group in cur.fetchall()}


def get_downstream_graph(cur):
    """producer_client -> list of (topic, consumer_group) it feeds."""
    cur.execute("SELECT producer_client, topic, consumer_group FROM service_graph_edges")
    graph = defaultdict(list)
    for producer_client, topic, consumer_group in cur.fetchall():
        graph[producer_client].append((topic, consumer_group))
    return graph


def find_downstream_impact(consumer_group, identity_map, graph, visited=None, depth=0, max_depth=5):
    """
    Recursively walk forward from a struggling consumer group:
    consumer_group -> (its producer identity) -> topics it feeds -> next consumer groups -> repeat.
    'visited' guards against infinite loops in case of circular dependencies.
    """
    if visited is None:
        visited = set()

    producer_client = identity_map.get(consumer_group)
    if not producer_client or producer_client in visited or depth >= max_depth:
        return []

    visited.add(producer_client)
    impact = []

    for topic, downstream_consumer_group in graph.get(producer_client, []):
        impact.append({
            "via_topic": topic,
            "affected_consumer_group": downstream_consumer_group,
            "hops": depth + 1,
        })
        impact.extend(
            find_downstream_impact(downstream_consumer_group, identity_map, graph, visited, depth + 1, max_depth)
        )

    return impact


def predict_cascades():
    conn = psycopg2.connect(**db_conf)
    cur = conn.cursor()

    at_risk_services = get_at_risk_services(cur)
    identity_map = get_identity_map(cur)
    graph = get_downstream_graph(cur)

    predictions = []
    for service in at_risk_services:
        downstream = find_downstream_impact(service["consumer_group"], identity_map, graph)
        predictions.append({
            "at_risk_consumer_group": service["consumer_group"],
            "topic": service["topic"],
            "partition": service["partition"],
            "current_lag": service["current_lag"],
            "velocity": service["velocity"],
            "eta_seconds": service["eta_seconds"],
            "downstream_impact": downstream,
        })

    cur.close()
    conn.close()
    return predictions


if __name__ == "__main__":
    results = predict_cascades()
    for r in results:
        print(f"\n[AT RISK] {r['at_risk_consumer_group']} (partition {r['partition']}) "
              f"- breaching in ~{r['eta_seconds']:.0f}s")
        if not r["downstream_impact"]:
            print("  No downstream services currently depend on this one.")
        else:
            for impact in r["downstream_impact"]:
                print(f"  -> via '{impact['via_topic']}' affects '{impact['affected_consumer_group']}' "
                      f"({impact['hops']} hop{'s' if impact['hops'] > 1 else ''} away)")