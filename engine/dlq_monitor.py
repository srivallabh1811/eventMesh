# engine/dlq_monitor.py

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

db_conf = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5433"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}


def get_dlq_summary():
    conn = psycopg2.connect(**db_conf)
    cur = conn.cursor()

    cur.execute("""
        SELECT source_group, error_type, count, oldest_ts
        FROM dlq_events
        ORDER BY count DESC
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            "source_group": row[0],
            "error_type": row[1],
            "count": row[2],
            "oldest_ts": row[3].isoformat(),
        }
        for row in rows
    ]


def print_summary(events):
    if not events:
        print("[dlq-monitor] No dead-lettered events recorded.")
        return

    total = sum(e["count"] for e in events)
    print(f"\n[dlq-monitor] {total} total dead-lettered messages across {len(events)} error types")
    print("=" * 60)
    for e in events:
        print(f"  {e['source_group']:<28} {e['error_type']:<22} count={e['count']:<4} since={e['oldest_ts']}")


if __name__ == "__main__":
    events = get_dlq_summary()
    print_summary(events)