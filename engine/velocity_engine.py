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

DEFAULT_WARN_LAG = 1
DEFAULT_HIGH_LAG = 20


def get_active_partitions(cur):
    """Find every (consumer_group, topic, partition) combo that has data."""
    cur.execute("""
        SELECT DISTINCT consumer_group, topic, partition
        FROM consumer_lag_samples
        ORDER BY consumer_group, topic, partition
    """)
    return cur.fetchall()


def get_velocity_for(cur, consumer_group, topic, partition, window=10):
    cur.execute("""
        SELECT time, lag
        FROM consumer_lag_samples
        WHERE consumer_group = %s AND topic = %s AND partition = %s
        ORDER BY time DESC
        LIMIT %s
    """, (consumer_group, topic, partition, window))

    rows = cur.fetchall()

    if len(rows) < 2:
        return None

    newest_time, newest_lag = rows[0]
    oldest_time, oldest_lag = rows[-1]

    time_diff_seconds = (newest_time - oldest_time).total_seconds()
    if time_diff_seconds == 0:
        return None

    lag_diff = newest_lag - oldest_lag
    velocity = lag_diff / time_diff_seconds

    return {
        "consumer_group": consumer_group,
        "topic": topic,
        "partition": partition,
        "current_lag": newest_lag,
        "velocity": velocity,
        "sample_count": len(rows),
    }


def classify_velocity(velocity, deadband=0.05):
    """
    Classify lag trend direction.
    deadband: velocities within +/- this range count as noise, not a real trend.
    """
    if velocity > deadband:
        return "WORSENING"
    elif velocity < -deadband:
        return "RECOVERING"
    else:
        return "STABLE"


def get_thresholds(cur):
    """Load configured thresholds, keyed by (consumer_group, topic)."""
    cur.execute("SELECT consumer_group, topic, warn_lag, high_lag FROM lag_thresholds")
    thresholds = {}
    for consumer_group, topic, warn_lag, high_lag in cur.fetchall():
        thresholds[(consumer_group, topic)] = (warn_lag, high_lag)
    return thresholds


def project_breach(current_lag, velocity, high_lag, trend):
    """Estimate seconds until breach (WORSENING) or full clear (RECOVERING)."""
    if trend == "WORSENING":
        if current_lag >= high_lag:
            return 0
        return (high_lag - current_lag) / velocity
    elif trend == "RECOVERING":
        if current_lag <= 0:
            return 0
        return current_lag / abs(velocity)
    return None


def format_eta(seconds):
    if seconds is None:
        return "-"
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        return f"{seconds/3600:.1f}h"


def print_results(results):
    if not results:
        print("(no data)")
        return

    headers = ["GROUP", "TOPIC", "PART", "LAG", "VELOCITY", "TREND", "ETA"]
    rows = [
        [r["consumer_group"], r["topic"], str(r["partition"]), str(r["current_lag"]),
         f"{r['velocity']:.4f}", r["trend"], r["eta"]]
        for r in results
    ]

    col_widths = [max(len(h), max(len(row[i]) for row in rows)) for i, h in enumerate(headers)]

    def fmt(row):
        return "  " + " | ".join(c.ljust(w) for c, w in zip(row, col_widths))

    print(fmt(headers))
    print("  " + "-+-".join("-" * w for w in col_widths))
    for row in rows:
        print(fmt(row))


def run():
    conn = psycopg2.connect(**db_conf)
    cur = conn.cursor()

    partitions = get_active_partitions(cur)
    thresholds = get_thresholds(cur)
    results = []

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

        results.append({
            **v,
            "trend": trend,
            "eta": format_eta(eta_seconds),
        })

    cur.close()
    conn.close()

    results.sort(key=lambda r: r["current_lag"], reverse=True)
    print_results(results)
    return results


if __name__ == "__main__":
    run()