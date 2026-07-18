# app/routers/thresholds.py

import psycopg2
from fastapi import APIRouter
from app.config import settings
from app.database import get_plain_connection
from app.schemas.thresholds import ThresholdEntry
from typing import List
router = APIRouter()



@router.get("/thresholds", response_model=List[ThresholdEntry])
def get_thresholds():
    """
    Returns configured WARN/HIGH lag thresholds per consumer group + topic.
    """
    conn = get_plain_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT consumer_group, topic, warn_lag, high_lag
        FROM lag_thresholds
        ORDER BY consumer_group, topic
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        {
            "consumer_group": row[0],
            "topic": row[1],
            "warn_lag": row[2],
            "high_lag": row[3],
        }
        for row in rows
    ]