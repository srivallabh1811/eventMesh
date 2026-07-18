# app/routers/lag.py

from fastapi import APIRouter
from app.database import get_db_connection
from app.database import get_plain_connection
from app.schemas.lag import LagSample
from typing import List
router = APIRouter()


@router.get("/lag", response_model=List[LagSample])
def get_current_lag():
    """
    Returns the most recent lag sample for every (consumer_group, topic, partition).
    """
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT ON (consumer_group, topic, partition)
            consumer_group,
            topic,
            partition,
            lag,
            time
        FROM consumer_lag_samples
        ORDER BY consumer_group, topic, partition, time DESC
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return rows