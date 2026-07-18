# app/routers/velocity.py

import psycopg2
from fastapi import APIRouter
from app.database import get_plain_connection
from app.config import settings
from app.schemas.velocity import VelocityResult
from typing import List
from engine.velocity_engine import (
    get_active_partitions,
    get_velocity_for,
    get_thresholds,
    classify_velocity,
    project_breach,
    DEFAULT_WARN_LAG,
    DEFAULT_HIGH_LAG,
)

router = APIRouter()

@router.get("/velocity", response_model=List[VelocityResult])
def get_velocity_status():
    conn = get_plain_connection()
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
            "eta_seconds": eta_seconds,
        })

    cur.close()
    conn.close()

    results.sort(key=lambda r: r["current_lag"], reverse=True)
    return results