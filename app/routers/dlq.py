from fastapi import APIRouter
from typing import List
from app.schemas.dlq import DLQEvent
from engine.dlq_monitor import get_dlq_summary

router = APIRouter()


@router.get("/dlq", response_model=List[DLQEvent])
def get_dlq_events():
    """
    Returns aggregated dead-letter queue events, sorted by frequency.
    """
    return get_dlq_summary()