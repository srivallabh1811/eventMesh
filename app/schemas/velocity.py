from pydantic import BaseModel
from typing import Optional


class VelocityResult(BaseModel):
    consumer_group: str
    topic: str
    partition: int
    current_lag: int
    velocity: float
    sample_count: int
    trend: str
    eta_seconds: Optional[float] = None