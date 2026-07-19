from pydantic import BaseModel
from typing import List, Optional


class CascadeImpact(BaseModel):
    via_topic: str
    affected_consumer_group: str
    hops: int


class CascadePrediction(BaseModel):
    at_risk_consumer_group: str
    topic: str
    partition: int
    current_lag: int
    velocity: float
    eta_seconds: Optional[float]
    downstream_impact: List[CascadeImpact]