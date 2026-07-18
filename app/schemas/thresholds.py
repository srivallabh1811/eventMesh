from pydantic import BaseModel


class ThresholdEntry(BaseModel):
    consumer_group: str
    topic: str
    warn_lag: int
    high_lag: int