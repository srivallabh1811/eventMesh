from pydantic import BaseModel
from datetime import datetime


class LagSample(BaseModel):
    consumer_group: str
    topic: str
    partition: int
    lag: int
    time: datetime