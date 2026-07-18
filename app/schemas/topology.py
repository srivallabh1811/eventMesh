from pydantic import BaseModel
from datetime import datetime
from typing import List


class TopologyEdge(BaseModel):
    producer_client: str
    topic: str
    consumer_group: str
    discovered_at: datetime


class TopologyResponse(BaseModel):
    edges: List[TopologyEdge]