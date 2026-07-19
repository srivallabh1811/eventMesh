from pydantic import BaseModel

class DLQEvent(BaseModel):
    source_group: str
    error_type: str
    count: int
    oldest_ts: str