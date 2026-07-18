
import psycopg2
from fastapi import APIRouter
from app.config import settings
from app.database import get_plain_connection
from app.schemas.topology import TopologyResponse


router = APIRouter()

@router.get("/topology", response_model=TopologyResponse)
def get_topology():
    """
    Returns the discovered service dependency graph as a flat list of edges.
    """
    conn = get_plain_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT producer_client, topic, consumer_group, discovered_at
        FROM service_graph_edges
        ORDER BY producer_client, topic, consumer_group
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    edges = [
        {
            "producer_client": row[0],
            "topic": row[1],
            "consumer_group": row[2],
            "discovered_at": row[3].isoformat(),
        }
        for row in rows
    ]

    return {"edges": edges}