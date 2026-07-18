# app/database.py

import psycopg2
from psycopg2.extras import RealDictCursor
from app.config import settings


# app/database.py

import psycopg2
from psycopg2.extras import RealDictCursor
from app.config import settings


def get_db_connection():
    """Returns a connection with dict-style rows (for simple direct-dict endpoints)."""
    return psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        cursor_factory=RealDictCursor,
    )


def get_plain_connection():
    """Returns a connection with plain tuple rows (for reusing engine/ logic that expects tuples)."""
    return psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )