"""Neon PostgreSQL connection management for RAG.

Uses psycopg2 with connection pooling suitable for Lambda (short-lived
connections via Neon's built-in pgbouncer pooler).

Requires: RAG_DATABASE_URL environment variable (Neon pooled connection string).
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

_DATABASE_URL: str | None = None


def _get_url() -> str:
    global _DATABASE_URL
    if _DATABASE_URL is None:
        _DATABASE_URL = os.environ.get("RAG_DATABASE_URL")
        if not _DATABASE_URL:
            raise RuntimeError("RAG_DATABASE_URL not set — cannot connect to vector DB")
    return _DATABASE_URL


@contextmanager
def get_connection():
    """Context manager for a database connection.

    Opens a new connection per call (Neon pooler handles connection reuse
    server-side). Commits on success, rolls back on exception.
    """
    conn = psycopg2.connect(_get_url())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def execute_query(sql: str, params: tuple | list = ()) -> list[dict]:
    """Execute a SELECT query and return results as list of dicts."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]


def execute_insert(sql: str, params: tuple | list = ()) -> None:
    """Execute an INSERT/UPDATE statement."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)


def execute_batch(sql: str, params_list: list[tuple]) -> None:
    """Execute a batch of INSERT statements efficiently."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, params_list, page_size=100)


def is_available() -> bool:
    """Check if the RAG database is reachable. Never raises.

    NOTE (2026-04-14): currently has zero callers inside alpha-engine-data.
    The ingestion pipelines call ``get_connection()`` directly, which
    hard-fails on connect errors (correct behavior while the system is
    unstable). Kept in the module in case retrieval-side consumers want
    a non-raising probe; flag for deletion if still unused after the
    cross-repo audit completes.
    """
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        return True
    except Exception as e:
        logger.warning("RAG database unavailable: %s", e)
        return False
