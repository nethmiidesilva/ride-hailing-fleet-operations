"""Small PostgreSQL helper shared by the stream job, the batch job, Airflow and the API.

Why not an ORM?
---------------
Every write in this project is an *idempotent UPSERT* (``INSERT ... ON CONFLICT DO UPDATE``).
That is the mechanism that turns Spark's at-least-once delivery into effectively-once storage,
and it is the property the replay/backfill test relies on.  Writing those statements by hand and
keeping them visible in the code is easier to defend in a viva than an ORM's generated SQL, and
it avoids adding SQLAlchemy to the PySpark image.

``psycopg`` v3 is used because it ships a fast ``executemany`` and a clean context-manager API.
"""

from __future__ import annotations

import time
from collections.abc import Iterable, Iterator, Sequence
from contextlib import contextmanager
from typing import Any

import psycopg

from common.config import CFG


@contextmanager
def connect(dsn: str | None = None, autocommit: bool = True) -> Iterator[psycopg.Connection]:
    """Open a short-lived connection.

    Connections are intentionally not pooled: the Spark ``foreachBatch`` callback runs on the
    driver a few times a minute, and Airflow tasks are separate processes, so a pool would add
    lifecycle complexity for no measurable gain at this scale.
    """
    conn = psycopg.connect(dsn or CFG.pg_dsn, autocommit=autocommit)
    try:
        yield conn
    finally:
        conn.close()


def wait_for_postgres(timeout_s: int = 120, dsn: str | None = None) -> bool:
    """Block until Postgres accepts connections, or return False after ``timeout_s``.

    Compose ``depends_on: service_healthy`` already covers the normal case; this covers the
    stream job being restarted while Postgres is still replaying WAL.
    """
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with connect(dsn) as conn:
                conn.execute("SELECT 1")
            return True
        except Exception:  # noqa: BLE001 - any connection error means "not ready yet"
            time.sleep(2)
    return False


def execute(sql: str, params: Sequence[Any] | None = None, dsn: str | None = None) -> int:
    """Run a single statement and return the affected row count."""
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.rowcount


def execute_many(sql: str, rows: Iterable[Sequence[Any]], dsn: str | None = None) -> int:
    """Run one statement for many parameter tuples inside a single transaction.

    Returns the number of tuples submitted.  This is the workhorse behind every UPSERT sink:
    one round trip per micro-batch instead of one per row.
    """
    batch = list(rows)
    if not batch:
        return 0
    with connect(dsn, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, batch)
        conn.commit()
    return len(batch)


def query(
    sql: str, params: Sequence[Any] | None = None, dsn: str | None = None
) -> list[dict[str, Any]]:
    """Run a SELECT and return a list of dicts (JSON-ready for the API and evidence dumps)."""
    with connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        if cur.description is None:
            return []
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row, strict=False)) for row in cur.fetchall()]


def query_one(
    sql: str, params: Sequence[Any] | None = None, dsn: str | None = None
) -> dict[str, Any] | None:
    """Run a SELECT expected to return at most one row."""
    rows = query(sql, params, dsn)
    return rows[0] if rows else None


__all__ = ["connect", "execute", "execute_many", "query", "query_one", "wait_for_postgres"]
