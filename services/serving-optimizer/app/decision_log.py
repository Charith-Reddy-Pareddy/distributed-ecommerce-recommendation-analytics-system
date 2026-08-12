"""Records every decision the tuners make to its own Postgres database."""
import json
import threading

import psycopg2
import psycopg2.extras

from . import config

_lock = threading.Lock()


def _connect():
    return psycopg2.connect(
        host=config.PG_HOST,
        port=config.PG_PORT,
        user=config.PG_USER,
        password=config.PG_PASSWORD,
        dbname=config.PG_OPTIMIZER_DB,
    )


def init() -> None:
    with _lock, _connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tuning_decisions (
                id SERIAL PRIMARY KEY,
                ts TIMESTAMPTZ NOT NULL DEFAULT now(),
                store TEXT NOT NULL,
                action TEXT NOT NULL,
                target TEXT NOT NULL,
                reason TEXT NOT NULL,
                metadata JSONB
            )
            """
        )
        conn.commit()


def record(store: str, action: str, target: str, reason: str, metadata: dict | None = None) -> None:
    with _lock, _connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO tuning_decisions (store, action, target, reason, metadata)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (store, action, target, reason, json.dumps(metadata or {})),
        )
        conn.commit()
    print(f"[{store}] {action} target={target} reason={reason}", flush=True)


def recent(limit: int = 50) -> list[dict]:
    with _lock, _connect() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT ts, store, action, target, reason, metadata "
            "FROM tuning_decisions ORDER BY ts DESC LIMIT %s",
            (limit,),
        )
        rows = cur.fetchall()
    return [
        {
            "ts": row["ts"].isoformat(),
            "store": row["store"],
            "action": row["action"],
            "target": row["target"],
            "reason": row["reason"],
            "metadata": row["metadata"],
        }
        for row in rows
    ]
