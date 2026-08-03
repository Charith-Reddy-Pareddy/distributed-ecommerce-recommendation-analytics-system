"""Writes per-product, per-minute demand window counts to Cassandra --
the exact same counts already computed for Z-score anomaly detection
in session_and_anomaly.py, reused here for a second purpose (a queryable
time-series store) rather than recomputed from scratch.

Partitioned by (product_id, bucket_date): a day's worth of per-minute
counts for one product live in a single partition -- the standard
Cassandra time-series pattern, bounding partition size by bucketing on
time instead of letting one partition grow unboundedly.

One connection per driver process, reused across all micro-batches
(foreachBatch runs on the driver, not distributed to executors, so a
single module-level session is safe here).
"""
import os

from cassandra.cluster import Cluster

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
KEYSPACE = "ecommerce"
TABLE = "product_demand_by_minute"

_state = {}


def _get_session():
    if "insert_stmt" in _state:
        return _state["session"], _state["insert_stmt"]

    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect()

    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
    )
    session.set_keyspace(KEYSPACE)
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            product_id int,
            bucket_date date,
            event_minute timestamp,
            event_count int,
            PRIMARY KEY ((product_id, bucket_date), event_minute)
        ) WITH CLUSTERING ORDER BY (event_minute DESC)
        """
    )

    insert_stmt = session.prepare(
        f"INSERT INTO {TABLE} (product_id, bucket_date, event_minute, event_count) "
        "VALUES (?, ?, ?, ?)"
    )

    _state["session"] = session
    _state["insert_stmt"] = insert_stmt
    return session, insert_stmt


def write_demand_count(product_id: int, window_start, event_count: int) -> None:
    session, insert_stmt = _get_session()
    session.execute(insert_stmt, (product_id, window_start.date(), window_start, event_count))
