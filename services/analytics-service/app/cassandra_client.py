"""Queries the per-product, per-minute demand time series that the
Spark Structured Streaming job writes to Cassandra (see
jobs/spark-streaming/cassandra_writer.py). Cassandra is the right tool
for this specific job -- high write throughput, partitioned by
(product_id, day) -- where Postgres (used for the other analytics
endpoints in this service) is not.
"""
import os
from datetime import date, timedelta

from cassandra.cluster import Cluster

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
KEYSPACE = "ecommerce"
TABLE = "product_demand_by_minute"

_state = {}


def _get_session():
    if "session" in _state:
        return _state["session"]

    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect()
    session.set_keyspace(KEYSPACE)
    _state["session"] = session
    return session


def get_demand_timeseries(product_id: int, days: int = 1) -> list[dict]:
    """Reads across `days` daily partitions (bucket_date is part of the
    partition key), most recent day first -- the standard access
    pattern for time-bucketed Cassandra tables.
    """
    session = _get_session()
    results: list[dict] = []

    for offset in range(days):
        bucket_date = date.today() - timedelta(days=offset)
        rows = session.execute(
            f"SELECT event_minute, event_count FROM {TABLE} "
            "WHERE product_id = %s AND bucket_date = %s",
            (product_id, bucket_date),
        )
        for row in rows:
            results.append(
                {"event_minute": row.event_minute.isoformat(), "event_count": row.event_count}
            )

    results.sort(key=lambda r: r["event_minute"], reverse=True)
    return results
