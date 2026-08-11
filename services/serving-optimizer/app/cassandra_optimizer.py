"""Autonomous Cassandra partition-strategy tuner.

Polls product_demand_by_minute on its own cycle (same read pattern as
analytics-service's cassandra_client.py) instead of writing from inside
the verified Spark Streaming job -- keeps that job's critical path free
of a new failure mode from this independently-evolving feature.

product_demand_by_hour is a read-pattern rollup, not a partition-size
fix: at this demo's event volume, minute-level partitions are nowhere
near Cassandra's real large-partition threshold. The mechanism (hot
products get a coarser materialized view) is real; the demo just
doesn't generate enough data for the problem it solves at scale.
"""
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

from cassandra.cluster import Cluster

from . import config, decision_log

KEYSPACE = "ecommerce"
MINUTE_TABLE = "product_demand_by_minute"
HOUR_TABLE = "product_demand_by_hour"
STRATEGY_TABLE = "partition_strategy"

_state = {}


def _get_session():
    if "session" in _state:
        return _state["session"]

    cluster = Cluster([config.CASSANDRA_HOST])
    session = cluster.connect()
    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} "
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace(KEYSPACE)
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {STRATEGY_TABLE} (
            product_id int PRIMARY KEY,
            granularity text,
            recent_volume int,
            updated_at timestamp,
            reason text
        )
        """
    )
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {HOUR_TABLE} (
            product_id int,
            bucket_date date,
            event_hour int,
            event_count int,
            PRIMARY KEY ((product_id, bucket_date), event_hour)
        ) WITH CLUSTERING ORDER BY (event_hour DESC)
        """
    )
    _state["session"] = session
    return session


def _as_date(value) -> date:
    # cassandra-driver returns CQL `date` columns as its own cassandra.util.Date
    # (a days-since-epoch wrapper), not stdlib datetime.date -- .date() converts.
    return value.date() if hasattr(value, "date") else value


def _recent_partitions(session, days: int = 2) -> set[tuple[int, date]]:
    # SELECT DISTINCT over partition-key-only columns is a supported,
    # efficient CQL pattern (partition summary scan, not a full data scan).
    rows = session.execute(f"SELECT DISTINCT product_id, bucket_date FROM {MINUTE_TABLE}")
    cutoff = date.today() - timedelta(days=days)
    return {(r.product_id, _as_date(r.bucket_date)) for r in rows if _as_date(r.bucket_date) >= cutoff}


def _as_utc(dt: datetime) -> datetime:
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def run_cycle() -> None:
    session = _get_session()
    partitions = _recent_partitions(session)
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=config.CASSANDRA_LOOKBACK_MINUTES)

    volume_by_product: dict[int, int] = defaultdict(int)
    hourly_by_partition: dict[tuple[int, date], dict[int, int]] = defaultdict(lambda: defaultdict(int))

    for product_id, bucket_date in partitions:
        rows = session.execute(
            f"SELECT event_minute, event_count FROM {MINUTE_TABLE} "
            "WHERE product_id = %s AND bucket_date = %s",
            (product_id, bucket_date),
        )
        hourly = hourly_by_partition[(product_id, bucket_date)]
        for row in rows:
            event_minute = _as_utc(row.event_minute)
            if event_minute >= cutoff:
                volume_by_product[product_id] += row.event_count
            hourly[event_minute.hour] += row.event_count

    for (product_id, bucket_date), hours in hourly_by_partition.items():
        for hour, count in hours.items():
            session.execute(
                f"INSERT INTO {HOUR_TABLE} (product_id, bucket_date, event_hour, event_count) "
                "VALUES (%s, %s, %s, %s)",
                (product_id, bucket_date, hour, count),
            )

    current = {
        r.product_id: r.granularity
        for r in session.execute(f"SELECT product_id, granularity FROM {STRATEGY_TABLE}")
    }

    for product_id, volume in volume_by_product.items():
        is_hot = volume >= config.CASSANDRA_HOT_VOLUME_THRESHOLD
        granularity = "hot" if is_hot else "cold"
        if current.get(product_id) == granularity:
            continue

        reason = (
            f"{volume} events in last {config.CASSANDRA_LOOKBACK_MINUTES}m "
            f"{'>=' if is_hot else '<'} threshold {config.CASSANDRA_HOT_VOLUME_THRESHOLD}"
        )
        session.execute(
            f"INSERT INTO {STRATEGY_TABLE} (product_id, granularity, recent_volume, updated_at, reason) "
            "VALUES (%s, %s, %s, %s, %s)",
            (product_id, granularity, volume, datetime.now(timezone.utc), reason),
        )
        decision_log.record(
            "cassandra",
            "mark_hot" if is_hot else "mark_cold",
            f"product_id={product_id}",
            reason,
            {"recent_volume": volume},
        )


def get_status() -> dict:
    session = _get_session()
    granularities = [r.granularity for r in session.execute(f"SELECT granularity FROM {STRATEGY_TABLE}")]
    return {"hot_products": granularities.count("hot"), "cold_products": granularities.count("cold")}


def run_forever() -> None:
    while True:
        try:
            run_cycle()
        except Exception as e:
            print(f"[cassandra] cycle error: {e}", flush=True)
        time.sleep(config.POLL_INTERVAL_SECONDS)
