"""Autonomous Cassandra partition-strategy tuner.

Deliberately does NOT touch jobs/spark-streaming/cassandra_writer.py or
the Spark Structured Streaming job that writes product_demand_by_minute
-- that job is already verified and runs on a critical path; coupling a
new, independently-evolving feature into its per-row write path would put
a new external failure mode (a Cassandra hiccup, a not-yet-started
serving-optimizer) directly on that job's blast radius. Instead this
polls product_demand_by_minute on its own cycle, the same read pattern
analytics-service's cassandra_client.py already uses, and computes
its own hot/cold classification and hourly rollup independently.

The `product_demand_by_hour` table is honestly a read-pattern rollup (a
dashboard query over a wide time range reads <=24 rows/partition/day
instead of <=1440), not a partition-size fix -- at this demo's actual
event volume, product_demand_by_minute's partitions are nowhere near
Cassandra's real large-partition problem threshold. The mechanism (hot
products get a materialized coarser-grained view) is real and correctly
modeled; the demo just doesn't generate enough data for the underlying
problem it would matter for at production scale.
"""
import time
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

    volume_by_product: dict[int, int] = {}
    hourly_by_partition: dict[tuple[int, date], dict[int, int]] = {}

    for product_id, bucket_date in partitions:
        rows = session.execute(
            f"SELECT event_minute, event_count FROM {MINUTE_TABLE} "
            "WHERE product_id = %s AND bucket_date = %s",
            (product_id, bucket_date),
        )
        hourly = hourly_by_partition.setdefault((product_id, bucket_date), {})
        for row in rows:
            event_minute = _as_utc(row.event_minute)
            if event_minute >= cutoff:
                volume_by_product[product_id] = volume_by_product.get(product_id, 0) + row.event_count
            hourly[event_minute.hour] = hourly.get(event_minute.hour, 0) + row.event_count

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
