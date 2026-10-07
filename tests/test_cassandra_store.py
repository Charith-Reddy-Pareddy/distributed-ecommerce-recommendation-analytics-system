from datetime import datetime, timezone
from uuid import UUID

import pytest

from ecommerce_analytics.cassandra_store import CassandraEventStore
from ecommerce_analytics.schemas import EventRecord


class StubSession:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...] | None]] = []

    def execute(self, query: str, parameters: tuple[object, ...] | None = None) -> None:
        self.calls.append((query, parameters))


def test_store_creates_keyspace_and_user_event_table() -> None:
    sess = StubSession()

    CassandraEventStore(sess).ensure_schema()

    assert "CREATE KEYSPACE IF NOT EXISTS ecommerce_analytics" in sess.calls[0][0]
    assert "PRIMARY KEY ((user_id), occurred_at, event_id)" in sess.calls[1][0]
    assert "CLUSTERING ORDER BY (occurred_at DESC, event_id ASC)" in sess.calls[1][0]


def test_store_writes_event_fields() -> None:
    sess = StubSession()
    rec = EventRecord(
        id=UUID("11111111-1111-4111-8111-111111111111"),
        user_id=7,
        product_id=12,
        event_type="purchase",
        occurred_at=datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc),
        created_at=datetime(2026, 1, 2, 3, 5, tzinfo=timezone.utc),
    )

    CassandraEventStore(sess).store(rec)

    query, args = sess.calls[0]
    assert "INSERT INTO ecommerce_analytics.events_by_user" in query
    assert args == (7, rec.occurred_at, rec.id, 12, "purchase", rec.created_at)


def test_store_rejects_unsafe_keyspace_name() -> None:
    with pytest.raises(ValueError, match="lowercase Cassandra identifier"):
        CassandraEventStore(StubSession(), keyspace="shop; DROP TABLE events")
