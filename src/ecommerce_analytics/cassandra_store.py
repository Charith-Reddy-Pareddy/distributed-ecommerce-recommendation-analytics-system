import re
from collections.abc import Iterable
from datetime import datetime
from typing import Protocol
from uuid import UUID

from ecommerce_analytics.schemas import EventRecord


class CassandraRow(Protocol):
    user_id: int
    occurred_at: datetime
    event_id: UUID
    product_id: int
    event_type: str
    created_at: datetime


class CassandraSession(Protocol):
    def execute(
        self, query: str, parameters: tuple[object, ...] | None = None
    ) -> Iterable[CassandraRow]: ...


class EventStoreUnavailable(RuntimeError):
    pass


class CassandraEventStore:
    def __init__(self, session: CassandraSession, keyspace: str = "ecommerce_analytics") -> None:
        if not re.fullmatch(r"[a-z][a-z0-9_]*", keyspace):
            raise ValueError("keyspace must be a lowercase Cassandra identifier")
        self._session = session
        self._keyspace = keyspace

    def ensure_schema(self) -> None:
        self._session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {self._keyspace} "
            "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
        )
        self._session.execute(
            f"CREATE TABLE IF NOT EXISTS {self._keyspace}.events_by_user ("
            "user_id bigint, occurred_at timestamp, event_id uuid, "
            "product_id bigint, event_type text, created_at timestamp, "
            "PRIMARY KEY ((user_id), occurred_at, event_id)) "
            "WITH CLUSTERING ORDER BY (occurred_at DESC, event_id ASC)"
        )

    def store(self, rec: EventRecord) -> None:
        self._session.execute(
            f"INSERT INTO {self._keyspace}.events_by_user "
            "(user_id, occurred_at, event_id, product_id, event_type, created_at) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (
                rec.user_id,
                rec.occurred_at,
                rec.id,
                rec.product_id,
                rec.event_type.value,
                rec.created_at,
            ),
        )

    def recent_for_user(self, user_id: int, limit: int = 20) -> list[EventRecord]:
        if user_id <= 0:
            raise ValueError("user_id must be positive")
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")

        try:
            rows = self._session.execute(
                f"SELECT user_id, occurred_at, event_id, product_id, event_type, created_at "
                f"FROM {self._keyspace}.events_by_user WHERE user_id = %s LIMIT %s",
                (user_id, limit),
            )
        except Exception as exc:
            raise EventStoreUnavailable("Cassandra event query failed") from exc
        return [
            EventRecord(
                id=row.event_id,
                user_id=row.user_id,
                product_id=row.product_id,
                event_type=row.event_type,
                occurred_at=row.occurred_at,
                created_at=row.created_at,
            )
            for row in rows
        ]
