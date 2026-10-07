import re
from typing import Protocol

from ecommerce_analytics.schemas import EventRecord


class CassandraSession(Protocol):
    def execute(self, query: str, parameters: tuple[object, ...] | None = None) -> object: ...


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
