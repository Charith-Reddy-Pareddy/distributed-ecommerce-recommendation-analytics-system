import os
from contextlib import asynccontextmanager
from functools import lru_cache
from typing import Annotated, Protocol

from fastapi import Depends, FastAPI, HTTPException, Path, Query
from fastapi.middleware.cors import CORSMiddleware

from ecommerce_analytics.cassandra_store import (
    CassandraEventStore,
    CassandraSession,
    EventStoreUnavailable,
)
from ecommerce_analytics.kafka import (
    EventPublisher,
    KafkaPublishError,
    flush_event_publisher,
    get_event_publisher,
)
from ecommerce_analytics.schemas import EventCreate, EventRecord


class CassandraCluster(Protocol):
    def connect(self) -> CassandraSession: ...

    def shutdown(self) -> None: ...


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield
    flush_event_publisher()
    if _connect_event_store.cache_info().currsize:
        _close_event_store()


app = FastAPI(title="Event Ingestion Service", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        origin.strip()
        for origin in os.environ.get(
            "DASHBOARD_ORIGINS",
            "http://localhost:8000,http://127.0.0.1:8000",
        ).split(",")
        if origin.strip()
    ],
    allow_methods=["GET"],
    allow_headers=["Accept"],
)


@lru_cache(maxsize=1)
def _connect_event_store() -> tuple[CassandraCluster, CassandraEventStore]:
    from cassandra.cluster import Cluster

    host = os.environ.get("CASSANDRA_HOST", "localhost")
    port = int(os.environ.get("CASSANDRA_PORT", "9042"))
    keyspace = os.environ.get("CASSANDRA_KEYSPACE", "ecommerce_analytics")
    cluster = Cluster([host], port=port)
    try:
        session = cluster.connect()
        store = CassandraEventStore(session, keyspace)
        store.ensure_schema()
    except Exception as exc:
        cluster.shutdown()
        raise EventStoreUnavailable("Cassandra event store is unavailable") from exc
    return cluster, store


def _close_event_store() -> None:
    try:
        _connect_event_store()[0].shutdown()
    finally:
        _connect_event_store.cache_clear()


def get_event_store() -> CassandraEventStore:
    try:
        return _connect_event_store()[1]
    except (EventStoreUnavailable, ValueError) as exc:
        raise HTTPException(
            status_code=503, detail="Event store is unavailable"
        ) from exc


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "event-service"}


@app.post("/events", status_code=202, response_model=EventRecord)
def create_event(
    event: EventCreate,
    publisher: Annotated[EventPublisher, Depends(get_event_publisher)],
) -> EventRecord:
    try:
        return publisher.publish(event)
    except KafkaPublishError as exc:
        raise HTTPException(status_code=503, detail="Kafka producer is full") from exc


@app.get("/users/{user_id}/events", response_model=list[EventRecord])
def recent_events(
    store: Annotated[CassandraEventStore, Depends(get_event_store)],
    user_id: Annotated[int, Path(gt=0)],
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
) -> list[EventRecord]:
    try:
        return store.recent_for_user(user_id, limit)
    except (EventStoreUnavailable, ValueError) as exc:
        raise HTTPException(
            status_code=503, detail="Event store is unavailable"
        ) from exc


def main() -> None:
    import uvicorn

    uvicorn.run(
        app,
        host=os.getenv("EVENT_HOST", "0.0.0.0"),
        port=int(os.getenv("EVENT_PORT", "8002")),
    )
