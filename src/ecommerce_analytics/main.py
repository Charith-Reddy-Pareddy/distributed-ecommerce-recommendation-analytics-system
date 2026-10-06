from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException

from ecommerce_analytics.kafka import (
    EventPublisher,
    KafkaPublishError,
    flush_event_publisher,
    get_event_publisher,
)
from ecommerce_analytics.schemas import EventCreate, EventRecord


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield
    flush_event_publisher()


app = FastAPI(title="Event Ingestion Service", lifespan=lifespan)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "event-service"}


@app.post("/events", status_code=202, response_model=EventRecord)
def create_event(
    event: EventCreate,
    publisher: EventPublisher = Depends(get_event_publisher),
) -> EventRecord:
    try:
        return publisher.publish(event)
    except KafkaPublishError as exc:
        raise HTTPException(status_code=503, detail="Kafka producer is full") from exc
