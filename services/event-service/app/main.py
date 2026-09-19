import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI

from . import schemas
from .kafka_producer import flush, publish_event
from .metrics import MetricsMiddleware, metrics_response


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    # event-service keeps no database of its own -- Kafka is its only
    # persistence, so a message still sitting in the producer's internal
    # buffer at shutdown (produce() batches for throughput; it doesn't
    # block until the broker acks) is silently lost even though the
    # client already got a 202 "accepted" response for it. flush() was
    # defined for exactly this but was never actually called anywhere.
    flush()


app = FastAPI(title="Event Ingestion Service", lifespan=lifespan)
app.add_middleware(MetricsMiddleware)


@app.get("/metrics")
def metrics():
    return metrics_response()


@app.get("/health")
def health():
    return {"status": "ok", "service": "event-service"}


@app.post("/events", response_model=schemas.EventOut, status_code=202)
async def create_event(event: schemas.EventCreate):
    payload = {
        "id": str(uuid.uuid4()),
        "user_id": event.user_id,
        "product_id": event.product_id,
        "event_type": event.event_type,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    publish_event(payload)
    return payload
