import uuid
from datetime import datetime, timezone

from fastapi import FastAPI

from . import schemas
from .kafka_producer import publish_event
from .metrics import MetricsMiddleware, metrics_response

app = FastAPI(title="Event Ingestion Service")
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
