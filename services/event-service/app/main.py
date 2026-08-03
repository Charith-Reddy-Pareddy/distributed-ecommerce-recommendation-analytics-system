from fastapi import Depends, FastAPI
from sqlalchemy.orm import Session

from . import models, schemas
from .database import engine, get_db
from .stream import publish_event

models.Base.metadata.create_all(bind=engine)

app = FastAPI(title="Event Ingestion Service")


@app.get("/health")
def health():
    return {"status": "ok", "service": "event-service"}


@app.post("/events", response_model=schemas.EventOut, status_code=201)
def create_event(event: schemas.EventCreate, db: Session = Depends(get_db)):
    db_event = models.Event(**event.model_dump())
    db.add(db_event)
    db.commit()
    db.refresh(db_event)

    publish_event(
        {
            "id": db_event.id,
            "user_id": db_event.user_id,
            "product_id": db_event.product_id,
            "event_type": db_event.event_type,
            "created_at": db_event.created_at.isoformat(),
        }
    )

    return db_event


@app.get("/events", response_model=list[schemas.EventOut])
def list_events(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return (
        db.query(models.Event)
        .order_by(models.Event.id.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )
