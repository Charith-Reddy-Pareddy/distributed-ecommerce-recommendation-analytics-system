"""Consumes the Kafka `events` topic independently of
recommendation-service, building rolling per-product and daily event
counters in this service's own Postgres database.
"""
import json
import threading
from datetime import date, datetime, timezone

from sqlalchemy.dialects.postgresql import insert

from .database import SessionLocal
from .kafka_consumer import new_consumer
from .models import DailyEventCount, ProductStats

STAT_FIELD = {"view": "views", "add_to_cart": "add_to_carts", "purchase": "purchases"}


def _event_day(event: dict) -> date:
    created_at = event.get("created_at")
    if created_at:
        return datetime.fromisoformat(created_at).date()
    return datetime.now(timezone.utc).date()


def _apply_event(event: dict) -> None:
    db = SessionLocal()
    try:
        field = STAT_FIELD.get(event["event_type"])
        if field:
            stmt = (
                insert(ProductStats)
                .values(product_id=event["product_id"], **{field: 1})
                .on_conflict_do_update(
                    index_elements=["product_id"],
                    set_={field: getattr(ProductStats, field) + 1},
                )
            )
            db.execute(stmt)

        stmt = (
            insert(DailyEventCount)
            .values(day=_event_day(event), event_type=event["event_type"], count=1)
            .on_conflict_do_update(
                index_elements=["day", "event_type"],
                set_={"count": DailyEventCount.count + 1},
            )
        )
        db.execute(stmt)
        db.commit()
    finally:
        db.close()


def consume_forever() -> None:
    consumer = new_consumer()
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                continue
            _apply_event(json.loads(msg.value()))
    finally:
        consumer.close()


def start_consumer() -> None:
    thread = threading.Thread(target=consume_forever, daemon=True)
    thread.start()
