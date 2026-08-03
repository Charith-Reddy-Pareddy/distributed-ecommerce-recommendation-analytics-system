"""Consumes the same Redis Stream as recommendation-service, but builds
a completely independent read model: rolling per-product counters and
daily event counts, persisted to this service's own Postgres database.

Each consumer replays the stream from the beginning on startup (via
XRANGE) so its aggregates are always fully rebuildable from the event
log, then tails new events with a blocking XREAD loop.
"""
import json
import threading
from datetime import date, datetime, timezone

from sqlalchemy.dialects.postgresql import insert

from .database import SessionLocal
from .models import DailyEventCount, ProductStats
from .redis_client import STREAM_NAME, redis_client

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
    last_id = "0-0"
    for entry_id, fields in redis_client.xrange(STREAM_NAME, min="-", max="+"):
        _apply_event(json.loads(fields["data"]))
        last_id = entry_id

    while True:
        response = redis_client.xread({STREAM_NAME: last_id}, block=5000, count=100)
        if not response:
            continue
        for _, entries in response:
            for entry_id, fields in entries:
                _apply_event(json.loads(fields["data"]))
                last_id = entry_id


def start_consumer() -> None:
    thread = threading.Thread(target=consume_forever, daemon=True)
    thread.start()
