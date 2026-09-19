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
    # This runs in a background daemon thread (see start_consumer()) --
    # an uncaught exception here just kills the thread silently, with
    # nothing in the logs to say so and the HTTP server staying up and
    # "healthy" the whole time. Log loudly and keep polling instead,
    # matching the same fix already applied to recommendation-service's
    # consumer thread (see services/recommendation-service/app/model.py
    # and docs/ARCHITECTURE.md's trade-offs section) -- this consumer
    # had the identical gap.
    print("[analytics-consumer] consumer thread starting", flush=True)
    try:
        consumer = new_consumer()
    except Exception as e:
        print(f"[analytics-consumer] failed to create consumer: {e!r}", flush=True)
        raise
    print("[analytics-consumer] consumer created, subscribed, polling...", flush=True)

    processed = 0
    try:
        while True:
            try:
                msg = consumer.poll(timeout=1.0)
            except Exception as e:
                print(f"[analytics-consumer] poll() raised: {e!r}", flush=True)
                continue
            if msg is None:
                continue
            if msg.error():
                print(f"[analytics-consumer] message error: {msg.error()}", flush=True)
                continue
            try:
                _apply_event(json.loads(msg.value()))
            except Exception as e:
                print(f"[analytics-consumer] failed to apply event: {e!r}", flush=True)
                continue
            processed += 1
            if processed % 5000 == 0:
                print(f"[analytics-consumer] processed {processed} events so far", flush=True)
    finally:
        print(f"[analytics-consumer] consumer thread exiting after {processed} events", flush=True)
        consumer.close()


def start_consumer() -> None:
    thread = threading.Thread(target=consume_forever, daemon=True)
    thread.start()
