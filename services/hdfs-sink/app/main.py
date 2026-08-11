"""Batches the Kafka `events` topic into HDFS as the raw, append-only
archive the batch layer (MapReduce, Spark MLlib) reads from. A
standalone worker, not a FastAPI service -- no HTTP surface, just the
consume-batch-write loop.

Unlike recommendation-service/analytics-service (fresh consumer group
every restart, replaying full history into memory/SQL), this sink uses
one *persistent* group with committed offsets -- replaying from scratch
would re-write batches already safely in HDFS. Offsets commit only
after a batch is durably written, giving at-least-once delivery.
"""
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Consumer
from hdfs import InsecureClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("hdfs-sink")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
WEBHDFS_URL = os.getenv("WEBHDFS_URL", "http://localhost:9870")
TOPIC = "events"
CONSUMER_GROUP = "hdfs-sink"

BATCH_MAX_SIZE = int(os.getenv("BATCH_MAX_SIZE", "500"))
BATCH_MAX_SECONDS = float(os.getenv("BATCH_MAX_SECONDS", "30"))


def _event_day(event: dict) -> str:
    created_at = event.get("created_at")
    if created_at:
        return datetime.fromisoformat(created_at).date().isoformat()
    return datetime.now(timezone.utc).date().isoformat()


def _flush_batch(hdfs_client: InsecureClient, batch: list[dict]) -> None:
    if not batch:
        return

    by_day: dict[str, list[dict]] = {}
    for event in batch:
        by_day.setdefault(_event_day(event), []).append(event)

    for day, events in by_day.items():
        directory = f"/events/dt={day}"
        filename = f"{directory}/batch-{uuid.uuid4()}.jsonl"
        body = "\n".join(json.dumps(event) for event in events) + "\n"
        hdfs_client.makedirs(directory)
        hdfs_client.write(filename, data=body, encoding="utf-8")
        log.info("Wrote %d events to %s", len(events), filename)


def run() -> None:
    hdfs_client = InsecureClient(WEBHDFS_URL, user="root")

    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    batch: list[dict] = []
    batch_started_at = time.monotonic()

    log.info("hdfs-sink starting, batch_max_size=%d batch_max_seconds=%.0f",
              BATCH_MAX_SIZE, BATCH_MAX_SECONDS)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is not None and not msg.error():
                batch.append(json.loads(msg.value()))

            elapsed = time.monotonic() - batch_started_at
            if batch and (len(batch) >= BATCH_MAX_SIZE or elapsed >= BATCH_MAX_SECONDS):
                _flush_batch(hdfs_client, batch)
                consumer.commit(asynchronous=False)
                batch = []
                batch_started_at = time.monotonic()
    finally:
        _flush_batch(hdfs_client, batch)
        if batch:
            consumer.commit(asynchronous=False)
        consumer.close()


if __name__ == "__main__":
    run()
