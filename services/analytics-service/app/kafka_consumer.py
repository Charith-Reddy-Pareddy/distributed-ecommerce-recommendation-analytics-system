import os
import uuid

from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = "events"


def new_consumer() -> Consumer:
    """A fresh, unique consumer group every process start replays the
    whole topic from the beginning (no committed offsets to resume
    from), so this service's Postgres aggregates are always fully
    rebuildable from the Kafka log.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": f"analytics-service-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])
    return consumer
