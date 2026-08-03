import os
import uuid

from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = "events"


def new_consumer() -> Consumer:
    """A fresh, unique consumer group every process start means this
    service always replays the full topic from the beginning on boot
    (no committed offsets to resume from), then keeps tailing new
    messages in the same poll loop. That keeps the service stateless
    from a deployment standpoint -- restart it and it reconstructs its
    in-memory model from the Kafka log.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": f"recommendation-service-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])
    return consumer
