import json
import logging
import os
from functools import lru_cache
from typing import Protocol
from uuid import uuid4

from ecommerce_analytics.schemas import EventCreate, EventRecord


logger = logging.getLogger(__name__)


class KafkaPublishError(RuntimeError):
    pass


class ProducerClient(Protocol):
    def produce(self, topic: str, **kwargs: object) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


class EventPublisher(Protocol):
    def publish(self, event: EventCreate) -> EventRecord: ...

    def flush(self) -> None: ...


class KafkaEventPublisher:
    def __init__(
        self,
        bootstrap_servers: str,
        topic: str = "events",
        producer: ProducerClient | None = None,
    ) -> None:
        if producer is None:
            from confluent_kafka import Producer

            producer = Producer({"bootstrap.servers": bootstrap_servers})
        self._producer = producer
        self._topic = topic

    def publish(self, event: EventCreate) -> EventRecord:
        record = EventRecord(
            id=uuid4(),
            user_id=event.user_id,
            product_id=event.product_id,
            event_type=event.event_type,
            occurred_at=event.occurred_at,
        )

        def on_delivery(error: object, _message: object) -> None:
            if error is not None:
                logger.error("Kafka delivery failed: %s", error)

        try:
            self._producer.produce(
                self._topic,
                key=str(event.user_id),
                value=record.model_dump_json().encode("utf-8"),
                on_delivery=on_delivery,
            )
        except BufferError as exc:
            raise KafkaPublishError("Kafka producer queue is full") from exc

        self._producer.poll(0)
        return record

    def flush(self) -> None:
        pending = self._producer.flush(10.0)
        if pending:
            logger.error("Kafka shutdown left %s event(s) undelivered", pending)


@lru_cache(maxsize=1)
def get_event_publisher() -> EventPublisher:
    return KafkaEventPublisher(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"),
        topic=os.environ.get("KAFKA_EVENTS_TOPIC", "events"),
    )


def flush_event_publisher() -> None:
    if get_event_publisher.cache_info().currsize:
        get_event_publisher().flush()
