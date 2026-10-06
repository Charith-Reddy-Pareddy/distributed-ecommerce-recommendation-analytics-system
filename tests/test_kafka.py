import json

import pytest

from ecommerce_analytics.kafka import KafkaEventPublisher, KafkaPublishError
from ecommerce_analytics.schemas import EventCreate


class StubProducer:
    def __init__(self, error: str | None = None, pending: int = 0) -> None:
        self.error = error
        self.pending = pending
        self.message: dict[str, object] | None = None
        self.callback = None

    def produce(self, topic: str, **kwargs: object) -> None:
        self.message = {"topic": topic, **kwargs}
        self.callback = kwargs["on_delivery"]

    def poll(self, timeout: float) -> int:
        if self.callback:
            self.callback(self.error, self.message)
        return 1

    def flush(self, timeout: float) -> int:
        return self.pending


def test_publisher_keys_and_serializes_event() -> None:
    producer = StubProducer()
    publisher = KafkaEventPublisher("broker:9092", "events", producer)
    event = EventCreate(user_id=7, product_id=12, event_type="view")

    record = publisher.publish(event)

    assert producer.message is not None
    assert producer.message["topic"] == "events"
    assert producer.message["key"] == "7"
    payload = json.loads(producer.message["value"])
    assert payload["id"] == str(record.id)
    assert payload["product_id"] == 12
    assert record.user_id == 7


def test_publisher_logs_delivery_error(caplog) -> None:
    publisher = KafkaEventPublisher("broker:9092", "events", StubProducer("offline"))

    publisher.publish(EventCreate(user_id=7, product_id=12, event_type="view"))

    assert "Kafka delivery failed: offline" in caplog.text


def test_publisher_flush_logs_messages_left_pending(caplog) -> None:
    publisher = KafkaEventPublisher("broker:9092", "events", StubProducer(pending=2))

    publisher.flush()

    assert "left 2 event(s) undelivered" in caplog.text


def test_publisher_reports_full_queue() -> None:
    class FullQueueProducer(StubProducer):
        def produce(self, topic: str, **kwargs: object) -> None:
            raise BufferError("queue full")

    publisher = KafkaEventPublisher("broker:9092", "events", FullQueueProducer())

    with pytest.raises(KafkaPublishError, match="Kafka producer queue is full"):
        publisher.publish(EventCreate(user_id=7, product_id=12, event_type="view"))
