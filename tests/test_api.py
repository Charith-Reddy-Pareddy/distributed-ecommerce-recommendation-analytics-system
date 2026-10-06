from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from ecommerce_analytics.kafka import EventPublisher, KafkaPublishError, get_event_publisher
from ecommerce_analytics.main import app
from ecommerce_analytics.schemas import EventCreate, EventRecord


class StubPublisher:
    def __init__(self, full: bool = False) -> None:
        self.events: list[EventCreate] = []
        self.full = full

    def publish(self, event: EventCreate) -> EventRecord:
        if self.full:
            raise KafkaPublishError("Kafka producer queue is full")
        self.events.append(event)
        return EventRecord(
            id=UUID("12345678-1234-5678-1234-567812345678"),
            user_id=event.user_id,
            product_id=event.product_id,
            event_type=event.event_type,
            occurred_at=event.occurred_at,
        )

    def flush(self) -> None:
        pass


@pytest.fixture
def api():
    publisher = StubPublisher()
    app.dependency_overrides[get_event_publisher] = lambda: publisher
    with TestClient(app) as client:
        yield client, publisher
    app.dependency_overrides.clear()


def test_event_endpoint_sends_valid_event(api) -> None:
    client, publisher = api
    response = client.post(
        "/events",
        json={"user_id": 7, "product_id": 12, "event_type": "purchase"},
    )

    assert response.status_code == 202
    assert response.json()["id"] == "12345678-1234-5678-1234-567812345678"
    assert response.json()["user_id"] == 7
    assert response.json()["product_id"] == 12
    assert response.json()["event_type"] == "purchase"
    assert len(publisher.events) == 1


def test_event_endpoint_rejects_invalid_input(api) -> None:
    client, publisher = api
    response = client.post(
        "/events",
        json={"user_id": 0, "product_id": 12, "event_type": "purchase"},
    )

    assert response.status_code == 422
    assert publisher.events == []


def test_event_endpoint_reports_full_kafka_queue() -> None:
    publisher = StubPublisher(full=True)
    app.dependency_overrides[get_event_publisher] = lambda: publisher
    try:
        response = TestClient(app).post(
            "/events",
            json={"user_id": 7, "product_id": 12, "event_type": "view"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 503
    assert response.json()["detail"] == "Kafka producer is full"
