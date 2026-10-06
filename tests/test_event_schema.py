from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from ecommerce_analytics.schemas import EventCreate, EventType


@pytest.mark.parametrize("kind", list(EventType))
def test_event_input_accepts_supported_types(kind: EventType) -> None:
    evt = EventCreate(user_id=1, product_id=2, event_type=kind)

    assert evt.event_type is kind
    assert evt.occurred_at.tzinfo is not None


def test_event_input_accepts_timezone_aware_timestamp() -> None:
    ts = datetime(2026, 1, 2, 3, 4, tzinfo=timezone.utc)
    evt = EventCreate(
        user_id=1,
        product_id=2,
        event_type="purchase",
        occurred_at=ts,
    )

    assert evt.occurred_at == ts


@pytest.mark.parametrize(
    "data",
    [
        {"user_id": 0, "product_id": 2, "event_type": "view"},
        {"user_id": 1, "product_id": -1, "event_type": "view"},
        {"user_id": 1, "product_id": 2, "event_type": "click"},
        {
            "user_id": 1,
            "product_id": 2,
            "event_type": "view",
            "occurred_at": "2026-01-02T03:04:00",
        },
        {"user_id": 1, "product_id": 2, "event_type": "view", "extra": "value"},
    ],
)
def test_event_input_rejects_invalid_values(data: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        EventCreate.model_validate(data)
