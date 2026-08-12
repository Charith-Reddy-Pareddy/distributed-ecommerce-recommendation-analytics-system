import pytest
from pydantic import ValidationError

from conftest import load_app_module

schemas = load_app_module("event-service", "schemas", "eventsvc_app")
EventCreate = schemas.EventCreate


@pytest.mark.parametrize("event_type", ["view", "add_to_cart", "purchase"])
def test_valid_event_types_are_accepted(event_type):
    event = EventCreate(user_id=1, product_id=2, event_type=event_type)
    assert event.event_type == event_type


def test_unknown_event_type_is_rejected():
    with pytest.raises(ValidationError):
        EventCreate(user_id=1, product_id=2, event_type="click")


def test_missing_user_id_is_rejected():
    with pytest.raises(ValidationError):
        EventCreate(product_id=2, event_type="view")


def test_missing_product_id_is_rejected():
    with pytest.raises(ValidationError):
        EventCreate(user_id=1, event_type="view")


def test_non_integer_user_id_is_rejected():
    with pytest.raises(ValidationError):
        EventCreate(user_id="not-an-id", product_id=2, event_type="view")


def test_numeric_string_ids_are_coerced_to_int():
    # Pydantic coerces numeric strings for int fields by default --
    # documenting that here since it's easy to assume it's strict.
    event = EventCreate(user_id="1", product_id="2", event_type="view")
    assert event.user_id == 1
    assert event.product_id == 2
