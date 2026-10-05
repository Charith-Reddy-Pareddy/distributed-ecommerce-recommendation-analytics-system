from decimal import Decimal

import pytest
from pydantic import ValidationError

from ecommerce_analytics.schemas import ProductCreate


def test_product_input_strips_text_and_uses_usd_by_default() -> None:
    product = ProductCreate(name="  Desk lamp ", category=" lighting ", price="24.50")

    assert product.name == "Desk lamp"
    assert product.category == "lighting"
    assert product.price == Decimal("24.50")
    assert product.currency == "USD"


def test_product_input_accepts_zero_price() -> None:
    product = ProductCreate(name="Sample", category="Other", price="0")

    assert product.price == Decimal("0")


@pytest.mark.parametrize(
    "payload",
    [
        {"name": "  ", "category": "Lighting", "price": "24.50"},
        {"name": "Desk lamp", "category": " ", "price": "24.50"},
        {"name": "Desk lamp", "category": "Lighting", "price": "-0.01"},
        {"name": "Desk lamp", "category": "Lighting", "price": "1.999"},
        {"name": "Desk lamp", "category": "Lighting", "price": "24.50", "currency": "usd"},
        {"name": "Desk lamp", "category": "Lighting", "price": "24.50", "unknown": True},
    ],
)
def test_product_input_rejects_invalid_values(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        ProductCreate.model_validate(payload)
