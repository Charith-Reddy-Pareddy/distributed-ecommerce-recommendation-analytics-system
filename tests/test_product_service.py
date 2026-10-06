from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from services.product_service.app.main import app
from services.product_service.app.schemas import GeoPoint, ProductCreate, ProductOut


def test_product_service_health() -> None:
    response = TestClient(app).get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "product-service"}


def test_product_schema_keeps_catalog_fields_and_defaults() -> None:
    product = ProductCreate(
        name="  Desk lamp ", category=" lighting ", price="24.50", asin=" B0123 "
    )

    assert product.name == "Desk lamp"
    assert product.category == "lighting"
    assert product.price == Decimal("24.50")
    assert product.currency == "USD"
    assert product.asin == "B0123"
    assert product.tags == []
    assert product.specifications == {}
    assert product.rating.average == 0


def test_product_schema_accepts_nested_catalog_data() -> None:
    product = ProductOut(
        id=1,
        name="Desk lamp",
        category="Lighting",
        price="24.50",
        tags=["desk", "led"],
        specifications={"color": "black"},
        images=["https://example.test/lamp.jpg"],
        location={"type": "Point", "coordinates": [-89.4, 43.1]},
        rating={"average": 4.5, "count": 12},
    )

    assert product.location == GeoPoint(coordinates=(-89.4, 43.1))
    assert product.rating.count == 12


def test_product_schema_rejects_invalid_coordinates_and_ratings() -> None:
    with pytest.raises(ValidationError):
        GeoPoint(coordinates=[181, 43])

    with pytest.raises(ValidationError):
        ProductCreate(
            name="Desk lamp",
            category="Lighting",
            price="24.50",
            rating={"average": 5.5},
        )


def test_product_schema_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        ProductCreate(
            name="Desk lamp", category="Lighting", price="24.50", unknown="value"
        )
