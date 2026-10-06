from collections.abc import Iterator
import pytest
from fastapi.testclient import TestClient

from services.product_service.app.main import app, get_product_repository
from services.product_service.app.schemas import ProductCreate, ProductOut


class MemoryCatalog:
    def __init__(self) -> None:
        self.products: dict[int, ProductOut] = {}

    async def create(self, product: ProductCreate) -> ProductOut:
        saved = ProductOut(id=len(self.products) + 1, **product.model_dump())
        self.products[saved.id] = saved
        return saved

    async def get(self, product_id: int) -> ProductOut | None:
        return self.products.get(product_id)

    async def list(self, skip: int, limit: int) -> list[ProductOut]:
        items = sorted(self.products.values(), key=lambda product: product.id)
        return items[skip : skip + limit]


@pytest.fixture
def catalog_api() -> Iterator[tuple[TestClient, MemoryCatalog]]:
    catalog = MemoryCatalog()
    app.dependency_overrides[get_product_repository] = lambda: catalog
    with TestClient(app) as client:
        yield client, catalog
    app.dependency_overrides.clear()


def test_product_routes_create_get_and_list(catalog_api) -> None:
    client, catalog = catalog_api
    response = client.post(
        "/products",
        json={
            "name": "Desk lamp",
            "category": "Lighting",
            "price": "24.50",
            "tags": ["desk", "led"],
        },
    )

    assert response.status_code == 201
    assert response.json()["id"] == 1
    assert response.json()["price"] == "24.50"
    assert response.json()["tags"] == ["desk", "led"]
    assert len(catalog.products) == 1

    assert client.get("/products/1").json()["name"] == "Desk lamp"
    assert client.get("/products?skip=0&limit=1").json()[0]["id"] == 1


def test_product_routes_return_not_found(catalog_api) -> None:
    client, _ = catalog_api

    response = client.get("/products/99")

    assert response.status_code == 404
    assert response.json()["detail"] == "Product not found"


def test_product_routes_validate_input_and_pagination(catalog_api) -> None:
    client, catalog = catalog_api
    bad_product = client.post(
        "/products",
        json={"name": "Desk lamp", "category": "Lighting", "price": "-1"},
    )

    assert bad_product.status_code == 422
    assert client.get("/products?skip=-1").status_code == 422
    assert client.get("/products?limit=0").status_code == 422
    assert catalog.products == {}
