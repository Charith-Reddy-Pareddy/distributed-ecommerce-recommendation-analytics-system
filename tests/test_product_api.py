from collections.abc import Iterator
import pytest
from fastapi.testclient import TestClient

from services.product_service.app.main import app, get_product_index, get_product_repository
from services.product_service.app.schemas import ProductCreate, ProductOut


class MemoryIndex:
    def __init__(self) -> None:
        self.products: dict[int, ProductOut] = {}
        self.search_call: tuple[str, int, int] | None = None

    async def index_product(self, product: ProductOut) -> None:
        self.products[product.id] = product

    async def search(self, query: str, skip: int, limit: int) -> list[ProductOut]:
        self.search_call = (query, skip, limit)
        return list(self.products.values())[skip : skip + limit]


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
def catalog_api() -> Iterator[tuple[TestClient, MemoryCatalog, MemoryIndex]]:
    catalog = MemoryCatalog()
    product_index = MemoryIndex()
    app.dependency_overrides[get_product_repository] = lambda: catalog
    app.dependency_overrides[get_product_index] = lambda: product_index
    with TestClient(app) as client:
        yield client, catalog, product_index
    app.dependency_overrides.clear()


def test_product_routes_create_get_and_list(catalog_api) -> None:
    client, catalog, product_index = catalog_api
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
    assert product_index.products[1].name == "Desk lamp"

    assert client.get("/products/1").json()["name"] == "Desk lamp"
    assert client.get("/products?skip=0&limit=1").json()[0]["id"] == 1


def test_product_routes_return_not_found(catalog_api) -> None:
    client, _, _ = catalog_api

    response = client.get("/products/99")

    assert response.status_code == 404
    assert response.json()["detail"] == "Product not found"


def test_product_routes_validate_input_and_pagination(catalog_api) -> None:
    client, catalog, _ = catalog_api
    bad_product = client.post(
        "/products",
        json={"name": "Desk lamp", "category": "Lighting", "price": "-1"},
    )

    assert bad_product.status_code == 422
    assert client.get("/products?skip=-1").status_code == 422
    assert client.get("/products?limit=0").status_code == 422
    assert catalog.products == {}


def test_product_search_uses_query_and_pagination(catalog_api) -> None:
    client, _, product_index = catalog_api
    product_index.products[1] = ProductOut(
        id=1, name="Desk lamp", category="Lighting", price="24.50"
    )

    response = client.get("/products/search?q=lamp&skip=0&limit=5")

    assert response.status_code == 200
    assert response.json()[0]["id"] == 1
    assert product_index.search_call == ("lamp", 0, 5)


def test_product_search_rejects_empty_query(catalog_api) -> None:
    client, _, _ = catalog_api

    assert client.get("/products/search?q=").status_code == 422
