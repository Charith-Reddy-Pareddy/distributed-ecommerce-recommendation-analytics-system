from asyncio import run
from decimal import Decimal
import json
import os
from pathlib import Path

import pytest

from services.product_service.app.schemas import ProductOut
from services.product_service.app.search import ElasticsearchProductIndex


class FakeElasticsearch:
    def __init__(self, hits: list[dict] | None = None) -> None:
        self.hits = hits or []
        self.index_call: dict | None = None
        self.search_call: dict | None = None
        self.bulk_calls: list[dict] = []
        self.indices = FakeIndices()

    async def index(self, **kwargs) -> None:
        self.index_call = kwargs

    async def search(self, **kwargs) -> dict:
        self.search_call = kwargs
        return {"hits": {"hits": [{"_source": hit} for hit in self.hits]}}

    async def bulk(self, **kwargs) -> dict:
        self.bulk_calls.append(kwargs)
        return {"errors": False}


class FakeIndices:
    def __init__(self) -> None:
        self.refresh_call: dict | None = None

    async def refresh(self, **kwargs) -> None:
        self.refresh_call = kwargs


def test_index_product_uses_product_id_and_keeps_price() -> None:
    client = FakeElasticsearch()
    index = ElasticsearchProductIndex(client)
    product = ProductOut(id=7, name="Desk lamp", category="Lighting", price=Decimal("24.50"))

    run(index.index_product(product))

    assert client.index_call["index"] == "products"
    assert client.index_call["id"] == "7"
    assert client.index_call["document"]["price"] == "24.50"
    assert client.index_call["refresh"] == "wait_for"


def test_search_maps_hits_to_product_models() -> None:
    client = FakeElasticsearch(
        [{"id": 7, "name": "Desk lamp", "category": "Lighting", "price": "24.50"}]
    )
    index = ElasticsearchProductIndex(client, "catalog")

    products = run(index.search("lamp", skip=10, limit=5))

    assert products == [
        ProductOut(id=7, name="Desk lamp", category="Lighting", price=Decimal("24.50"))
    ]
    assert client.search_call["index"] == "catalog"
    assert client.search_call["from_"] == 10
    assert client.search_call["size"] == 5
    assert client.search_call["query"]["multi_match"]["query"] == "lamp"


def test_index_catalog_bulk_indexes_and_refreshes() -> None:
    cat_path = Path(os.getenv("AMAZON_CATALOG_PATH", "data/amazon_products.json"))
    map_path = Path(os.getenv("AMAZON_CATALOG_MAP_PATH", "data/asin_product_id_map.json"))
    if not cat_path.is_file() or not map_path.is_file():
        pytest.skip("local Amazon catalog and ID map are required")

    catalog = json.loads(cat_path.read_text())
    id_map = json.loads(map_path.read_text())
    rows = [
        ProductOut(
            id=id_map[row["asin"]],
            asin=row["asin"],
            name=row["name"],
            category=row["category"],
            price=Decimal(str(row.get("price", 0))),
            brand=row.get("brand"),
            description=row.get("description", ""),
        )
        for row in catalog
        if row.get("asin") in id_map and row.get("name") and row.get("category")
    ][:2]
    assert len(rows) == 2, "the real catalog must contain at least two mapped products"

    client = FakeElasticsearch()
    index = ElasticsearchProductIndex(client, "catalog")
    count = run(index.index_catalog(rows))

    assert count == len(rows)
    assert client.bulk_calls[0]["operations"][0] == {
        "index": {"_index": "catalog", "_id": rows[0].asin}
    }
    assert client.bulk_calls[0]["operations"][1] == rows[0].model_dump(mode="json")
    assert client.indices.refresh_call == {"index": "catalog"}
