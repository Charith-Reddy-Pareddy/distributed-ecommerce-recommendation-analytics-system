import asyncio
import json
import os
import socket
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import pytest
from elasticsearch import AsyncElasticsearch
from fastapi.testclient import TestClient
from pymongo import AsyncMongoClient

from services.product_service.app.amazon_catalog import product_from_amazon
from services.product_service.app.main import app


ROOT = Path(__file__).resolve().parents[1]
BACKUP_CATALOG = (
    ROOT.parent
    / "distributed-ecommerce-recommendation-analytics-system"
    / "data"
    / "amazon_products.json"
)


def _missing_services() -> None:
    message = "Start MongoDB and Elasticsearch with Docker Compose first"
    required = os.environ.get("REQUIRE_INTEGRATION", "").lower()
    if required in {"1", "true", "yes"}:
        pytest.fail(message)
    pytest.skip(message)


def _port_open(url: str, default_port: int) -> bool:
    parsed = urlparse(url)
    host = parsed.hostname or "localhost"
    port = parsed.port or default_port
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def _real_product() -> dict:
    configured = os.environ.get("AMAZON_PRODUCTS_PATH")
    paths = [Path(configured)] if configured else [
        ROOT / "data" / "amazon_products.json",
        BACKUP_CATALOG,
    ]
    path = next((item for item in paths if item.is_file()), None)
    if path is None:
        message = "Real Amazon catalog not found; set AMAZON_PRODUCTS_PATH"
        if os.environ.get("REQUIRE_INTEGRATION", "").lower() in {"1", "true", "yes"}:
            pytest.fail(message)
        pytest.skip(message)

    rows = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(rows, list) or not rows:
        pytest.fail(f"Real Amazon catalog is empty or invalid: {path}")
    return product_from_amazon(rows[0]).model_dump(mode="json")


def test_missing_services_skip_by_default(monkeypatch) -> None:
    monkeypatch.delenv("REQUIRE_INTEGRATION", raising=False)

    with pytest.raises(pytest.skip.Exception, match="MongoDB and Elasticsearch"):
        _missing_services()


def test_missing_services_fail_when_required(monkeypatch) -> None:
    monkeypatch.setenv("REQUIRE_INTEGRATION", "1")

    with pytest.raises(pytest.fail.Exception, match="MongoDB and Elasticsearch"):
        _missing_services()


@pytest.mark.integration
def test_product_write_is_searchable_with_local_services(monkeypatch) -> None:
    mongo_url = os.environ.get("MONGODB_URL", "mongodb://localhost:27017")
    elasticsearch_url = os.environ.get("ELASTICSEARCH_URL", "http://localhost:9200")
    if not _port_open(mongo_url, 27017) or not _port_open(elasticsearch_url, 9200):
        _missing_services()

    product = _real_product()
    suffix = uuid4().hex[:12]
    database = f"product_test_{suffix}"
    index = f"products_test_{suffix}"
    monkeypatch.setenv("MONGODB_DATABASE", database)
    monkeypatch.setenv("ELASTICSEARCH_INDEX", index)

    try:
        with TestClient(app) as client:
            response = client.post(
                "/products",
                json=product,
            )
            assert response.status_code == 201
            product_id = response.json()["id"]
            assert response.json()["asin"] == product["asin"]
            assert response.json()["brand"] == product["brand"]

            saved = client.get(f"/products/{product_id}")
            assert saved.status_code == 200
            assert saved.json()["asin"] == product["asin"]
            assert saved.json()["price"] == product["price"]

            found = client.get("/products/search", params={"q": product["name"]})
            assert found.status_code == 200
            assert [item["id"] for item in found.json()] == [product_id]
    finally:
        async def cleanup() -> None:
            mongo = AsyncMongoClient(mongo_url, serverSelectionTimeoutMS=3000)
            elasticsearch = AsyncElasticsearch(elasticsearch_url, request_timeout=3)
            try:
                await mongo.drop_database(database)
                await elasticsearch.indices.delete(index=index, ignore_unavailable=True)
            finally:
                await elasticsearch.close()
                await mongo.close()

        asyncio.run(cleanup())
