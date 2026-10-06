import asyncio
import os
import socket
from urllib.parse import urlparse
from uuid import uuid4

import pytest
from elasticsearch import AsyncElasticsearch
from fastapi.testclient import TestClient
from pymongo import AsyncMongoClient

from services.product_service.app.main import app


pytestmark = pytest.mark.integration


def _port_open(url: str, default_port: int) -> bool:
    parsed = urlparse(url)
    host = parsed.hostname or "localhost"
    port = parsed.port or default_port
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def test_product_write_is_searchable_with_local_services(monkeypatch) -> None:
    mongo_url = os.environ.get("MONGODB_URL", "mongodb://localhost:27017")
    elasticsearch_url = os.environ.get("ELASTICSEARCH_URL", "http://localhost:9200")
    if not _port_open(mongo_url, 27017) or not _port_open(elasticsearch_url, 9200):
        pytest.skip("Start MongoDB and Elasticsearch with Docker Compose first")

    suffix = uuid4().hex[:12]
    database = f"product_test_{suffix}"
    index = f"products_test_{suffix}"
    monkeypatch.setenv("MONGODB_DATABASE", database)
    monkeypatch.setenv("ELASTICSEARCH_INDEX", index)

    try:
        with TestClient(app) as client:
            response = client.post(
                "/products",
                json={
                    "name": f"lamp-{suffix}",
                    "category": "Lighting",
                    "price": "24.50",
                },
            )
            assert response.status_code == 201
            product_id = response.json()["id"]

            saved = client.get(f"/products/{product_id}")
            assert saved.status_code == 200
            assert saved.json()["price"] == "24.50"

            found = client.get("/products/search", params={"q": f"lamp-{suffix}"})
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
