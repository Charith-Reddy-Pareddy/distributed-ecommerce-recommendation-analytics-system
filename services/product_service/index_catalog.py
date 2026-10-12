"""Index the MongoDB product catalog in Elasticsearch."""

import asyncio
import os

from elasticsearch import AsyncElasticsearch
from pymongo import AsyncMongoClient

from services.product_service.app.catalog import MongoProductRepository
from services.product_service.app.search import ElasticsearchProductIndex

PAGE_SIZE = 500


async def index_catalog() -> int:
    mongo = AsyncMongoClient(
        os.environ.get("MONGODB_URL", "mongodb://localhost:27017"),
        serverSelectionTimeoutMS=5000,
    )
    elasticsearch = AsyncElasticsearch(
        os.environ.get("ELASTICSEARCH_URL", "http://localhost:9200"),
        request_timeout=30,
    )
    try:
        repo = MongoProductRepository(
            mongo, os.environ.get("MONGODB_DATABASE", "product_catalog")
        )
        products = []
        skip = 0
        while page := await repo.list(skip=skip, limit=PAGE_SIZE):
            products.extend(page)
            skip += len(page)

        index = ElasticsearchProductIndex(
            elasticsearch, os.environ.get("ELASTICSEARCH_INDEX", "products")
        )
        return await index.index_catalog(products)
    finally:
        await elasticsearch.close()
        await mongo.close()


if __name__ == "__main__":
    count = asyncio.run(index_catalog())
    print(f"Indexed {count} products in Elasticsearch")
