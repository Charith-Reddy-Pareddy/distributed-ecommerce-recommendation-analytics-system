"""Load the local Amazon catalog into MongoDB."""

import asyncio
import os

from pymongo import AsyncMongoClient

from services.product_service.app.amazon_catalog import load_mapped_catalog
from services.product_service.app.catalog import MongoProductRepository


async def import_catalog() -> int:
    client = AsyncMongoClient(
        os.environ.get("MONGODB_URL", "mongodb://localhost:27017"),
        serverSelectionTimeoutMS=5000,
    )
    try:
        products = load_mapped_catalog()
        repo = MongoProductRepository(
            client, os.environ.get("MONGODB_DATABASE", "product_catalog")
        )
        saved = await repo.upsert_catalog(products)
        return len(saved)
    finally:
        await client.close()


if __name__ == "__main__":
    count = asyncio.run(import_catalog())
    print(f"Loaded {count} products into MongoDB")
