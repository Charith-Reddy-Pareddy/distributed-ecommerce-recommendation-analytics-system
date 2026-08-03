import os

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ReturnDocument

MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://localhost:27017")

client = AsyncIOMotorClient(MONGODB_URL)
db = client["product_catalog"]
products_collection = db["products"]
counters_collection = db["counters"]


async def next_product_id() -> int:
    """MongoDB has no native auto-increment -- the standard workaround
    is an atomic $inc against a dedicated counters document, which is
    what this does.
    """
    result = await counters_collection.find_one_and_update(
        {"_id": "product_id"},
        {"$inc": {"seq": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    return result["seq"]
