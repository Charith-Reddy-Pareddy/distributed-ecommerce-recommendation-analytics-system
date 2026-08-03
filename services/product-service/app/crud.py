from . import schemas
from .database import next_product_id, products_collection


def _to_out(doc: dict) -> dict:
    doc["id"] = doc.pop("_id")
    return doc


async def create_product(product: schemas.ProductCreate) -> dict:
    product_id = await next_product_id()
    doc = {"_id": product_id, **product.model_dump()}
    await products_collection.insert_one(doc)
    return _to_out(doc)


async def get_product(product_id: int) -> dict | None:
    doc = await products_collection.find_one({"_id": product_id})
    return _to_out(doc) if doc else None


async def list_products(skip: int = 0, limit: int = 100) -> list[dict]:
    cursor = products_collection.find().sort("_id", 1).skip(skip).limit(limit)
    return [_to_out(doc) async for doc in cursor]


async def get_products_by_ids(product_ids: list[int]) -> list[dict]:
    cursor = products_collection.find({"_id": {"$in": product_ids}})
    return [_to_out(doc) async for doc in cursor]
