from typing import Protocol

from bson.decimal128 import Decimal128
from pymongo import AsyncMongoClient, ReturnDocument

from services.product_service.app.schemas import ProductCreate, ProductOut


class ProductRepository(Protocol):
    async def create(self, product: ProductCreate) -> ProductOut: ...

    async def get(self, product_id: int) -> ProductOut | None: ...

    async def list(self, skip: int, limit: int) -> list[ProductOut]: ...


class MongoProductRepository:
    def __init__(self, client: AsyncMongoClient, database: str = "product_catalog") -> None:
        db = client[database]
        self._products = db["products"]
        self._counters = db["counters"]

    async def create(self, product: ProductCreate) -> ProductOut:
        counter = await self._counters.find_one_and_update(
            {"_id": "product_id"},
            {"$inc": {"seq": 1}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        record = product.model_dump(mode="json")
        record["price"] = Decimal128(str(product.price))
        document = {"_id": counter["seq"], **record}
        await self._products.insert_one(document)
        return self._to_product(document)

    async def get(self, product_id: int) -> ProductOut | None:
        document = await self._products.find_one({"_id": product_id})
        return self._to_product(document) if document else None

    async def list(self, skip: int = 0, limit: int = 100) -> list[ProductOut]:
        cursor = self._products.find().sort("_id", 1).skip(skip).limit(limit)
        documents = await cursor.to_list(length=limit)
        return [self._to_product(document) for document in documents]

    @staticmethod
    def _to_product(document: dict) -> ProductOut:
        record = document.copy()
        record["id"] = record.pop("_id")
        record["price"] = record["price"].to_decimal()
        return ProductOut.model_validate(record)
