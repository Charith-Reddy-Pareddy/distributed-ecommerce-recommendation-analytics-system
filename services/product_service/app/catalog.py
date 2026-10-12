from typing import Iterable, Protocol

from bson.decimal128 import Decimal128
from pymongo import AsyncMongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError

from services.product_service.app.schemas import ProductCreate, ProductOut


class ProductRepository(Protocol):
    async def create(self, product: ProductCreate) -> ProductOut: ...

    async def get(self, product_id: int) -> ProductOut | None: ...

    async def list(self, skip: int, limit: int) -> list[ProductOut]: ...

    async def count(self) -> int: ...


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

    async def upsert_catalog(
        self, products: Iterable[tuple[int, ProductCreate]]
    ) -> list[ProductOut]:
        records = list(products)
        product_ids = [product_id for product_id, _ in records]
        asins = [product.asin for _, product in records]
        if any(type(product_id) is not int or product_id <= 0 for product_id in product_ids):
            raise ValueError("Catalog product IDs must be positive integers")
        if len(set(product_ids)) != len(product_ids):
            raise ValueError("Catalog product IDs must be unique")
        if any(not asin for asin in asins):
            raise ValueError("Catalog products must have an ASIN")
        if len(set(asins)) != len(asins):
            raise ValueError("Catalog ASINs must be unique")

        await self._products.create_index(
            "asin",
            unique=True,
            partialFilterExpression={"asin": {"$type": "string"}},
        )
        saved = []
        for product_id, product in records:
            record = product.model_dump(mode="json")
            record["price"] = Decimal128(str(product.price))
            document = {"_id": product_id, **record}
            existing = await self._products.find_one({"asin": product.asin})
            if existing is not None:
                if existing["_id"] != product_id:
                    raise ValueError(
                        f"MongoDB ID for ASIN {product.asin} does not match the catalog map"
                    )
                await self._products.update_one({"_id": product_id}, {"$set": record})
            else:
                try:
                    await self._products.insert_one(document)
                except DuplicateKeyError as exc:
                    existing = await self._products.find_one({"asin": product.asin})
                    if existing is None or existing["_id"] != product_id:
                        raise ValueError(
                            f"MongoDB ID {product_id} conflicts with an existing product"
                        ) from exc
                    await self._products.update_one(
                        {"_id": product_id}, {"$set": record}
                    )
            saved.append(self._to_product(document))

        if product_ids:
            await self._counters.find_one_and_update(
                {"_id": "product_id"},
                {"$max": {"seq": max(product_ids)}},
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
        return saved

    async def get(self, product_id: int) -> ProductOut | None:
        document = await self._products.find_one({"_id": product_id})
        return self._to_product(document) if document else None

    async def list(self, skip: int = 0, limit: int = 100) -> list[ProductOut]:
        cursor = self._products.find().sort("_id", 1).skip(skip).limit(limit)
        documents = await cursor.to_list(length=limit)
        return [self._to_product(document) for document in documents]

    async def count(self) -> int:
        return await self._products.count_documents({})

    @staticmethod
    def _to_product(document: dict) -> ProductOut:
        record = document.copy()
        record["id"] = record.pop("_id")
        record["price"] = record["price"].to_decimal()
        return ProductOut.model_validate(record)
