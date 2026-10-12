from typing import Protocol

from elasticsearch import AsyncElasticsearch

from services.product_service.app.schemas import ProductOut

BULK_SIZE = 500


class ProductIndex(Protocol):
    async def index_product(self, product: ProductOut) -> None: ...

    async def search(self, query: str, skip: int, limit: int) -> list[ProductOut]: ...


class ElasticsearchProductIndex:
    def __init__(self, client: AsyncElasticsearch, index: str = "products") -> None:
        self._client = client
        self._index = index

    async def index_product(self, product: ProductOut) -> None:
        await self._client.index(
            index=self._index,
            id=str(product.asin or product.id),
            document=product.model_dump(mode="json"),
            refresh="wait_for",
        )

    async def index_catalog(self, products: list[ProductOut]) -> int:
        if not products:
            return 0

        for start in range(0, len(products), BULK_SIZE):
            batch = products[start : start + BULK_SIZE]
            operations = []
            for product in batch:
                if not product.asin:
                    raise ValueError("Catalog products must have an ASIN")
                operations.extend(
                    [
                        {"index": {"_index": self._index, "_id": product.asin}},
                        product.model_dump(mode="json"),
                    ]
                )

            response = await self._client.bulk(operations=operations)
            if response.get("errors"):
                raise RuntimeError("Elasticsearch failed to index part of the catalog")

        await self._client.indices.refresh(index=self._index)
        return len(products)

    async def search(self, query: str, skip: int = 0, limit: int = 20) -> list[ProductOut]:
        response = await self._client.search(
            index=self._index,
            from_=skip,
            size=limit,
            query={
                "multi_match": {
                    "query": query,
                    "fields": ["name^3", "category^2", "description", "tags", "specifications.*"],
                }
            },
        )
        return [
            ProductOut.model_validate(hit["_source"])
            for hit in response["hits"]["hits"]
        ]
