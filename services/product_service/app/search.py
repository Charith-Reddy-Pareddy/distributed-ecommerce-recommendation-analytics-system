from typing import Protocol

from elasticsearch import AsyncElasticsearch

from services.product_service.app.schemas import ProductOut


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
            id=str(product.id),
            document=product.model_dump(mode="json"),
        )

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
