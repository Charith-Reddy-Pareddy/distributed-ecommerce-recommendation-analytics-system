from asyncio import run
from decimal import Decimal

from services.product_service.app.schemas import ProductOut
from services.product_service.app.search import ElasticsearchProductIndex


class FakeElasticsearch:
    def __init__(self, hits: list[dict] | None = None) -> None:
        self.hits = hits or []
        self.index_call: dict | None = None
        self.search_call: dict | None = None

    async def index(self, **kwargs) -> None:
        self.index_call = kwargs

    async def search(self, **kwargs) -> dict:
        self.search_call = kwargs
        return {"hits": {"hits": [{"_source": hit} for hit in self.hits]}}


def test_index_product_uses_product_id_and_keeps_price() -> None:
    client = FakeElasticsearch()
    index = ElasticsearchProductIndex(client)
    product = ProductOut(id=7, name="Desk lamp", category="Lighting", price=Decimal("24.50"))

    run(index.index_product(product))

    assert client.index_call["index"] == "products"
    assert client.index_call["id"] == "7"
    assert client.index_call["document"]["price"] == "24.50"


def test_search_maps_hits_to_product_models() -> None:
    client = FakeElasticsearch(
        [{"id": 7, "name": "Desk lamp", "category": "Lighting", "price": "24.50"}]
    )
    index = ElasticsearchProductIndex(client, "catalog")

    products = run(index.search("lamp", skip=10, limit=5))

    assert products == [
        ProductOut(id=7, name="Desk lamp", category="Lighting", price=Decimal("24.50"))
    ]
    assert client.search_call["index"] == "catalog"
    assert client.search_call["from_"] == 10
    assert client.search_call["size"] == 5
    assert client.search_call["query"]["multi_match"]["query"] == "lamp"
