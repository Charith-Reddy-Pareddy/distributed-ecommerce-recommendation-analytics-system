"""Elasticsearch integration: full-text search over name/description/
tags, with structured filters (category, brand, price range, minimum
rating) and an optional geo-distance filter over the same warehouse
location already stored on each product in MongoDB. Indexing happens
on every product create (see crud.py) so the ES index stays in sync
going forward, plus a startup backfill for products that existed
before this index did (or if the index was wiped -- ES here has no
persistent volume, so a restart starts empty and self-heals from
MongoDB, which remains the source of truth).
"""
import os

from elasticsearch import AsyncElasticsearch

ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")
INDEX_NAME = "products"

client = AsyncElasticsearch(ELASTICSEARCH_URL)

INDEX_MAPPING = {
    "settings": {"number_of_replicas": 0},
    "mappings": {
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "text"},
            "category": {"type": "keyword"},
            "brand": {"type": "keyword"},
            "price": {"type": "float"},
            "description": {"type": "text"},
            "tags": {"type": "keyword"},
            "images": {"type": "keyword", "index": False},
            "location": {"type": "geo_point"},
            "rating_average": {"type": "float"},
            "rating_count": {"type": "integer"},
            "asin": {"type": "keyword", "index": False},
        }
    },
}

SORT_OPTIONS = {
    "price_asc": [{"price": "asc"}],
    "price_desc": [{"price": "desc"}],
    "rating_desc": [{"rating_average": "desc"}],
}


async def ensure_index() -> None:
    if not await client.indices.exists(index=INDEX_NAME):
        await client.indices.create(index=INDEX_NAME, body=INDEX_MAPPING)


def _to_es_doc(product: dict) -> dict:
    doc = {
        "id": product["id"],
        "name": product["name"],
        "category": product["category"],
        "brand": product.get("specifications", {}).get("brand", ""),
        "price": product["price"],
        "description": product.get("description", ""),
        "tags": product.get("tags", []),
        "images": product.get("images", []),
        "rating_average": product.get("rating", {}).get("average", 0.0),
        "rating_count": product.get("rating", {}).get("count", 0),
        "asin": product.get("asin"),
    }
    location = product.get("location")
    if location and location.get("coordinates"):
        lon, lat = location["coordinates"]
        doc["location"] = {"lat": lat, "lon": lon}
    return doc


async def index_product(product: dict) -> None:
    await client.index(index=INDEX_NAME, id=str(product["id"]), document=_to_es_doc(product))


async def search_products(
    query: str | None = None,
    category: str | None = None,
    brand: str | None = None,
    price_min: float | None = None,
    price_max: float | None = None,
    rating_min: float | None = None,
    lat: float | None = None,
    lon: float | None = None,
    radius_km: float | None = None,
    sort: str | None = None,
    limit: int = 20,
) -> list[dict]:
    must: list[dict] = []
    filter_: list[dict] = []

    if query:
        must.append({"multi_match": {"query": query, "fields": ["name^2", "description", "tags"]}})
    else:
        must.append({"match_all": {}})

    if category:
        filter_.append({"term": {"category": category}})

    if brand:
        filter_.append({"term": {"brand": brand}})

    if price_min is not None or price_max is not None:
        price_range: dict = {}
        if price_min is not None:
            price_range["gte"] = price_min
        if price_max is not None:
            price_range["lte"] = price_max
        filter_.append({"range": {"price": price_range}})

    if rating_min is not None:
        filter_.append({"range": {"rating_average": {"gte": rating_min}}})

    if lat is not None and lon is not None and radius_km is not None:
        filter_.append(
            {"geo_distance": {"distance": f"{radius_km}km", "location": {"lat": lat, "lon": lon}}}
        )

    body: dict = {"query": {"bool": {"must": must, "filter": filter_}}, "size": limit}
    if sort in SORT_OPTIONS:
        body["sort"] = SORT_OPTIONS[sort]

    result = await client.search(index=INDEX_NAME, body=body)
    return [hit["_source"] for hit in result["hits"]["hits"]]
