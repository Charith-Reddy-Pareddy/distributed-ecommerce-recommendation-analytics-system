"""Measure Elasticsearch catalog indexing with source Amazon products."""

import argparse
import asyncio
import json
import os
from time import perf_counter
from typing import Any
from uuid import uuid4

from elasticsearch import AsyncElasticsearch

from ecommerce_analytics.als_export import load_catalog_map
from services.product_service.app.amazon_catalog import load_catalog
from services.product_service.app.schemas import ProductCreate, ProductOut
from services.product_service.app.search import ElasticsearchProductIndex


def _products(
    rows: list[ProductCreate], id_map: dict[str, int], size: int
) -> list[ProductOut]:
    if size < 1:
        raise ValueError("benchmark sizes must be positive")

    selected = []
    for row in rows:
        if row.asin in id_map:
            selected.append(ProductOut(id=id_map[row.asin], **row.model_dump()))
            if len(selected) == size:
                return selected
    raise ValueError(f"catalog has fewer than {size} mapped products")


async def benchmark_catalog(
    client: Any,
    rows: list[ProductCreate],
    id_map: dict[str, int],
    sizes: tuple[int, ...] = (100, 300, 1000),
) -> list[dict[str, float | int]]:
    if not sizes or any(size < 1 for size in sizes):
        raise ValueError("benchmark sizes must be positive")
    if tuple(sorted(set(sizes))) != sizes:
        raise ValueError("benchmark sizes must be unique and increasing")

    results = []
    run_id = uuid4().hex[:8]
    for size in sizes:
        index_name = f"catalog-benchmark-{run_id}-{size}"
        selected = _products(rows, id_map, size)
        start = perf_counter()
        try:
            count = await ElasticsearchProductIndex(client, index_name).index_catalog(
                selected
            )
        finally:
            await client.indices.delete(index=index_name, ignore_unavailable=True)
        elapsed = perf_counter() - start
        results.append(
            {
                "products": count,
                "elapsed_seconds": elapsed,
                "products_per_second": count / elapsed if elapsed else 0.0,
            }
        )
    return results


async def _run(args: argparse.Namespace) -> None:
    client = AsyncElasticsearch(args.url, request_timeout=30)
    try:
        rows = load_catalog()
        id_map = load_catalog_map(args.catalog_map)
        results = await benchmark_catalog(client, rows, id_map, tuple(args.sizes))
        print(json.dumps(results, indent=2))
    finally:
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url", default=os.environ.get("ELASTICSEARCH_URL", "http://localhost:9200")
    )
    parser.add_argument(
        "--catalog-map", default="data/asin_product_id_map.json"
    )
    parser.add_argument("--sizes", nargs="+", type=int, default=[100, 300, 1000])
    asyncio.run(_run(parser.parse_args()))


if __name__ == "__main__":
    main()
