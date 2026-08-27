"""Thin client for HBase's REST server (Stargate) -- fetches
precomputed ALS recommendations for low-latency lookups by user id.

Whether the returned item ids match this project's demo catalog depends
on which model loaded this HBase table: the original RetailRocket job
(jobs/als-training/) uses RetailRocket's own item ids, a different id
space from the catalog; the catalog-native model
(experiments/recommendation/catalog_als/) shares the catalog's ids, so
its results can be enriched via product-service like every other
endpoint. main.py's callers handle both by falling back to raw ids if
enrichment fails.
"""
import base64
import os

import httpx

HBASE_REST_URL = os.getenv("HBASE_REST_URL", "http://hbase-rest:8080")
TABLE_NAME = "als_recommendations"


def _b64_decode(s: str) -> str:
    return base64.b64decode(s).decode("utf-8")


async def get_precomputed_recommendations(user_id: int) -> list[dict] | None:
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{HBASE_REST_URL}/{TABLE_NAME}/{user_id}",
            headers={"Accept": "application/json"},
            timeout=5.0,
        )

    if resp.status_code == 404:
        return None
    resp.raise_for_status()

    cells: dict[str, str] = {}
    for row in resp.json()["Row"]:
        for cell in row["Cell"]:
            column = _b64_decode(cell["column"])
            value = _b64_decode(cell["$"])
            cells[column] = value

    ranked = []
    rank = 0
    while f"rec:item_{rank:02d}" in cells:
        ranked.append(
            {
                "itemid": int(cells[f"rec:item_{rank:02d}"]),
                "score": float(cells[f"rec:score_{rank:02d}"]),
            }
        )
        rank += 1

    return ranked
