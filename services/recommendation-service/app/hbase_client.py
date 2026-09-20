"""Thin client for HBase's REST server (Stargate) -- fetches
precomputed ALS recommendations for low-latency lookups by user id.

This table is loaded from jobs/als-training/, trained on RetailRocket
(real interaction data). RetailRocket's item ids are a different,
disjoint space from this project's own demo catalog -- real
interactions on someone else's real catalog, not this one -- so the
returned ids can never be enriched via product-service; main.py's
caller always falls back to raw ids. A synthetic interaction log
generated over this project's own catalog specifically to make the
ids line up used to exist for this (letting ALS results resolve to
real product names), but simulating "recommendation quality" numbers
on fabricated behavior isn't a trade worth making just for that.
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
