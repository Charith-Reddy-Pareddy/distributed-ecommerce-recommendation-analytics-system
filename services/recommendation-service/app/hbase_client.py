"""Thin client for HBase's REST server (Stargate) -- fetches the ALS
batch-layer recommendations that jobs/hbase-load-recommendations
loaded from HDFS, for low-latency point lookups by user id.

Note on IDs: these are RetailRocket's item ids (the real, external
dataset the ALS model was trained on), not this project's own demo
product-service catalog (ids 1-20, seeded via scripts/seed_data.py).
They're two intentionally separate id spaces -- the RetailRocket data
exists to train and evaluate a real model at real scale, while the
demo catalog exists to exercise the live microservices end-to-end
without needing a full external catalog wired into every service. So
unlike the other /recommendations endpoints, this one does not (and
cannot) enrich results via product-service.
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
