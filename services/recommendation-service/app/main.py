import os
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException

from .hbase_client import get_precomputed_recommendations
from .hybrid import blend
from .model import engine
from .metrics import MetricsMiddleware, metrics_response

PRODUCT_SERVICE_URL = os.getenv("PRODUCT_SERVICE_URL", "http://localhost:8001")


@asynccontextmanager
async def lifespan(app: FastAPI):
    engine.start()
    yield


app = FastAPI(title="Recommendation Service", lifespan=lifespan)
app.add_middleware(MetricsMiddleware)


@app.get("/metrics")
def metrics():
    return metrics_response()


@app.get("/health")
def health():
    return {"status": "ok", "service": "recommendation-service"}


async def _enrich_with_products(scored_ids: list[tuple[int, float]]) -> list[dict]:
    if not scored_ids:
        return []

    ids = [product_id for product_id, _ in scored_ids]
    async with httpx.AsyncClient() as client:
        resp = await client.post(f"{PRODUCT_SERVICE_URL}/products/batch", json=ids)
        resp.raise_for_status()
        products = {p["id"]: p for p in resp.json()}

    results = []
    for product_id, score in scored_ids:
        product = products.get(product_id)
        if product:
            results.append({**product, "score": round(score, 4)})
    return results


# Literal-path routes must be registered before the "/{user_id}" catch-all
# below, or FastAPI will match "/recommendations/popular" as user_id="popular".
@app.get("/recommendations/similar/{product_id}")
async def similar(product_id: int, n: int = 5):
    scored = engine.similar_items(product_id, top_n=n)
    return {"product_id": product_id, "similar_products": await _enrich_with_products(scored)}


@app.get("/recommendations/popular")
async def popular(n: int = 10):
    scored = engine.popular_items(top_n=n)
    return {"popular_products": await _enrich_with_products(scored)}


# Batch-layer ALS recs from HBase. Enriched via product-service when the
# loaded model is catalog-native (experiments/recommendation/catalog_als/),
# whose ids match the demo catalog -- falls back to raw ids if enrichment
# fails, since HBase may still hold the older RetailRocket-id-space model.
@app.get("/recommendations/precomputed/{visitor_id}")
async def precomputed(visitor_id: int):
    recs = await get_precomputed_recommendations(visitor_id)
    if recs is None:
        raise HTTPException(status_code=404, detail="No precomputed recommendations for this visitor_id")

    scored = [(r["itemid"], r["score"]) for r in recs]
    try:
        enriched = await _enrich_with_products(scored)
    except httpx.HTTPError:
        enriched = None

    if enriched:
        return {"visitor_id": visitor_id, "source": "als-hbase", "recommendations": enriched}
    return {"visitor_id": visitor_id, "source": "als-hbase", "recommendations": recs}


@app.get("/recommendations/{user_id}")
async def recommend(user_id: int, n: int = 5):
    scored = engine.recommend_for_user(user_id, top_n=n)
    return {"user_id": user_id, "recommendations": await _enrich_with_products(scored)}


# Blends live item-CF with precomputed catalog-ALS (see hybrid.py for why
# alpha=0.25). Falls back to pure CF if this user has no HBase row yet --
# new users, or HBase not yet loaded with the catalog-native model.
@app.get("/recommendations/hybrid/{user_id}")
async def hybrid(user_id: int, n: int = 5, alpha: float | None = None):
    cf_scored = engine.recommend_for_user(user_id, top_n=50)
    als_recs = await get_precomputed_recommendations(user_id)

    if not als_recs:
        return {
            "user_id": user_id,
            "source": "item-cf (no precomputed ALS for this user)",
            "recommendations": await _enrich_with_products(cf_scored[:n]),
        }

    als_scored = [(r["itemid"], r["score"]) for r in als_recs]
    blend_kwargs = {} if alpha is None else {"alpha": alpha}
    blended = blend(cf_scored, als_scored, **blend_kwargs)
    return {
        "user_id": user_id,
        "source": "hybrid-cf-als",
        "recommendations": await _enrich_with_products(blended[:n]),
    }
