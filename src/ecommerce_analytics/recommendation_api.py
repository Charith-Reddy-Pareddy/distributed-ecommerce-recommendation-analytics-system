from typing import Protocol
from urllib.error import URLError
import os

from fastapi import Depends, FastAPI, HTTPException, Path, Query
from pydantic import BaseModel, Field

from ecommerce_analytics.hbase_recommendations import HBaseRecommendations


class ScoredProduct(BaseModel):
    product_id: int = Field(gt=0)
    score: float


class RecommendationResult(BaseModel):
    user_id: str
    recommendations: list[ScoredProduct]


class RecommendationStore(Protocol):
    def get(self, user_id: str) -> list[dict[str, int | float]] | None: ...


def get_store() -> RecommendationStore:
    return HBaseRecommendations()


app = FastAPI(title="Recommendation Service")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "service": "recommendation-service"}


@app.get("/recommendations/{user_id}", response_model=RecommendationResult)
def recommend(
    user_id: str = Path(min_length=1),
    n: int = Query(default=10, ge=1, le=100),
    store: RecommendationStore = Depends(get_store),
) -> RecommendationResult:
    try:
        recommendations = store.get(user_id)
    except URLError as exc:
        raise HTTPException(
            status_code=503, detail="Recommendation store is unavailable"
        ) from exc
    if recommendations is None:
        raise HTTPException(status_code=404, detail="No recommendations for this user")
    return RecommendationResult(
        user_id=user_id,
        recommendations=[ScoredProduct(**rec) for rec in recommendations[:n]],
    )


def main() -> None:
    import uvicorn

    uvicorn.run(
        app,
        host=os.getenv("RECOMMENDATION_HOST", "0.0.0.0"),
        port=int(os.getenv("RECOMMENDATION_PORT", "8003")),
    )


if __name__ == "__main__":
    main()
