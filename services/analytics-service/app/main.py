from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI
from sqlalchemy.orm import Session

from . import models
from .cassandra_client import get_demand_timeseries
from .consumer import start_consumer
from .database import engine, get_db
from .metrics import MetricsMiddleware, metrics_response

models.Base.metadata.create_all(bind=engine)


@asynccontextmanager
async def lifespan(app: FastAPI):
    start_consumer()
    yield


app = FastAPI(title="Analytics Service", lifespan=lifespan)
app.add_middleware(MetricsMiddleware)


@app.get("/metrics")
def metrics():
    return metrics_response()


@app.get("/health")
def health():
    return {"status": "ok", "service": "analytics-service"}


@app.get("/analytics/top-products")
def top_products(limit: int = 10, db: Session = Depends(get_db)):
    score = (
        models.ProductStats.views
        + models.ProductStats.add_to_carts * 3
        + models.ProductStats.purchases * 5
    )
    rows = (
        db.query(models.ProductStats)
        .order_by(score.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "product_id": r.product_id,
            "views": r.views,
            "add_to_carts": r.add_to_carts,
            "purchases": r.purchases,
        }
        for r in rows
    ]


@app.get("/analytics/summary")
def summary(db: Session = Depends(get_db)):
    rows = (
        db.query(models.DailyEventCount)
        .order_by(models.DailyEventCount.day.desc())
        .limit(90)
        .all()
    )
    return [
        {"day": r.day.isoformat(), "event_type": r.event_type, "count": r.count}
        for r in rows
    ]


# Cassandra-backed, unlike the Postgres endpoints above -- written live
# by the Spark Streaming job.
@app.get("/analytics/demand-timeseries/{product_id}")
def demand_timeseries(product_id: int, days: int = 1):
    return {
        "product_id": product_id,
        "days": days,
        "points": get_demand_timeseries(product_id, days=days),
    }
