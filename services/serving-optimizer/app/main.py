"""Runs the Postgres, Cassandra, and Elasticsearch tuners as background
threads and exposes their status over HTTP.
"""
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI

from . import cassandra_optimizer, decision_log, es_optimizer, pg_optimizer
from .metrics import MetricsMiddleware, metrics_response


@asynccontextmanager
async def lifespan(app: FastAPI):
    decision_log.init()
    threading.Thread(target=pg_optimizer.run_forever, daemon=True).start()
    threading.Thread(target=cassandra_optimizer.run_forever, daemon=True).start()
    threading.Thread(target=es_optimizer.run_forever, daemon=True).start()
    yield


app = FastAPI(title="Serving-Layer Optimizer", lifespan=lifespan)
app.add_middleware(MetricsMiddleware)


@app.get("/metrics")
def metrics():
    return metrics_response()


@app.get("/health")
def health():
    return {"status": "ok", "service": "serving-optimizer"}


@app.get("/tuning/decisions")
def tuning_decisions(limit: int = 50):
    return decision_log.recent(limit)


@app.get("/tuning/status")
def tuning_status():
    return {
        "postgres": pg_optimizer.get_status(),
        "cassandra": cassandra_optimizer.get_status(),
        "elasticsearch": es_optimizer.get_status(),
    }
