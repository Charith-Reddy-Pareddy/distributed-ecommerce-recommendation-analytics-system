"""Autonomous serving-layer optimizer: three independent background
tuners (Postgres indexes, Cassandra partition strategy, Elasticsearch
refresh_interval), each polling its own store's already-aggregated
telemetry (pg_stat_statements, the existing product_demand_by_minute
table, ES's _stats API) rather than a shared streaming pipeline -- a bug
in one tuner cannot affect another or any existing verified service.
Inspired by github.com/nimit-pasricha/auto-indexing (see README.md for
the concrete differences); this reimplements the idea's heuristics
independently, extended across three stores instead of one.
"""
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI

from . import cassandra_optimizer, decision_log, es_optimizer, pg_optimizer


@asynccontextmanager
async def lifespan(app: FastAPI):
    decision_log.init()
    threading.Thread(target=pg_optimizer.run_forever, daemon=True).start()
    threading.Thread(target=cassandra_optimizer.run_forever, daemon=True).start()
    threading.Thread(target=es_optimizer.run_forever, daemon=True).start()
    yield


app = FastAPI(title="Serving-Layer Optimizer", lifespan=lifespan)


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
