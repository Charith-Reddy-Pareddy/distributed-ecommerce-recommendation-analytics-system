"""Regression test for a real bug found live-auditing this repo:
event-service keeps no database of its own -- Kafka is its only
persistence -- but its FastAPI app never called kafka_producer.flush()
on shutdown, even though flush() existed and was clearly written for
exactly this. produce() only calls poll(0), which services delivery
callbacks but doesn't block until the broker acks, so a message still
sitting in the producer's internal buffer at process shutdown (e.g.
`docker compose down`, a SIGTERM) could be silently dropped even
though the client already received a 202 "accepted" response for it.

Drives main.py's actual lifespan context manager (not a
reimplementation of what it should do) via asyncio.run() directly,
without needing FastAPI's TestClient/httpx as a test dependency.
"""
import asyncio

from conftest import load_app_module

kafka_producer = load_app_module("event-service", "kafka_producer", "eventsvc_app")
main = load_app_module("event-service", "main", "eventsvc_app")


def test_lifespan_shutdown_flushes_the_producer(monkeypatch):
    flushed = []
    monkeypatch.setattr(kafka_producer, "flush", lambda: flushed.append(True))
    # main.py does `from .kafka_producer import flush, publish_event`,
    # binding its own name to the pre-patch function object -- patch
    # main's reference too, the same way the real module would be
    # reloaded-and-rebound in a fresh process.
    monkeypatch.setattr(main, "flush", lambda: flushed.append(True))

    async def _run_lifespan():
        async with main.lifespan(main.app):
            pass

    asyncio.run(_run_lifespan())

    assert flushed == [True], "app shutdown must flush the Kafka producer's buffer, not just close silently"
