"""Publishes ingested events onto a Redis Stream, which acts as the
lightweight event bus that the recommendation and analytics services
consume from independently. This decouples event-service from its
downstream consumers -- it doesn't know or care who reads the stream.
"""
import json
import os

import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM_NAME = "events_stream"

redis_client = redis.from_url(REDIS_URL, decode_responses=True)


def publish_event(event: dict) -> str:
    return redis_client.xadd(STREAM_NAME, {"data": json.dumps(event)})
