import os

import redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM_NAME = "events_stream"

redis_client = redis.from_url(REDIS_URL, decode_responses=True)
