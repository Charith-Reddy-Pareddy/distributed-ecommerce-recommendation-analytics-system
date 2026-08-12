"""Tuning thresholds, all overridable via environment variables."""
import os

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")
PG_OPTIMIZER_DB = os.getenv("PG_OPTIMIZER_DB", "optimizer_db")
PG_TARGET_DBS = os.getenv("PG_TARGET_DBS", "user_db,analytics_db").split(",")

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://elasticsearch:9200")
ES_INDEX = os.getenv("ES_INDEX", "products")

POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "15"))

# --- Postgres index tuner ---
PG_CREATE_THRESHOLD_CALLS = int(os.getenv("PG_CREATE_THRESHOLD_CALLS", "20"))
PG_STALE_CYCLES = int(os.getenv("PG_STALE_CYCLES", "3"))
PG_MIN_TABLE_ROWS = int(os.getenv("PG_MIN_TABLE_ROWS", "20"))
PG_MIN_DISTINCT_RATIO = float(os.getenv("PG_MIN_DISTINCT_RATIO", "0.05"))
PG_WRITE_RATIO_THRESHOLD = float(os.getenv("PG_WRITE_RATIO_THRESHOLD", "3.0"))

# --- Cassandra partition tuner ---
CASSANDRA_HOT_VOLUME_THRESHOLD = int(os.getenv("CASSANDRA_HOT_VOLUME_THRESHOLD", "50"))
CASSANDRA_LOOKBACK_MINUTES = int(os.getenv("CASSANDRA_LOOKBACK_MINUTES", "60"))

# --- Elasticsearch refresh-interval tuner ---
ES_BURST_OPS_THRESHOLD = int(os.getenv("ES_BURST_OPS_THRESHOLD", "15"))
ES_BURST_WINDOW_SECONDS = int(os.getenv("ES_BURST_WINDOW_SECONDS", "15"))
ES_BURST_REFRESH_INTERVAL = os.getenv("ES_BURST_REFRESH_INTERVAL", "5s")
ES_NORMAL_REFRESH_INTERVAL = os.getenv("ES_NORMAL_REFRESH_INTERVAL", "1s")
