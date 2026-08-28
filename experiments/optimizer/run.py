"""RQ4: does serving-optimizer's workload-aware tuning actually reduce
serving latency, and at what write/indexing overhead? Three real
sub-experiments against the live stack, each comparing the optimizer
stopped vs. running under the same traffic:

1. Postgres: repeated GET /users?country=X (a filtered, unindexed query
   -- see pg_optimizer.py's own PG_CREATE_THRESHOLD_CALLS) with the
   optimizer off (no index gets created) vs. on (an index should appear
   once traffic crosses the threshold across two 15s poll cycles),
   measuring read latency and write latency (POST /users) before/after.
2. Elasticsearch: a burst of product creates with the optimizer off
   (refresh_interval stays 1s) vs. on (should raise it to 5s once the
   burst crosses ES_BURST_OPS_THRESHOLD), measuring time-to-searchable
   per product and total burst wall time.
3. Cassandra: a concentrated burst of events for one product (well past
   CASSANDRA_HOT_VOLUME_THRESHOLD=50) with the optimizer running,
   checking whether it actually gets marked hot in /tuning/decisions.
   No off/on comparison here -- this mechanism only exists when the
   optimizer runs at all, and organic traffic (spread across thousands
   of random product ids) never concentrates enough on one product to
   trigger it naturally.

Requires the stack up, including serving-optimizer, postgres,
elasticsearch, cassandra, user-service, product-service, event-service.
"""
import statistics
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))
from experiments.common import record_result  # noqa: E402

USER_SERVICE = "http://localhost:8002"
PRODUCT_SERVICE = "http://localhost:8001"
EVENT_SERVICE = "http://localhost:8003"
OPTIMIZER_SERVICE = "http://localhost:8007"
RESULTS_DIR = Path(__file__).resolve().parent / "results"

COUNTRIES = ["US", "IN", "DE", "UK"]
PG_POLL_INTERVAL_S = 15


def percentile(values, p):
    if not values:
        return None
    if len(values) < 2:
        return values[0]
    return statistics.quantiles(values, n=100, method="inclusive")[p - 1]


def stop_optimizer():
    subprocess.run(["docker", "compose", "stop", "serving-optimizer"], cwd=REPO_ROOT, capture_output=True, timeout=30)


def start_optimizer():
    subprocess.run(["docker", "compose", "start", "serving-optimizer"], cwd=REPO_ROOT, capture_output=True, timeout=30)


def psql(sql, db="user_db"):
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "postgres", "-d", db, "-t", "-c", sql],
        cwd=REPO_ROOT, capture_output=True, text=True, timeout=15,
    )
    return result.stdout.strip()


def country_index_exists():
    out = psql("SELECT indexname FROM pg_indexes WHERE tablename='users' AND indexdef ILIKE '%country%';")
    return bool(out)


def measure_read_latency(n=30):
    latencies = []
    with httpx.Client(timeout=10.0) as client:
        for i in range(n):
            country = COUNTRIES[i % len(COUNTRIES)]
            start = time.perf_counter()
            resp = client.get(f"{USER_SERVICE}/users", params={"country": country})
            resp.raise_for_status()
            latencies.append((time.perf_counter() - start) * 1000)
    return latencies


def measure_write_latency(n=10):
    latencies = []
    with httpx.Client(timeout=10.0) as client:
        for i in range(n):
            marker = uuid.uuid4().hex[:8]
            payload = {"username": f"opt_test_{marker}", "email": f"opt_test_{marker}@example.com", "country": COUNTRIES[i % len(COUNTRIES)]}
            start = time.perf_counter()
            resp = client.post(f"{USER_SERVICE}/users", json=payload)
            resp.raise_for_status()
            latencies.append((time.perf_counter() - start) * 1000)
    return latencies


def run_postgres_experiment():
    print("=== Postgres index experiment ===", flush=True)
    stop_optimizer()
    time.sleep(2)
    index_before = country_index_exists()
    read_off = measure_read_latency(30)
    write_off = measure_write_latency(10)
    print(f"[optimizer OFF] index_on_country={index_before} "
          f"read_p50={percentile(read_off,50):.2f}ms read_p95={percentile(read_off,95):.2f}ms "
          f"write_p50={percentile(write_off,50):.2f}ms", flush=True)

    start_optimizer()
    print("Optimizer started, sustaining query traffic across poll cycles...", flush=True)
    deadline = time.monotonic() + PG_POLL_INTERVAL_S * 2 + 5
    with httpx.Client(timeout=10.0) as client:
        i = 0
        while time.monotonic() < deadline:
            client.get(f"{USER_SERVICE}/users", params={"country": COUNTRIES[i % len(COUNTRIES)]})
            i += 1
            time.sleep(0.3)
    time.sleep(PG_POLL_INTERVAL_S + 2)  # let one more cycle pass so CREATE INDEX CONCURRENTLY finishes

    index_after = country_index_exists()
    read_on = measure_read_latency(30)
    write_on = measure_write_latency(10)
    print(f"[optimizer ON] index_on_country={index_after} "
          f"read_p50={percentile(read_on,50):.2f}ms read_p95={percentile(read_on,95):.2f}ms "
          f"write_p50={percentile(write_on,50):.2f}ms", flush=True)

    result = {
        "index_created": bool(index_after and not index_before),
        "read_latency_ms_off": {"p50": percentile(read_off, 50), "p95": percentile(read_off, 95)},
        "read_latency_ms_on": {"p50": percentile(read_on, 50), "p95": percentile(read_on, 95)},
        "write_latency_ms_off": {"p50": percentile(write_off, 50), "p95": percentile(write_off, 95)},
        "write_latency_ms_on": {"p50": percentile(write_on, 50), "p95": percentile(write_on, 95)},
    }
    record_result(
        RESULTS_DIR, name="optimizer_postgres", config={"queries": 30, "writes": 10},
        dataset="live user-service traffic", model="pg_index_tuner",
        metric="read_latency_p50_ms,read_latency_p95_ms,write_latency_p50_ms", result=result,
    )
    return result


def es_burst(n=20):
    created = []
    burst_start = time.perf_counter()
    with httpx.Client(timeout=10.0) as client:
        for _ in range(n):
            marker = uuid.uuid4().hex[:12]
            payload = {"name": f"OptimizerTestProduct-{marker}", "category": "electronics", "price": 9.99, "description": "optimizer experiment product"}
            create_start = time.perf_counter()
            resp = client.post(f"{PRODUCT_SERVICE}/products", json=payload)
            resp.raise_for_status()
            created.append((marker, create_start))
    burst_wall_s = time.perf_counter() - burst_start

    times_to_searchable = []
    with httpx.Client(timeout=10.0) as client:
        for marker, create_start in created:
            deadline = create_start + 15
            found_at = None
            while time.perf_counter() < deadline:
                resp = client.get(f"{PRODUCT_SERVICE}/products/search", params={"q": marker})
                if resp.json().get("results"):
                    found_at = time.perf_counter()
                    break
                time.sleep(0.2)
            if found_at:
                times_to_searchable.append((found_at - create_start) * 1000)
    return burst_wall_s, times_to_searchable


def run_es_experiment():
    print("=== Elasticsearch refresh-interval experiment ===", flush=True)
    stop_optimizer()
    time.sleep(2)
    burst_wall_off, tts_off = es_burst(20)
    print(f"[optimizer OFF] burst_wall={burst_wall_off:.2f}s "
          f"time_to_searchable_p50={percentile(tts_off,50):.1f}ms found={len(tts_off)}/20", flush=True)

    start_optimizer()
    time.sleep(3)
    burst_wall_on, tts_on = es_burst(20)
    print(f"[optimizer ON] burst_wall={burst_wall_on:.2f}s "
          f"time_to_searchable_p50={percentile(tts_on,50):.1f}ms found={len(tts_on)}/20", flush=True)

    tuning_status = httpx.get(f"{OPTIMIZER_SERVICE}/tuning/status", timeout=10).json().get("elasticsearch")

    result = {
        "burst_wall_s_off": round(burst_wall_off, 2),
        "burst_wall_s_on": round(burst_wall_on, 2),
        "time_to_searchable_ms_off": {"p50": percentile(tts_off, 50), "p95": percentile(tts_off, 95), "found": len(tts_off)},
        "time_to_searchable_ms_on": {"p50": percentile(tts_on, 50), "p95": percentile(tts_on, 95), "found": len(tts_on)},
        "es_tuner_status_after": tuning_status,
    }
    record_result(
        RESULTS_DIR, name="optimizer_elasticsearch", config={"burst_size": 20},
        dataset="live product-service traffic", model="es_refresh_tuner",
        metric="burst_wall_s,time_to_searchable_ms", result=result,
    )
    return result


def _product_granularity(product_id):
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "cassandra", "cqlsh", "-e",
         f"SELECT granularity FROM ecommerce.partition_strategy WHERE product_id={product_id};"],
        cwd=REPO_ROOT, capture_output=True, text=True, timeout=20,
    )
    return "hot" in result.stdout


def _wait_for_mark_hot(product_id, deadline_s=60, poll_s=5):
    # /tuning/decisions defaults to the 50 most recent entries, which the
    # residual reclassification churn from earlier heavy test traffic
    # (thousands of products aging in/out of the 60-minute lookback) can
    # bury a single decision under within moments -- querying it produced
    # false negatives even for changes that had genuinely landed. The
    # partition_strategy table itself is the authoritative source, so
    # check that directly instead.
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        if _product_granularity(product_id):
            return True
        time.sleep(poll_s)
    return False


def run_cassandra_experiment(product_id=999001, n_events=80):
    print("=== Cassandra hot/cold experiment ===", flush=True)
    start_optimizer()
    time.sleep(2)
    with httpx.Client(timeout=10.0) as client:
        for _ in range(n_events):
            client.post(f"{EVENT_SERVICE}/events", json={"user_id": 1, "product_id": product_id, "event_type": "view"})
    print(f"Fired {n_events} events for product_id={product_id}, polling for the mark_hot decision...", flush=True)

    marked_hot = _wait_for_mark_hot(product_id)
    status = httpx.get(f"{OPTIMIZER_SERVICE}/tuning/status", timeout=10).json().get("cassandra")
    print(f"product_id={product_id} marked_hot={marked_hot} cassandra_status={status}", flush=True)

    result = {"product_id": product_id, "events_fired": n_events, "marked_hot": marked_hot, "cassandra_status_after": status}
    record_result(
        RESULTS_DIR, name="optimizer_cassandra", config={"product_id": product_id, "n_events": n_events},
        dataset="live event-service traffic (concentrated on one product)", model="cassandra_partition_tuner",
        metric="marked_hot", result=result,
    )
    return result


def main():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    run_postgres_experiment()
    run_es_experiment()
    run_cassandra_experiment()
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
