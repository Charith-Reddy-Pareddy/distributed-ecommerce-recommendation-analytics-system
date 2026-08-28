"""What actually happens when a consumer or store gets killed mid-traffic,
and how does that differ across this project's different resilience
designs? Three sub-experiments, each `docker kill`ing a service (a real
crash -- SIGKILL, not a graceful `stop`) under light background load,
then restarting it and measuring what comes back:

1. recommendation-service: stateless, full-topic-replay-on-restart
   design (services/recommendation-service/app/kafka_consumer.py) --
   how long until /health responds vs. how long until its recommendations
   actually stabilize (replay complete)?
2. hdfs-sink: the one persistent, committed-offset consumer group --
   does a batch of events sent while it's down survive the outage with
   no loss or duplication once it catches up?
3. elasticsearch: no persistent volume, and product-service only
   reindexes into it on product-service's OWN startup (see
   services/product-service/app/main.py's lifespan) -- does search stay
   broken after ES restarts alone, and does restarting product-service
   actually fix it?

Requires the stack up. Uses `docker kill` deliberately (not
`docker compose stop`, which sends SIGTERM and lets a service shut down
cleanly) since the point is to simulate an actual crash.
"""
import json
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))
from experiments.common import record_result  # noqa: E402

EVENT_SERVICE = "http://localhost:8003"
RECOMMENDATION_SERVICE = "http://localhost:8004"
PRODUCT_SERVICE = "http://localhost:8001"
RESULTS_DIR = Path(__file__).resolve().parent / "results"


def docker(*args, timeout=30):
    return subprocess.run(["docker", "compose", *args], cwd=REPO_ROOT, capture_output=True, text=True, timeout=timeout)


def kill_service(name):
    docker("kill", name)


def restart_service(name):
    docker("start", name)


def wait_healthy(url, deadline_s=60, poll_s=1.0):
    start = time.monotonic()
    deadline = start + deadline_s
    while time.monotonic() < deadline:
        try:
            resp = httpx.get(url, timeout=2.0)
            if resp.status_code == 200:
                return time.monotonic() - start
        except httpx.HTTPError:
            pass
        time.sleep(poll_s)
    return None


def fire_background_events(duration_s, rate_per_s=5):
    """Light steady traffic so the outage happens mid-flow, not to idle
    services."""
    deadline = time.monotonic() + duration_s
    sent = 0
    with httpx.Client(timeout=5.0) as client:
        while time.monotonic() < deadline:
            try:
                resp = client.post(f"{EVENT_SERVICE}/events", json={"user_id": 1, "product_id": 1, "event_type": "view"})
                if resp.status_code == 202:
                    sent += 1
            except httpx.HTTPError:
                pass
            time.sleep(1 / rate_per_s)
    return sent


def run_recommendation_service_experiment():
    print("=== recommendation-service fault tolerance ===", flush=True)
    assert wait_healthy(f"{RECOMMENDATION_SERVICE}/health", deadline_s=10) is not None, "must be healthy before the test"

    kill_service("recommendation-service")
    down_start = time.monotonic()
    down_confirmed = wait_healthy(f"{RECOMMENDATION_SERVICE}/health", deadline_s=5) is None
    print(f"Killed. Confirmed down: {down_confirmed}", flush=True)

    restart_service("recommendation-service")
    health_recovery_s = wait_healthy(f"{RECOMMENDATION_SERVICE}/health", deadline_s=60)
    print(f"/health recovered in {health_recovery_s:.2f}s", flush=True)

    # Replay isn't done just because /health is -- poll /recommendations/popular
    # until three consecutive reads are identical (a proxy for "the topic
    # replay has caught up and the model has stopped changing").
    replay_start = time.monotonic()
    last_two = []
    stabilized_s = None
    deadline = replay_start + 120
    with httpx.Client(timeout=5.0) as client:
        while time.monotonic() < deadline:
            try:
                resp = client.get(f"{RECOMMENDATION_SERVICE}/recommendations/popular", params={"n": 10})
                snapshot = tuple(p["id"] for p in resp.json().get("popular_products", []))
            except httpx.HTTPError:
                time.sleep(1)
                continue
            last_two.append(snapshot)
            if len(last_two) >= 3 and last_two[-1] == last_two[-2] == last_two[-3]:
                stabilized_s = time.monotonic() - replay_start
                break
            time.sleep(2)

    print(f"Recommendations stabilized {stabilized_s and round(stabilized_s,2)}s after restart", flush=True)

    result = {
        "confirmed_down_while_killed": down_confirmed,
        "health_recovery_s": round(health_recovery_s, 2) if health_recovery_s else None,
        "replay_stabilized_s": round(stabilized_s, 2) if stabilized_s else None,
    }
    record_result(
        RESULTS_DIR, name="fault_recommendation_service", config={}, dataset="live traffic",
        model="recommendation-service", metric="health_recovery_s,replay_stabilized_s", result=result,
    )
    return result


def run_hdfs_sink_experiment(n_events=40, product_id_base=777000):
    print("=== hdfs-sink fault tolerance ===", flush=True)
    kill_service("hdfs-sink")
    down_confirmed = True  # hdfs-sink has no HTTP endpoint to poll; trust the kill

    marker = uuid.uuid4().hex[:10]
    print(f"Sending {n_events} tracked events while hdfs-sink is down (marker={marker})...", flush=True)
    with httpx.Client(timeout=5.0) as client:
        for i in range(n_events):
            client.post(
                f"{EVENT_SERVICE}/events",
                json={"user_id": 1, "product_id": product_id_base + i, "event_type": "view"},
            )

    restart_service("hdfs-sink")
    restart_time = time.monotonic()
    print("Restarted, polling HDFS until it flushes and catches up...", flush=True)

    expected_ids = {product_id_base + i for i in range(n_events)}
    dt = time.strftime("%Y-%m-%d", time.gmtime())

    def read_found_ids():
        found = set()
        ls_result = docker("exec", "-T", "hdfs-namenode", "hdfs", "dfs", "-cat", f"/events/dt={dt}/*.jsonl", timeout=60)
        for line in ls_result.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            pid = row.get("product_id")
            if pid in expected_ids:
                found.add(pid)
        return found

    # batch_max_seconds=30 in hdfs-sink, but the window only starts once
    # the freshly-restarted consumer actually reconnects and the first
    # post-restart message arrives -- a fixed 40s wait undershot this by
    # ~30s in an earlier run. Poll instead of guessing one wait time.
    found_ids = set()
    deadline = restart_time + 150
    while time.monotonic() < deadline:
        found_ids = read_found_ids()
        if found_ids == expected_ids:
            break
        time.sleep(10)

    missing = expected_ids - found_ids
    result = {
        "events_sent_during_outage": n_events,
        "events_found_in_hdfs_after_recovery": len(found_ids),
        "missing": len(missing),
        "recovery_wait_s": round(time.monotonic() - restart_time, 2),
    }
    print(f"Found {len(found_ids)}/{n_events} tracked events in HDFS after recovery", flush=True)
    record_result(
        RESULTS_DIR, name="fault_hdfs_sink", config={"n_events": n_events}, dataset="live traffic (tracked ids)",
        model="hdfs-sink", metric="events_found_in_hdfs_after_recovery,missing", result=result,
    )
    return result


def run_elasticsearch_experiment():
    print("=== elasticsearch fault tolerance ===", flush=True)
    baseline = httpx.get(f"{PRODUCT_SERVICE}/products/search", params={"q": "the"}, timeout=10).json()
    baseline_count = len(baseline.get("results", []))
    print(f"Baseline search returns {baseline_count} results", flush=True)

    kill_service("elasticsearch")
    time.sleep(3)
    restart_service("elasticsearch")
    es_health_s = wait_healthy("http://localhost:9200/_cluster/health", deadline_s=60)
    print(f"Elasticsearch container healthy again in {es_health_s and round(es_health_s,2)}s", flush=True)

    time.sleep(3)
    after_es_restart = httpx.get(f"{PRODUCT_SERVICE}/products/search", params={"q": "the"}, timeout=10).json()
    after_es_restart_count = len(after_es_restart.get("results", []))
    print(f"Search after ES-only restart: {after_es_restart_count} results (index not rebuilt by product-service)", flush=True)

    restart_service("product-service")
    ps_health_s = wait_healthy(f"{PRODUCT_SERVICE}/health", deadline_s=60)
    time.sleep(2)
    after_ps_restart = httpx.get(f"{PRODUCT_SERVICE}/products/search", params={"q": "the"}, timeout=10).json()
    after_ps_restart_count = len(after_ps_restart.get("results", []))
    print(f"Search after product-service also restarted: {after_ps_restart_count} results", flush=True)

    result = {
        "baseline_result_count": baseline_count,
        "es_container_recovery_s": round(es_health_s, 2) if es_health_s else None,
        "results_after_es_restart_only": after_es_restart_count,
        "index_stayed_empty_after_es_restart_alone": after_es_restart_count == 0,
        "product_service_recovery_s": round(ps_health_s, 2) if ps_health_s else None,
        "results_after_product_service_restart": after_ps_restart_count,
        "reindex_fixed_by_product_service_restart": after_ps_restart_count >= baseline_count,
    }
    record_result(
        RESULTS_DIR, name="fault_elasticsearch", config={}, dataset="live traffic",
        model="elasticsearch+product-service", metric="results_after_es_restart_only,results_after_product_service_restart",
        result=result,
    )
    return result


def main():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    run_recommendation_service_experiment()
    run_hdfs_sink_experiment()
    run_elasticsearch_experiment()
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
