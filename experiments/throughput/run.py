"""RQ4-adjacent: how does Kafka ingestion throughput, request latency,
consumer lag, and Kafka's own container resource usage scale from light
to heavy load? Extends scripts/kafka_load_test.py's concurrent-firing
approach with target-rate pacing instead of firing flat-out, per-request
latency, and post-run consumer lag / container stats capture.

Single-process asyncio, not scripts/kafka_load_test.py's multiprocessing
(its own docstring notes a single asyncio process plateaus around
200-300/sec) -- deliberately, after multiprocessing.Queue/Process hung
indefinitely mid-run here (start() never returning), most likely because
a leaked semaphore/shared-memory resource was left in a bad state after
an earlier stuck run's worker processes had to be force-killed. Every
achieved rate observed so far has stayed well under that 200-300/sec
ceiling anyway and degrades as consumer lag climbs -- a pattern
consistent with genuine server-side saturation, not a fixed client cap,
so losing multiprocessing's extra headroom isn't expected to change
what this sweep actually shows.

Requires the stack up (docker compose up, at minimum kafka, zookeeper,
event-service, hdfs-sink, recommendation-service, analytics-service).

Only hdfs-sink's lag is measurable here -- recommendation-service and
analytics-service each use a fresh random consumer group on every
restart and never commit offsets (see
services/recommendation-service/app/kafka_consumer.py), by design, so
there's nothing in Kafka's __consumer_offsets topic for
kafka-consumer-groups.sh to report for them.
"""
import asyncio
import json
import random
import statistics
import subprocess
import sys
import time
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))
from experiments.common import record_result  # noqa: E402

EVENT_SERVICE = "http://localhost:8003"
EVENT_TYPES = ["view"] * 6 + ["add_to_cart"] * 3 + ["purchase"] * 1
RESULTS_DIR = Path(__file__).resolve().parent / "results"

TARGET_RATES = [100, 250, 500, 750, 1000, 1500]
DURATION_S = 15
CONCURRENCY = 256  # matches scripts/kafka_load_test.py's default 8 processes x 32
SETTLE_S = 5  # between rates, so lag from the previous rate drains
REQUEST_TIMEOUT_S = 3.0  # once the server is saturated, a slow request should
# count as a failure quickly, not drag the whole run out near a 10s timeout


async def _paced_worker(rate_per_worker, duration, latencies, counters):
    interval = 1 / rate_per_worker if rate_per_worker > 0 else 0
    deadline = time.monotonic() + duration
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_S) as client:
        while time.monotonic() < deadline:
            start = time.perf_counter()
            payload = {
                "user_id": random.randint(1, 10_000),
                "product_id": random.randint(1, 10_000),
                "event_type": random.choice(EVENT_TYPES),
            }
            try:
                resp = await client.post(f"{EVENT_SERVICE}/events", json=payload)
                latencies.append((time.perf_counter() - start) * 1000)
                counters["success" if resp.status_code == 202 else "failure"] += 1
            except httpx.HTTPError:
                counters["failure"] += 1
            sleep_for = interval - (time.perf_counter() - start)
            if sleep_for > 0:
                await asyncio.sleep(sleep_for)


async def _run_all_workers(rate_per_worker, duration, concurrency, hard_timeout):
    counters = {"success": 0, "failure": 0}
    latencies = []
    tasks = [_paced_worker(rate_per_worker, duration, latencies, counters) for _ in range(concurrency)]
    try:
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=hard_timeout)
        timed_out = False
    except asyncio.TimeoutError:
        # Whatever counters/latencies were accumulated before the timeout
        # are still valid partial data -- keep them rather than discard.
        timed_out = True
    return counters, latencies, timed_out


def percentile(values, p):
    if len(values) < 2:
        return None
    return statistics.quantiles(values, n=100, method="inclusive")[p - 1]


def _kafka_container_id():
    result = subprocess.run(
        ["docker", "compose", "ps", "-q", "kafka"], cwd=REPO_ROOT, capture_output=True, text=True, timeout=15
    )
    return result.stdout.strip() or None


def get_hdfs_sink_lag():
    try:
        result = subprocess.run(
            ["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-consumer-groups.sh",
             "--bootstrap-server", "localhost:9092", "--describe", "--group", "hdfs-sink"],
            cwd=REPO_ROOT, capture_output=True, text=True, timeout=20,
        )
    except subprocess.SubprocessError:
        return None

    total_lag = 0
    found = False
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 6 and parts[0] == "hdfs-sink":
            try:
                total_lag += int(parts[5])
                found = True
            except ValueError:
                pass
    return total_lag if found else None


def get_kafka_container_stats(container_id):
    if not container_id:
        return None
    try:
        result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}\t{{.MemUsage}}", container_id],
            capture_output=True, text=True, timeout=15,
        )
    except subprocess.SubprocessError:
        return None
    line = result.stdout.strip()
    if not line:
        return None
    cpu, mem = line.split("\t")
    return {"cpu": cpu, "mem": mem}


def run_at_rate(target_rate, kafka_container_id, duration=DURATION_S, concurrency=CONCURRENCY):
    rate_per_worker = target_rate / concurrency

    # The worker loop itself is bounded by `duration` and each in-flight
    # request by REQUEST_TIMEOUT_S -- still running well past both means
    # something's actually stuck (e.g. a socket left in a bad state after
    # the host slept), not just running slow. wait_for enforces that hard
    # ceiling itself, rather than trusting an inner timeout to fire.
    hard_timeout = duration + REQUEST_TIMEOUT_S * 2 + 10

    start = time.monotonic()
    counters, all_latencies, timed_out = asyncio.run(
        _run_all_workers(rate_per_worker, duration, concurrency, hard_timeout)
    )
    elapsed = time.monotonic() - start

    return {
        "timed_out": timed_out,
        "target_rate": target_rate,
        "achieved_rate": round(counters["success"] / elapsed, 1) if elapsed > 0 else 0,
        "duration_s": round(elapsed, 2),
        "total_requests": counters["success"] + counters["failure"],
        "success": counters["success"],
        "failure": counters["failure"],
        "latency_p50_ms": percentile(all_latencies, 50),
        "latency_p95_ms": percentile(all_latencies, 95),
        "latency_p99_ms": percentile(all_latencies, 99),
        "hdfs_sink_lag": get_hdfs_sink_lag(),
        "kafka_container_stats": get_kafka_container_stats(kafka_container_id),
    }


def _already_recorded_rates(results_path):
    if not results_path.exists():
        return set()
    with results_path.open() as f:
        return {json.loads(line)["config"]["target_rate"] for line in f}


def main():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    results_path = RESULTS_DIR / "throughput_sweep.jsonl"
    done = _already_recorded_rates(results_path)

    kafka_container_id = _kafka_container_id()
    if not kafka_container_id:
        print("Kafka container not found -- is `docker compose up` running?", flush=True)
        return

    for rate in TARGET_RATES:
        if rate in done:
            print(f"--- target_rate={rate} already recorded, skipping ---", flush=True)
            continue
        print(f"--- target_rate={rate} events/sec ---", flush=True)
        result = run_at_rate(rate, kafka_container_id)
        print(result, flush=True)
        record_result(
            RESULTS_DIR,
            name="throughput_sweep",
            config={
                "target_rate": rate,
                "duration_s": DURATION_S,
                "concurrency": CONCURRENCY,
            },
            dataset="live event-service traffic",
            model="kafka_ingestion",
            metric="achieved_rate,latency_p50_ms,latency_p95_ms,latency_p99_ms,hdfs_sink_lag",
            result=result,
        )
        time.sleep(SETTLE_S)


if __name__ == "__main__":
    main()
