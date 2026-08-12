"""Fires concurrent requests at event-service's /events endpoint for a
fixed duration and reports the measured throughput.

Runs multiple worker processes in parallel, since a single asyncio
process plateaus around 200-300/sec regardless of server capacity --
this way the number reflects server throughput, not client overhead.

Usage (with the stack already up via `docker compose up`):

    pip install httpx
    python scripts/kafka_load_test.py --duration 10 --processes 4 --concurrency 32
"""
import argparse
import asyncio
import multiprocessing
import random
import time

import httpx

EVENT_SERVICE = "http://localhost:8003"
EVENT_TYPES = ["view"] * 6 + ["add_to_cart"] * 3 + ["purchase"] * 1


async def _fire_events(client: httpx.AsyncClient, deadline: float, counters: dict) -> None:
    while time.monotonic() < deadline:
        payload = {
            "user_id": random.randint(1, 10_000),
            "product_id": random.randint(1, 10_000),
            "event_type": random.choice(EVENT_TYPES),
        }
        try:
            resp = await client.post(f"{EVENT_SERVICE}/events", json=payload)
            counters["success" if resp.status_code == 202 else "failure"] += 1
        except httpx.HTTPError:
            counters["failure"] += 1


async def _worker_main(duration: float, concurrency: int) -> dict:
    counters = {"success": 0, "failure": 0}
    deadline = time.monotonic() + duration
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(limits=limits, timeout=10.0) as client:
        await asyncio.gather(*[_fire_events(client, deadline, counters) for _ in range(concurrency)])
    return counters


def _worker_process(duration: float, concurrency: int, result_queue: multiprocessing.Queue) -> None:
    counters = asyncio.run(_worker_main(duration, concurrency))
    result_queue.put(counters)


def run_load_test(duration: float, processes: int, concurrency: int) -> None:
    result_queue: multiprocessing.Queue = multiprocessing.Queue()
    workers = [
        multiprocessing.Process(target=_worker_process, args=(duration, concurrency, result_queue))
        for _ in range(processes)
    ]

    start = time.monotonic()
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    elapsed = time.monotonic() - start

    total_success = 0
    total_failure = 0
    while not result_queue.empty():
        counters = result_queue.get()
        total_success += counters["success"]
        total_failure += counters["failure"]

    rate = total_success / elapsed if elapsed > 0 else 0

    print(f"Duration:            {elapsed:.2f}s")
    print(f"Processes:           {processes}")
    print(f"Concurrency/process: {concurrency}")
    print(f"Total requests:      {total_success + total_failure}")
    print(f"Successful (202):    {total_success}")
    print(f"Failed:              {total_failure}")
    print(f"Measured throughput: {rate:.1f} events/sec")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=10.0, help="seconds to run")
    parser.add_argument("--processes", type=int, default=4, help="parallel worker processes")
    parser.add_argument("--concurrency", type=int, default=32, help="concurrent requests per process")
    args = parser.parse_args()

    run_load_test(args.duration, args.processes, args.concurrency)
