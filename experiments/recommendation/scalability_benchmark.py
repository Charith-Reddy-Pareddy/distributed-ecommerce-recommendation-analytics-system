"""Point 5: does the CF engine's similar-items lookup actually scale?

similar_items() used to compute cosine similarity against every other
item on every call -- O(n_items) per request. That's fine at this
project's real catalog size (300 items) but would not remotely hold up
at 100K-10M items, per the user's own framing. This measures the
before/after at increasing catalog sizes using the *real* production
RecommendationEngine (services/recommendation-service/app/model.py),
not a reimplementation:

  - "live": similar_items() before the neighbor cache is ever built --
    exercises the original O(n_items) path directly.
  - "cache build": one-time cost of refresh_neighbor_cache() -- O(n^2),
    meant to run periodically in the background, not per-request. This
    is itself a real cost that doesn't disappear, just moves off the
    request path -- reported honestly, including if it gets slow at
    the largest scale tested.
  - "cached": similar_items() once the cache is warm -- O(1) lookup.

Synthetic data only (this project's real catalog is 300 items) --
Zipfian-skewed item popularity per user, same spirit as
scripts/generate_interactions.py but generated directly into the
engine's dicts for speed, not through Kafka/event-service.
"""
import random
import statistics
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))
from scripts.load_app_module import load_app_module  # noqa: E402
from experiments.common import record_result  # noqa: E402

RESULTS_DIR = Path(__file__).resolve().parent / "results"
# Users scale proportionally with catalog size, matching this project's
# own real ratio (2,000 synthetic users / 300 real products) -- holding
# user count fixed while items grow makes each item's interactions
# sparser and *understates* the bottleneck (empty-intersection cosine
# calls short-circuit fast); a catalog that's genuinely grown to serve
# more products presumably also serves proportionally more traffic.
USERS_PER_ITEM_RATIO = 2000 / 300
ITEMS_PER_USER = 15
QUERY_SAMPLE_SIZE = 20
ZIPF_S = 1.1


def zipf_weights(n, rng):
    ranks = list(range(1, n + 1))
    weights = [1.0 / (r**ZIPF_S) for r in ranks]
    total = sum(weights)
    return [w / total for w in weights]


def build_synthetic_engine(n_items, seed=42):
    model = load_app_module("recommendation-service", "model", "recsvc_app")
    engine = model.RecommendationEngine()
    rng = random.Random(seed)

    n_users = round(n_items * USERS_PER_ITEM_RATIO)
    item_ids = list(range(1, n_items + 1))
    weights = zipf_weights(n_items, rng)

    for user_id in range(1, n_users + 1):
        chosen = rng.choices(item_ids, weights=weights, k=ITEMS_PER_USER)
        for product_id in set(chosen):
            event_type = rng.choice(["view", "view", "view", "add_to_cart", "purchase"])
            engine._apply_event({"user_id": user_id, "product_id": product_id, "event_type": event_type})

    return engine, n_users


def percentile(values, p):
    if len(values) < 2:
        return values[0] if values else None
    return statistics.quantiles(values, n=100, method="inclusive")[p - 1]


def time_similar_items_calls(engine, query_items):
    latencies_ms = []
    for product_id in query_items:
        start = time.perf_counter()
        engine.similar_items(product_id, top_n=10)
        latencies_ms.append((time.perf_counter() - start) * 1000)
    return latencies_ms


def run_at_scale(n_items):
    print(f"=== n_items={n_items:,} ===", flush=True)
    build_start = time.perf_counter()
    engine, n_users = build_synthetic_engine(n_items)
    print(f"Built synthetic engine ({n_items:,} items, {n_users:,} users) in {time.perf_counter()-build_start:.1f}s", flush=True)

    rng = random.Random(7)
    query_items = rng.sample(list(engine.item_users.keys()), min(QUERY_SAMPLE_SIZE, len(engine.item_users)))

    live_latencies = time_similar_items_calls(engine, query_items)
    print(f"[live]   p50={percentile(live_latencies,50):.3f}ms p95={percentile(live_latencies,95):.3f}ms "
          f"p99={percentile(live_latencies,99):.3f}ms (n={len(live_latencies)} queries)", flush=True)

    cache_build_start = time.perf_counter()
    engine.refresh_neighbor_cache()
    cache_build_s = time.perf_counter() - cache_build_start
    print(f"[cache build] {cache_build_s:.2f}s for all {n_items:,} items", flush=True)

    cached_latencies = time_similar_items_calls(engine, query_items)
    print(f"[cached] p50={percentile(cached_latencies,50):.3f}ms p95={percentile(cached_latencies,95):.3f}ms "
          f"p99={percentile(cached_latencies,99):.3f}ms (n={len(cached_latencies)} queries)", flush=True)

    result = {
        "n_items": n_items,
        "n_users": n_users,
        "live_latency_ms": {"p50": percentile(live_latencies, 50), "p95": percentile(live_latencies, 95), "p99": percentile(live_latencies, 99)},
        "cache_build_seconds": cache_build_s,
        "cached_latency_ms": {"p50": percentile(cached_latencies, 50), "p95": percentile(cached_latencies, 95), "p99": percentile(cached_latencies, 99)},
        "speedup_p95": (percentile(live_latencies, 95) / percentile(cached_latencies, 95)) if percentile(cached_latencies, 95) else None,
    }

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    record_result(
        RESULTS_DIR,
        name="scalability_benchmark",
        config={"n_items": n_items, "n_users": n_users, "items_per_user": ITEMS_PER_USER, "query_sample_size": QUERY_SAMPLE_SIZE},
        dataset="synthetic (Zipfian item popularity)",
        model="item_cf_similar_items",
        metric="live_p95_ms,cache_build_s,cached_p95_ms,speedup_p95",
        result=result,
    )
    return result


def main():
    # Default scales run in well under a minute total. Because users
    # scale with items (see USERS_PER_ITEM_RATIO), cache-build cost grows
    # faster than a naive O(n^2) estimate from the sparse-data case would
    # suggest -- 300->3,000 items (10x) took 1.06s->142s (~134x) in
    # testing, not ~100x. Pass larger scales explicitly if you have the
    # time budget for it: `python scalability_benchmark.py 10000 50000`.
    scales = [int(x) for x in sys.argv[1:]] or [300, 3000]
    for n_items in scales:
        run_at_scale(n_items)
        print(flush=True)


if __name__ == "__main__":
    main()
