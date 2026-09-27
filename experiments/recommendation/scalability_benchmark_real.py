"""Same question as scalability_benchmark.py -- does the CF engine's
similar-items lookup actually scale as the catalog grows -- but against
real interaction data instead of synthetic Zipfian traffic.

scalability_benchmark.py's synthetic traffic exists to hold the
users-per-item ratio exactly constant across scales, for a clean,
controlled before/after comparison; that's a real, useful thing a
generated load can do that real data can't promise. This script is the
complement: real reviews, real users, real density -- whatever that
density actually is at each scale, not engineered to match a ratio.
Both are kept, not one replacing the other; they answer related but
different questions.

Item selection reuses rank_review_items.py's own method (top-N by real
interaction count, the same approach that selected this project's
7,675-item catalog in the first place -- see CATALOG_REBUILD.md) rather
than fetching anything new: experiments/recommendation/interactions.parquet
already has every real review for this project's real catalog.

Usage:

    python -m experiments.recommendation.scalability_benchmark_real
    python -m experiments.recommendation.scalability_benchmark_real 100 300 1000
"""
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))
from scripts.load_app_module import load_app_module  # noqa: E402
from experiments.common import record_result  # noqa: E402
from experiments.recommendation.scalability_benchmark import percentile, time_similar_items_calls  # noqa: E402

RESULTS_DIR = Path(__file__).resolve().parent / "results"
INTERACTIONS_PATH = Path(__file__).resolve().parent / "interactions.parquet"
QUERY_SAMPLE_SIZE = 20


def load_interactions() -> list[tuple[str, int, float]]:
    table = pq.read_table(INTERACTIONS_PATH, columns=["user_id", "product_id", "weight"])
    cols = table.to_pydict()
    return list(zip(cols["user_id"], cols["product_id"], cols["weight"]))


def build_real_engine(rows: list[tuple[str, int, float]], n_items: int):
    """Top-N items by real interaction count -- the same selection
    method CATALOG_REBUILD.md already validated, applied here to pick
    which slice of the real catalog this scale point measures. Unlike
    build_synthetic_engine(), n_users isn't set by a ratio -- it's
    whatever real user count that item selection actually has.
    """
    item_counts: Counter = Counter()
    for _u, product_id, _w in rows:
        item_counts[product_id] += 1
    top_items = {pid for pid, _n in item_counts.most_common(n_items)}

    model = load_app_module("recommendation-service", "model", "recsvc_app")
    engine = model.RecommendationEngine()
    users: set = set()
    for user_id, product_id, weight in rows:
        if product_id not in top_items:
            continue
        engine.user_item[user_id][product_id] += weight
        engine.item_users[product_id][user_id] += weight
        users.add(user_id)

    return engine, len(users)


def run_at_scale(rows: list[tuple[str, int, float]], n_items: int, seed: int = 7):
    print(f"=== n_items={n_items:,} (real) ===", flush=True)
    build_start = time.perf_counter()
    engine, n_users = build_real_engine(rows, n_items)
    actual_items = len(engine.item_users)
    print(
        f"Built real engine ({actual_items:,} items, {n_users:,} real users) in "
        f"{time.perf_counter()-build_start:.1f}s",
        flush=True,
    )

    import random

    rng = random.Random(seed)
    query_items = rng.sample(list(engine.item_users.keys()), min(QUERY_SAMPLE_SIZE, actual_items))

    live_latencies = time_similar_items_calls(engine, query_items)
    print(f"[live]   p50={percentile(live_latencies,50):.3f}ms p95={percentile(live_latencies,95):.3f}ms "
          f"p99={percentile(live_latencies,99):.3f}ms (n={len(live_latencies)} queries)", flush=True)

    cache_build_start = time.perf_counter()
    engine.refresh_neighbor_cache()
    cache_build_s = time.perf_counter() - cache_build_start
    print(f"[cache build] {cache_build_s:.2f}s for all {actual_items:,} items", flush=True)

    cached_latencies = time_similar_items_calls(engine, query_items)
    print(f"[cached] p50={percentile(cached_latencies,50):.3f}ms p95={percentile(cached_latencies,95):.3f}ms "
          f"p99={percentile(cached_latencies,99):.3f}ms (n={len(cached_latencies)} queries)", flush=True)

    result = {
        "n_items": actual_items,
        "n_users": n_users,
        "live_latency_ms": {"p50": percentile(live_latencies, 50), "p95": percentile(live_latencies, 95), "p99": percentile(live_latencies, 99)},
        "cache_build_seconds": cache_build_s,
        "cached_latency_ms": {"p50": percentile(cached_latencies, 50), "p95": percentile(cached_latencies, 95), "p99": percentile(cached_latencies, 99)},
        "speedup_p95": (percentile(live_latencies, 95) / percentile(cached_latencies, 95)) if percentile(cached_latencies, 95) else None,
    }

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    record_result(
        RESULTS_DIR,
        name="scalability_benchmark_real",
        config={"n_items_requested": n_items, "n_items_actual": actual_items, "n_users": n_users, "query_sample_size": QUERY_SAMPLE_SIZE},
        dataset="real Amazon reviews (McAuley-Lab/Amazon-Reviews-2023, 5-core), joined to the live catalog, top-N most-interacted items",
        model="item_cf_similar_items",
        metric="live_p95_ms,cache_build_s,cached_p95_ms,speedup_p95",
        result=result,
    )
    return result


def main():
    print("Loading real interactions...", flush=True)
    rows = load_interactions()
    print(f"{len(rows):,} real interaction rows loaded\n", flush=True)

    # Conservative defaults: Day 4's offline model comparison already
    # found live/cached similar_items() struggling well below this
    # dataset's full 7,675-item scale (didn't finish 20 users' live
    # lookups at full scale in 120s -- see OFFLINE_MODELS_FINDINGS.md).
    # These scales are chosen to actually finish; pass larger ones
    # explicitly if you have the time budget: `... 3000 7675`.
    scales = [int(x) for x in sys.argv[1:]] or [100, 300, 1000]
    for n_items in scales:
        run_at_scale(rows, n_items)
        print(flush=True)


if __name__ == "__main__":
    main()
