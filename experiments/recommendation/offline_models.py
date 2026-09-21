"""Popularity and item-CF, evaluated on the real review interactions
built by build_interactions.py / split_interactions.py.

Item-CF uses the actual production `RecommendationEngine`
(services/recommendation-service/app/model.py) -- not a
reimplementation. Its own `_apply_event` derives weight from a fixed
event_type -> {view,cart,purchase} mapping, which doesn't apply here
(our weight is already a real 1.0-5.0 star rating, not an event type),
so `build_cf_engine` populates the exact same `user_item`/`item_users`
dicts `_apply_event` maintains directly, then hands off to the class's
own real `similar_items`/`recommend_for_user`/`popular_items`/`_cosine`
for everything downstream -- same scoring code the live service runs.

Usage (needs interactions_train.parquet / interactions_test.parquet,
see split_interactions.py):

    python -m experiments.recommendation.offline_models
"""
import json
import sys
import time
from collections import defaultdict
from pathlib import Path

import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))
from scripts.load_app_module import load_app_module  # noqa: E402
from experiments.common import record_result  # noqa: E402
from experiments.recommendation.build_interactions import fetch_asin_to_product_id  # noqa: E402
from experiments.recommendation.metrics import evaluate  # noqa: E402
from experiments.recommendation.text_similarity import add_vectors, build_tfidf, cosine  # noqa: E402

CATALOG_PATH = REPO_ROOT / "data" / "amazon_products.json"

RESULTS_DIR = Path(__file__).resolve().parent / "results"
TOP_K = 10
MIN_INTERACTIONS_FOR_EVAL = 5
# The real review interactions are dense, not sparse: popular items have
# tens of thousands of shared users, making the production engine's
# pairwise _cosine (services/recommendation-service/app/model.py) -- fine
# at this project's live catalog, live traffic volumes -- too slow for a
# full 7,675-item x 7,675-item offline sweep, cached or live (measured:
# didn't finish 20 users' live lookups in 120s). Evaluation here is
# bounded to the top MAX_ITEMS most-interacted-with items and a sample of
# MAX_USERS eligible test users -- real data, real scoring code, just not
# the full catalog -- with the density itself reported as a finding, not
# hidden by silently shrinking scope. See experiments/recommendation/results/.
MAX_ITEMS = 150
MAX_USERS = 150
SAMPLE_SEED = 42


def load_rows(path: Path) -> list[tuple[str, int, float, int]]:
    table = pq.read_table(path)
    cols = table.to_pydict()
    return list(zip(cols["user_id"], cols["product_id"], cols["weight"], cols["timestamp"]))


def build_cf_engine(train_rows: list[tuple[str, int, float, int]]):
    """Populates the real engine's state directly (see module docstring)
    but deliberately skips `refresh_neighbor_cache()` -- its O(n_items^2)
    full-graph precompute was benchmarked (scalability_benchmark.py) at
    357s for 5,000 *sparse* synthetic items; this dataset's 7,675 items
    have real, dense overlap (up to tens of thousands of shared users
    per item), making that pairwise pass far more expensive than the
    sparse-data benchmark predicts. `similar_items()` already falls
    back to `_similar_items_live()` per call when the cache is cold
    (see model.py) -- exactly the live-lookup path the running service
    itself uses for any item added since its last refresh cycle, so
    evaluation here exercises real, uncached production code, just
    without precomputing neighbors for the ~7,675-1 items no eligible
    user's evaluation actually needs.
    """
    item_counts: dict[int, int] = defaultdict(int)
    for _u, product_id, _w, _t in train_rows:
        item_counts[product_id] += 1
    top_items = {pid for pid, _n in sorted(item_counts.items(), key=lambda kv: kv[1], reverse=True)[:MAX_ITEMS]}

    model = load_app_module("recommendation-service", "model", "recsvc_app")
    engine = model.RecommendationEngine()
    for user_id, product_id, weight, _timestamp in train_rows:
        if product_id not in top_items:
            continue
        engine.user_item[user_id][product_id] += weight
        engine.item_users[product_id][user_id] += weight
    return engine, top_items


def build_actuals(test_rows: list[tuple[str, int, float, int]]) -> dict[str, set[int]]:
    actuals: dict[str, set[int]] = defaultdict(set)
    for user_id, product_id, _weight, _timestamp in test_rows:
        actuals[user_id].add(product_id)
    return actuals


def eligible_users(actuals: dict[str, set[int]], engine) -> set[str]:
    """Eligibility is based on what the (item-restricted) engine itself
    knows about each user, not the raw unrestricted train data -- a user
    whose >=5 interactions all fell outside the top-MAX_ITEMS items would
    look like a brand-new user to this engine and just get the popularity
    fallback, which wouldn't really exercise item-CF for them.
    """
    return {u for u in actuals if len(engine.user_item.get(u, {})) >= MIN_INTERACTIONS_FOR_EVAL}


def load_product_texts(top_items: set[int]) -> dict[int, str]:
    """category + brand + description for each top-item's real product,
    joined via the same asin -> product_id mapping build_interactions.py
    uses (product-service's own `asin` field, not JSON array position).
    """
    asin_to_product_id = fetch_asin_to_product_id()
    product_id_to_asin = {pid: asin for asin, pid in asin_to_product_id.items()}
    catalog = {p["asin"]: p for p in json.loads(CATALOG_PATH.read_text())}

    texts: dict[int, str] = {}
    for product_id in top_items:
        asin = product_id_to_asin.get(product_id)
        product = catalog.get(asin) if asin else None
        if product is None:
            continue
        texts[product_id] = f"{product['category']} {product['brand']} {product['description']}"
    return texts


def content_based_recommend(interacted: dict[int, float], vectors: dict[int, dict[str, float]], top_n: int = TOP_K) -> list[int]:
    """One user's recommendations: a TF-IDF profile built from their
    interacted items (weighted by interaction weight), scored by cosine
    against every other candidate item's own TF-IDF vector. Pure
    function of (interacted, vectors) so it's testable without any
    live service or file I/O -- run_content_based just wires real data
    into it.
    """
    seed_ids = [pid for pid in interacted if pid in vectors]
    if not seed_ids:
        return []
    profile = add_vectors([vectors[pid] for pid in seed_ids], [interacted[pid] for pid in seed_ids])
    scored = sorted(
        ((pid, cosine(profile, vec)) for pid, vec in vectors.items() if pid not in interacted),
        key=lambda kv: kv[1],
        reverse=True,
    )
    return [pid for pid, score in scored[:top_n] if score > 0]


def run_content_based(engine, actuals: dict[str, set[int]], users: set[str], top_items: set[int]) -> tuple[dict, float]:
    texts = load_product_texts(top_items)
    vectors = build_tfidf(texts)

    recs = {}
    start = time.perf_counter()
    for u in users:
        recs[u] = content_based_recommend(engine.user_item.get(u, {}), vectors, TOP_K)
    latency_ms = (time.perf_counter() - start) * 1000 / max(len(users), 1)
    result = evaluate(recs, {u: actuals[u] for u in users}, k=TOP_K)
    return result, latency_ms


def run_popularity(engine, actuals: dict[str, set[int]], users: set[str]) -> tuple[dict, float]:
    popular = engine.popular_items(top_n=TOP_K)
    ranked = [pid for pid, _score in popular]
    start = time.perf_counter()
    recs = {u: ranked for u in users}
    latency_ms = (time.perf_counter() - start) * 1000 / max(len(users), 1)
    result = evaluate(recs, {u: actuals[u] for u in users}, k=TOP_K)
    return result, latency_ms


def run_item_cf(engine, actuals: dict[str, set[int]], users: set[str]) -> tuple[dict, float]:
    recs = {}
    start = time.perf_counter()
    for u in users:
        scored = engine.recommend_for_user(u, top_n=TOP_K)
        recs[u] = [pid for pid, _score in scored]
    latency_ms = (time.perf_counter() - start) * 1000 / max(len(users), 1)
    result = evaluate(recs, {u: actuals[u] for u in users}, k=TOP_K)
    return result, latency_ms


def main() -> None:
    import random

    train_rows = load_rows(RESULTS_DIR.parent / "interactions_train.parquet")
    test_rows = load_rows(RESULTS_DIR.parent / "interactions_test.parquet")
    print(f"Train: {len(train_rows)} rows, Test: {len(test_rows)} rows", flush=True)

    engine, top_items = build_cf_engine(train_rows)
    print(f"Engine built on top {len(top_items)} most-interacted-with items", flush=True)
    actuals = build_actuals(test_rows)
    # sorted(), not list(): a set's iteration order is hash-randomized
    # per Python process for str keys (user ids), so sampling from
    # list(a_set) with a fixed random.Random seed still picked a
    # different sample every run -- sorting first makes the input
    # sequence itself deterministic, which is what actually makes the
    # seeded sample reproducible.
    users = sorted(eligible_users(actuals, engine))
    print(f"{len(users)} users eligible (>={MIN_INTERACTIONS_FOR_EVAL} interactions within top items)", flush=True)
    if len(users) > MAX_USERS:
        users = random.Random(SAMPLE_SEED).sample(users, MAX_USERS)
    users = set(users)
    print(f"Evaluating on a sample of {len(users)} users", flush=True)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    runs = [
        ("popularity", lambda: run_popularity(engine, actuals, users)),
        ("item_cf", lambda: run_item_cf(engine, actuals, users)),
        ("content_based", lambda: run_content_based(engine, actuals, users, top_items)),
    ]
    for name, run_fn in runs:
        result, latency_ms = run_fn()
        result["latency_ms_per_request"] = latency_ms
        print(f"{name}: {result}", flush=True)
        record_result(
            RESULTS_DIR,
            name="offline_models",
            config={
                "k": TOP_K,
                "min_interactions_for_eval": MIN_INTERACTIONS_FOR_EVAL,
                "split": "random",
                "max_items": MAX_ITEMS,
                "max_users": MAX_USERS,
                "sample_seed": SAMPLE_SEED,
            },
            dataset="real Amazon reviews (McAuley-Lab/Amazon-Reviews-2023, 5-core), joined to the live catalog, top "
            f"{MAX_ITEMS} most-interacted-with items, {MAX_USERS}-user sample",
            model=name,
            metric="precision,recall,map,ndcg,latency_ms_per_request",
            result=result,
        )


if __name__ == "__main__":
    main()
