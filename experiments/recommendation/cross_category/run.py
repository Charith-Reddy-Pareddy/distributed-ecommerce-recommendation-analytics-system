"""RQ5: does the RQ2 model comparison (popularity vs. item-CF vs.
content-based) generalize to a different Amazon product category, or
is it specific to this project's own demo catalog's particular mix of
categories (electronics, toys and games, musical instruments, cell
phones and accessories)?

Runs the exact same production code as RQ2 (the real
RecommendationEngine via offline_models.build_cf_engine, the same
metrics.py) against a genuinely different, separately-fetched Amazon
category (All_Beauty -- see fetch_category.py) with its own
independently-generated synthetic interaction log, at a comparable
users-per-product density to the main catalog's own generation
(2000 users / 300 products ~= 6.67 users/product; this 201-product
catalog uses ~1340 users to match).

ALS and NeuMF are intentionally out of scope here -- they need
Spark/PyTorch training infrastructure, and this check is specifically
about whether the *comparison* between popularity/CF/content-based
(and CF's margin over the other two) holds on different data, not
about re-deriving every RQ2 number from scratch on a second dataset.

Requires no Docker -- local-mode PySpark isn't even used by this
script (only offline_models.py's Spark-free functions), same as
offline_models.py itself.
"""
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from scripts.generate_interactions import generate_events  # noqa: E402
from experiments.common import record_result  # noqa: E402
from experiments.recommendation.metrics import evaluate  # noqa: E402
from experiments.recommendation.offline_models import (  # noqa: E402
    build_cf_engine,
    build_content_similarity,
    cf_recommendations,
    content_recommendations,
    popularity_recommendations,
)
from experiments.recommendation.split_interactions import split, weighted_interactions  # noqa: E402

CATALOG_PATH = Path(__file__).resolve().parent / "catalog.json"
RESULTS_DIR = Path(__file__).resolve().parent / "results"
TOP_K = 10
WEEKS = 14
SEED = 42

# Same users-per-product density as the main catalog's own generation
# (2000 users / 300 products), scaled to this catalog's actual size.
MAIN_CATALOG_USERS = 2000
MAIN_CATALOG_PRODUCTS = 300


def test_actuals(test):
    return test.groupby("user_id")["product_id"].apply(set).to_dict()


def main():
    n_products = len(json.loads(CATALOG_PATH.read_text()))
    n_users = round(n_products * MAIN_CATALOG_USERS / MAIN_CATALOG_PRODUCTS)
    print(f"Catalog: {n_products} products (All_Beauty) -> generating {n_users} synthetic users", flush=True)

    events = generate_events(n_users=n_users, weeks=WEEKS, seed=SEED, catalog_path=CATALOG_PATH)
    interactions = weighted_interactions(events=events)
    train, test = split(interactions, seed=SEED)
    actuals = test_actuals(test)
    test_users = list(actuals.keys())
    print(f"Train pairs: {len(train):,}  Test pairs: {len(test):,}  Eval users: {len(test_users):,}", flush=True)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    models = {}

    pop_recs, pop_latency = popularity_recommendations(train, test_users, k=TOP_K)
    models["popularity"] = (pop_recs, pop_latency)

    engine = build_cf_engine(train)
    cf_recs, cf_latency = cf_recommendations(engine, test_users, k=TOP_K)
    models["item_cf"] = (cf_recs, cf_latency)

    sim, index, product_ids = build_content_similarity(catalog_path=CATALOG_PATH)
    content_recs, content_latency = content_recommendations(train, test_users, sim, index, product_ids, k=TOP_K)
    models["content_based"] = (content_recs, content_latency)

    for name, (recs, latency) in models.items():
        metrics_result = evaluate(recs, actuals, k=TOP_K)
        result = {**metrics_result, "latency_ms_per_user": latency * 1000}
        print(f"{name}: {result}", flush=True)
        record_result(
            RESULTS_DIR,
            name="cross_category_all_beauty",
            config={"k": TOP_K, "n_products": n_products, "n_users": n_users, "weeks": WEEKS, "seed": SEED},
            dataset="experiments/recommendation/cross_category/catalog.json (All_Beauty, synthetic interactions)",
            model=name,
            metric="precision@10,recall@10,map@10,ndcg@10,latency_ms_per_user",
            result=result,
        )


if __name__ == "__main__":
    main()
