"""Reruns the model comparison and hybrid CF+ALS sweep at 5 random
seeds, regenerating a fresh synthetic interaction log and train/test
split each time, and reports mean +/- std across seeds instead of a
single point estimate.

This is a different question from bootstrap_ci.py's confidence
intervals: bootstrapping resamples the *same* fixed test-user split
generated at seed=42, so it captures sampling variance within that one
population. This script regenerates an entirely different simulated
user population per seed, so it captures variance from *which*
population got generated -- the other axis of uncertainty in a
result built on synthetic data.

Everything here runs in memory. It never writes to data/interactions*
.parquet or experiments/recommendation/catalog_als/model/, which stay
pinned to the canonical seed=42 run the rest of the project (docs, the
live hybrid pipeline, bootstrap_ci.py) is built on.
"""
import os
import statistics
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

import setuptools  # noqa: F401,E402  -- distutils compat shim, see catalog_als/train.py
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession  # noqa: E402

from scripts.generate_interactions import generate_events  # noqa: E402
from experiments.common import record_result  # noqa: E402
from experiments.recommendation.metrics import evaluate  # noqa: E402
from experiments.recommendation.offline_models import (  # noqa: E402
    popularity_recommendations,
    build_cf_engine,
    cf_recommendations,
    build_content_similarity,
    content_recommendations,
)
from experiments.recommendation.split_interactions import weighted_interactions, split  # noqa: E402
from experiments.recommendation.catalog_als.train import (  # noqa: E402
    train_and_evaluate as als_train_and_evaluate,
    ALS_PARAMS,
)
from experiments.recommendation.hybrid import (  # noqa: E402
    als_full_catalog_scores,
    cf_full_catalog_scores,
    blend_and_rank,
)

SEEDS = [1, 7, 21, 42, 100]
N_USERS = 2000
WEEKS = 14
TOP_K = 10
HYBRID_ALPHAS = [0.0, 0.25, 0.5, 0.75, 1.0]
METRICS = ["precision", "recall", "map", "ndcg"]
RESULTS_DIR = REPO_ROOT / "experiments" / "recommendation" / "results"


def test_actuals(test):
    return test.groupby("user_id")["product_id"].apply(set).to_dict()


def run_one_seed(spark, seed, sim, index, content_product_ids):
    print(f"--- seed={seed}: generating interactions ---", flush=True)
    events = generate_events(N_USERS, WEEKS, seed)
    interactions = weighted_interactions(events=events)
    train, test = split(interactions, seed=seed)
    actuals = test_actuals(test)
    test_users = list(actuals.keys())
    product_ids = sorted(train["product_id"].unique().tolist())
    print(f"    {len(train):,} train rows, {len(test):,} test rows, {len(test_users):,} test users", flush=True)

    model_results = {}

    pop_recs, _ = popularity_recommendations(train, test_users, k=TOP_K)
    model_results["popularity"] = evaluate(pop_recs, actuals, k=TOP_K)

    engine = build_cf_engine(train)
    cf_recs, _ = cf_recommendations(engine, test_users, k=TOP_K)
    model_results["item_cf"] = evaluate(cf_recs, actuals, k=TOP_K)

    content_recs, _ = content_recommendations(train, test_users, sim, index, content_product_ids, k=TOP_K)
    model_results["content_based"] = evaluate(content_recs, actuals, k=TOP_K)

    train_spark = spark.createDataFrame(train[["user_id", "product_id", "weight"]])
    test_spark = spark.createDataFrame(test[["user_id", "product_id", "weight"]])
    als_model, als_result, _ = als_train_and_evaluate(
        spark, train_spark, test_spark, actuals, als_params=ALS_PARAMS, k=TOP_K
    )
    model_results["catalog_als"] = als_result

    print("    scoring hybrid CF+ALS alpha sweep...", flush=True)
    als_scores = als_full_catalog_scores(spark, als_model, test_users, len(product_ids))
    cf_scores = {u: cf_full_catalog_scores(engine, u, product_ids) for u in test_users}
    exclude_by_user = {u: set(engine.user_item.get(u, {}).keys()) for u in test_users}

    hybrid_results = {}
    for alpha in HYBRID_ALPHAS:
        recs = {
            u: blend_and_rank(als_scores.get(u, {}), cf_scores.get(u, {}), alpha, exclude_by_user.get(u, set()))
            for u in test_users
        }
        hybrid_results[alpha] = evaluate(recs, actuals, k=TOP_K)

    print(f"--- seed={seed} done ---", flush=True)
    return model_results, hybrid_results


def summarize(name, per_seed_metrics, config_extra=None):
    """per_seed_metrics: one evaluate()-shaped dict per seed. Records mean
    +/- sample std (n-1) across the SEEDS run, plus the raw per-seed
    values for anyone who wants to recompute a different aggregate.
    """
    summary = {}
    for metric in METRICS:
        values = [m[metric] for m in per_seed_metrics]
        mean = statistics.mean(values)
        std = statistics.stdev(values) if len(values) > 1 else 0.0
        summary[metric] = {"mean": mean, "std": std, "values": values}

    record_result(
        RESULTS_DIR,
        name="multi_seed_summary",
        config={"seeds": SEEDS, "k": TOP_K, **(config_extra or {})},
        dataset="synthetic, catalog-native, regenerated per seed (not data/interactions.parquet)",
        model=name,
        metric="precision@10,recall@10,map@10,ndcg@10 (mean +/- std across 5 seeds)",
        result=summary,
    )
    print(f"[multi_seed_summary] {name}: {summary}", flush=True)
    return summary


def main():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    print("Building content-based similarity matrix (catalog-only, seed-independent)...", flush=True)
    sim, index, content_product_ids = build_content_similarity()

    spark = SparkSession.builder.master("local[*]").appName("multi-seed-rerun").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    per_seed_model_results = {"popularity": [], "item_cf": [], "content_based": [], "catalog_als": []}
    per_seed_hybrid_results = {alpha: [] for alpha in HYBRID_ALPHAS}

    for seed in SEEDS:
        model_results, hybrid_results = run_one_seed(spark, seed, sim, index, content_product_ids)
        for model_name, metrics in model_results.items():
            per_seed_model_results[model_name].append(metrics)
        for alpha, metrics in hybrid_results.items():
            per_seed_hybrid_results[alpha].append(metrics)

    spark.stop()

    print("=== Model comparison: mean +/- std across 5 seeds ===", flush=True)
    for model_name, per_seed in per_seed_model_results.items():
        summarize(model_name, per_seed)

    print("=== Hybrid CF+ALS alpha sweep: mean +/- std across 5 seeds ===", flush=True)
    for alpha, per_seed in per_seed_hybrid_results.items():
        summarize(f"hybrid_cf_als_alpha_{alpha}", per_seed, config_extra={"alpha": alpha})


if __name__ == "__main__":
    main()
