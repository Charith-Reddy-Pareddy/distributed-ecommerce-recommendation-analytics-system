"""Hybrid CF+ALS and CF+content scoring: Score(i) = alpha*a(i) + (1-alpha)*b(i).

Both component models are normalized per user to [0,1] (min-max) before
blending -- ALS's implicit-feedback confidence scores and CF's
weighted-cosine sums live on different, unbounded scales, so alpha is
only a meaningful mixing weight once both are on the same footing.

Scores the *entire* catalog per user (300 items, cheap) rather than a
pre-truncated candidate set from either model alone, so the blend isn't
biased toward whichever model would have picked candidates first.
"""
import os
import sys
from pathlib import Path

import numpy as np

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

import setuptools  # noqa: F401,E402  -- distutils compat shim, see catalog_als/train.py
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.ml.recommendation import ALSModel  # noqa: E402

from scripts.load_app_module import load_app_module  # noqa: E402
from experiments.common import record_result  # noqa: E402
from experiments.recommendation.metrics import evaluate  # noqa: E402
from experiments.recommendation.offline_models import (  # noqa: E402
    load_split,
    test_actuals,
    build_content_similarity,
)

CATALOG_ALS_MODEL_PATH = str(REPO_ROOT / "experiments" / "recommendation" / "catalog_als" / "model")
RESULTS_DIR = REPO_ROOT / "experiments" / "recommendation" / "results"
TOP_K = 10
ALPHAS = [0.0, 0.25, 0.5, 0.75, 1.0]


def build_cf_engine(train):
    model = load_app_module("recommendation-service", "model", "recsvc_app")
    engine = model.RecommendationEngine()
    for row in train.itertuples(index=False):
        engine.user_item[row.user_id][row.product_id] += row.weight
        engine.item_users[row.product_id][row.user_id] += row.weight
    return engine


def minmax_normalize(scores):
    if not scores:
        return {}
    values = list(scores.values())
    lo, hi = min(values), max(values)
    if hi == lo:
        return {k: 0.0 for k in scores}
    return {k: (v - lo) / (hi - lo) for k, v in scores.items()}


def cf_full_catalog_scores(engine, user_id, product_ids):
    """Score every catalog item via CF, not just recommend_for_user's own
    top-20-per-seed shortlist -- needed so the blend isn't pre-filtered by
    CF's own candidate selection before ALS/content ever get a say.
    """
    interacted = dict(engine.user_item.get(user_id, {}))
    if not interacted:
        totals = {pid: sum(users.values()) for pid, users in engine.item_users.items()}
        return {pid: totals.get(pid, 0.0) for pid in product_ids}

    scores = {pid: 0.0 for pid in product_ids if pid not in interacted}
    for seed_id, weight in interacted.items():
        target = engine.item_users.get(seed_id)
        if not target:
            continue
        for pid in scores:
            other = engine.item_users.get(pid)
            if other:
                scores[pid] += engine._cosine(target, other) * weight
    return scores


def content_full_catalog_scores(sim, index, product_ids, interacted):
    scores = {pid: 0.0 for pid in product_ids if pid not in interacted}
    if not interacted:
        return scores
    vec = np.zeros(len(product_ids))
    for pid, weight in interacted.items():
        if pid in index:
            vec += sim[index[pid]] * weight
    for pid in scores:
        if pid in index:
            scores[pid] = vec[index[pid]]
    return scores


def als_full_catalog_scores(spark, model, user_ids, n_items):
    users_df = spark.createDataFrame([(u,) for u in user_ids], ["user_id"])
    raw = model.recommendForUserSubset(users_df, n_items)
    exploded = raw.selectExpr("user_id", "explode(recommendations) as rec").select(
        "user_id", "rec.product_id", "rec.rating"
    )
    per_user = {}
    for row in exploded.collect():
        per_user.setdefault(row.user_id, {})[row.product_id] = row.rating
    return per_user


def blend_and_rank(scores_a, scores_b, alpha, exclude, k=TOP_K):
    norm_a = minmax_normalize(scores_a)
    norm_b = minmax_normalize(scores_b)
    keys = (set(norm_a) | set(norm_b)) - exclude
    combined = {pid: alpha * norm_a.get(pid, 0.0) + (1 - alpha) * norm_b.get(pid, 0.0) for pid in keys}
    ranked = sorted(combined.items(), key=lambda x: x[1], reverse=True)
    return [pid for pid, _ in ranked[:k]]


def run_sweep(name, scores_a_by_user, scores_b_by_user, test_users, actuals, exclude_by_user):
    for alpha in ALPHAS:
        recs = {
            u: blend_and_rank(
                scores_a_by_user.get(u, {}), scores_b_by_user.get(u, {}), alpha, exclude_by_user.get(u, set())
            )
            for u in test_users
        }
        metrics = evaluate(recs, actuals, k=TOP_K)
        print(f"[{name}] alpha={alpha}: {metrics}", flush=True)
        record_result(
            RESULTS_DIR,
            name=name,
            config={"alpha": alpha, "k": TOP_K},
            dataset="data/interactions.parquet (synthetic, catalog-native)",
            model=f"{name}_alpha_{alpha}",
            metric="precision@10,recall@10,map@10,ndcg@10",
            result=metrics,
        )


def main():
    train, test = load_split()
    actuals = test_actuals(test)
    test_users = list(actuals.keys())
    product_ids = sorted(train["product_id"].unique().tolist())

    engine = build_cf_engine(train)

    spark = SparkSession.builder.master("local[*]").appName("hybrid-eval").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    als_model = ALSModel.load(CATALOG_ALS_MODEL_PATH)

    print(f"Scoring ALS over full catalog for {len(test_users)} users...", flush=True)
    als_scores = als_full_catalog_scores(spark, als_model, test_users, len(product_ids))

    print("Scoring CF over full catalog...", flush=True)
    cf_scores = {u: cf_full_catalog_scores(engine, u, product_ids) for u in test_users}

    print("Scoring content-based over full catalog...", flush=True)
    sim, index, content_product_ids = build_content_similarity()
    user_items = (
        train.groupby("user_id")
        .apply(lambda df: dict(zip(df["product_id"], df["weight"])), include_groups=False)
        .to_dict()
    )
    content_scores = {
        u: content_full_catalog_scores(sim, index, content_product_ids, user_items.get(u, {}))
        for u in test_users
    }

    exclude_by_user = {u: set(engine.user_item.get(u, {}).keys()) for u in test_users}

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    run_sweep("hybrid_cf_als", als_scores, cf_scores, test_users, actuals, exclude_by_user)
    run_sweep("hybrid_cf_content", content_scores, cf_scores, test_users, actuals, exclude_by_user)

    spark.stop()


if __name__ == "__main__":
    main()
