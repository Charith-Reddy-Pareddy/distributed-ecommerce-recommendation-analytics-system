"""Bootstrap confidence intervals for the model comparison and hybrid sweep.

Reuses offline_models.py's and hybrid.py's own recommendation/scoring code
(not a reimplementation) so the CIs are computed on the exact same
recommendations those scripts evaluate -- only the statistical layer on
top is new. Point estimates alone can't say whether an observed gap (e.g.
hybrid beating pure CF by a few thousandths of a point) reflects a real
effect or sampling noise from which users happened to land in the test
split; a percentile bootstrap over the per-user metric values answers
that.
"""
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

import setuptools  # noqa: F401,E402  -- distutils compat shim, see catalog_als/train.py
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.ml.recommendation import ALSModel  # noqa: E402

from experiments.common import record_result  # noqa: E402
from experiments.recommendation.bootstrap import bootstrap_ci, paired_bootstrap_diff  # noqa: E402
from experiments.recommendation.metrics import evaluate_per_user  # noqa: E402
from experiments.recommendation.offline_models import (  # noqa: E402
    load_split,
    test_actuals,
    popularity_recommendations,
    build_cf_engine,
    cf_recommendations,
    build_content_similarity,
    content_recommendations,
)
from experiments.recommendation.hybrid import (  # noqa: E402
    CATALOG_ALS_MODEL_PATH,
    ALPHAS,
    als_full_catalog_scores,
    cf_full_catalog_scores,
    content_full_catalog_scores,
    blend_and_rank,
)

TOP_K = 10
N_RESAMPLES = 1000
METRICS = ["precision", "recall", "map", "ndcg"]
RESULTS_DIR = REPO_ROOT / "experiments" / "recommendation" / "results"


def ci_for_recs(recs, actuals, k=TOP_K, n_resamples=N_RESAMPLES, seed=42):
    """Per-user metrics for `recs`, plus a bootstrap CI for each metric's
    mean. Returns (ci_dict, per_user_dict) -- callers that need the raw
    per-user values again later (e.g. for a paired diff) get them back
    without recomputing.
    """
    per_user = evaluate_per_user(recs, actuals, k=k)
    ci = {}
    for metric in METRICS:
        values = [v[metric] for v in per_user.values()]
        point, lower, upper = bootstrap_ci(values, n_resamples=n_resamples, seed=seed)
        ci[metric] = {"point": point, "ci_lower": lower, "ci_upper": upper}
    return ci, per_user


def run_model_comparison(train, test_users, actuals, engine):
    pop_recs, _ = popularity_recommendations(train, test_users)
    cf_recs, _ = cf_recommendations(engine, test_users)
    sim, index, product_ids = build_content_similarity()
    content_recs, _ = content_recommendations(train, test_users, sim, index, product_ids)

    models = {"popularity": pop_recs, "item_cf": cf_recs, "content_based": content_recs}
    per_user_by_model = {}
    for model_name, recs in models.items():
        ci, per_user = ci_for_recs(recs, actuals)
        per_user_by_model[model_name] = per_user
        print(f"[model_comparison] {model_name}: {ci}", flush=True)
        record_result(
            RESULTS_DIR,
            name="bootstrap_ci_model_comparison",
            config={"k": TOP_K, "n_resamples": N_RESAMPLES, "test_users": len(test_users)},
            dataset="data/interactions.parquet (synthetic, catalog-native)",
            model=model_name,
            metric="precision@10,recall@10,map@10,ndcg@10 (95% bootstrap CI)",
            result=ci,
        )
    return per_user_by_model


def run_hybrid_sweep_with_ci(name, scores_a_by_user, scores_b_by_user, test_users, actuals, exclude_by_user):
    per_user_by_alpha = {}
    for alpha in ALPHAS:
        recs = {
            u: blend_and_rank(
                scores_a_by_user.get(u, {}), scores_b_by_user.get(u, {}), alpha, exclude_by_user.get(u, set())
            )
            for u in test_users
        }
        ci, per_user = ci_for_recs(recs, actuals)
        per_user_by_alpha[alpha] = per_user
        print(f"[{name}] alpha={alpha}: {ci}", flush=True)
        record_result(
            RESULTS_DIR,
            name="bootstrap_ci_hybrid_sweep",
            config={"alpha": alpha, "k": TOP_K, "n_resamples": N_RESAMPLES},
            dataset="data/interactions.parquet (synthetic, catalog-native)",
            model=f"{name}_alpha_{alpha}",
            metric="precision@10,recall@10,map@10,ndcg@10 (95% bootstrap CI)",
            result=ci,
        )
    return per_user_by_alpha


def best_alpha_by_precision(per_user_by_alpha):
    non_pure_cf = [a for a in ALPHAS if a != 0.0]

    def mean_precision(alpha):
        values = [v["precision"] for v in per_user_by_alpha[alpha].values()]
        return sum(values) / len(values)

    return max(non_pure_cf, key=mean_precision)


def run_cf_vs_hybrid_paired_diff(cf_per_user, hybrid_per_user, best_alpha):
    shared_users = [u for u in cf_per_user if u in hybrid_per_user]
    out = {}
    for metric in METRICS:
        cf_values = [cf_per_user[u][metric] for u in shared_users]
        hybrid_values = [hybrid_per_user[u][metric] for u in shared_users]
        diff, lower, upper = paired_bootstrap_diff(hybrid_values, cf_values, n_resamples=N_RESAMPLES, seed=42)
        out[metric] = {
            "cf_mean": sum(cf_values) / len(cf_values),
            "hybrid_mean": sum(hybrid_values) / len(hybrid_values),
            "diff": diff,
            "ci_lower": lower,
            "ci_upper": upper,
            "significant": not (lower <= 0 <= upper),
        }
        print(f"[cf_vs_hybrid alpha={best_alpha}] {metric}: {out[metric]}", flush=True)

    record_result(
        RESULTS_DIR,
        name="bootstrap_ci_cf_vs_hybrid",
        config={"best_alpha": best_alpha, "k": TOP_K, "n_resamples": N_RESAMPLES, "n_users": len(shared_users)},
        dataset="data/interactions.parquet (synthetic, catalog-native)",
        model=f"hybrid_cf_als_alpha_{best_alpha}_vs_item_cf",
        metric="precision@10,recall@10,map@10,ndcg@10 (paired bootstrap diff, 95% CI)",
        result=out,
    )
    return out


def main():
    train, test = load_split()
    actuals = test_actuals(test)
    test_users = list(actuals.keys())
    product_ids = sorted(train["product_id"].unique().tolist())

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    engine = build_cf_engine(train)

    print("=== Model comparison bootstrap CIs ===", flush=True)
    model_per_user = run_model_comparison(train, test_users, actuals, engine)

    spark = SparkSession.builder.master("local[*]").appName("bootstrap-ci-hybrid").getOrCreate()
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

    print("=== Hybrid CF+ALS alpha sweep bootstrap CIs ===", flush=True)
    cf_als_per_alpha = run_hybrid_sweep_with_ci(
        "hybrid_cf_als", als_scores, cf_scores, test_users, actuals, exclude_by_user
    )

    print("=== Hybrid CF+content alpha sweep bootstrap CIs ===", flush=True)
    run_hybrid_sweep_with_ci(
        "hybrid_cf_content", content_scores, cf_scores, test_users, actuals, exclude_by_user
    )

    spark.stop()

    best_alpha = best_alpha_by_precision(cf_als_per_alpha)
    print(f"=== Paired bootstrap: item_cf vs hybrid_cf_als(alpha={best_alpha}) ===", flush=True)
    run_cf_vs_hybrid_paired_diff(model_per_user["item_cf"], cf_als_per_alpha[best_alpha], best_alpha)


if __name__ == "__main__":
    main()
