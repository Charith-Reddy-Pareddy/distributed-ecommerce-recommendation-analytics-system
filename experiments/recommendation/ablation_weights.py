"""RQ1: does the view/cart/purchase event weighting actually matter?

Re-aggregates the raw interaction log under four weighting schemes and
retrains/re-evaluates both item-CF and catalog-ALS on each -- not just
the production 1/3/5 weights, so we can see whether recommendation
quality is sensitive to this choice or fairly flat across it.
"""
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

import setuptools  # noqa: F401,E402
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession  # noqa: E402

from experiments.common import record_result  # noqa: E402
from experiments.recommendation.metrics import evaluate  # noqa: E402
from experiments.recommendation.split_interactions import weighted_interactions, split  # noqa: E402
from experiments.recommendation.offline_models import build_cf_engine, cf_recommendations  # noqa: E402
from experiments.recommendation.catalog_als.train import train_and_evaluate, ALS_PARAMS  # noqa: E402

RESULTS_DIR = REPO_ROOT / "experiments" / "recommendation" / "results"
TOP_K = 10

WEIGHT_SCHEMES = {
    "uniform_1_1_1": {"view": 1.0, "add_to_cart": 1.0, "purchase": 1.0},
    "linear_1_2_3": {"view": 1.0, "add_to_cart": 2.0, "purchase": 3.0},
    "production_1_3_5": {"view": 1.0, "add_to_cart": 3.0, "purchase": 5.0},
    "steep_1_5_10": {"view": 1.0, "add_to_cart": 5.0, "purchase": 10.0},
}


def main():
    spark = SparkSession.builder.master("local[*]").appName("weighting-ablation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    for scheme_name, weights in WEIGHT_SCHEMES.items():
        interactions = weighted_interactions(weights)
        train, test = split(interactions)
        actuals = test.groupby("user_id")["product_id"].apply(set).to_dict()
        test_users = list(actuals.keys())

        engine = build_cf_engine(train)
        cf_recs, _ = cf_recommendations(engine, test_users, k=TOP_K)
        cf_metrics = evaluate(cf_recs, actuals, k=TOP_K)
        print(f"[weights={scheme_name}] item_cf: {cf_metrics}", flush=True)
        record_result(
            RESULTS_DIR,
            name="ablation_weights",
            config={"scheme": scheme_name, "weights": weights, "k": TOP_K},
            dataset="data/interactions.parquet (synthetic, catalog-native)",
            model=f"item_cf_{scheme_name}",
            metric="precision@10,recall@10,map@10,ndcg@10",
            result=cf_metrics,
        )

        train_spark = spark.createDataFrame(train)
        test_spark = spark.createDataFrame(test)
        train_spark.cache()
        _, als_result, _ = train_and_evaluate(spark, train_spark, test_spark, actuals, ALS_PARAMS, k=TOP_K)
        print(f"[weights={scheme_name}] catalog_als: {als_result}", flush=True)
        record_result(
            RESULTS_DIR,
            name="ablation_weights",
            config={"scheme": scheme_name, "weights": weights, "k": TOP_K, **ALS_PARAMS},
            dataset="data/interactions.parquet (synthetic, catalog-native)",
            model=f"catalog_als_{scheme_name}",
            metric="precision@10,recall@10,map@10,ndcg@10,latency_ms_per_user",
            result=als_result,
        )
        train_spark.unpersist()

    spark.stop()


if __name__ == "__main__":
    main()
