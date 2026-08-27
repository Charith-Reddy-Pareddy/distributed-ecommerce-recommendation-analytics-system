"""RQ3: does a temporal (realistic) train/test split change recommendation
quality versus the random 80/20 split everywhere else in this project,
and how does "freshness" -- how long after an interaction a model can
act on it -- actually compare between streaming CF and batch ALS?

Temporal split: weeks 1-12 train, week 13 held out as validation
(unused here, reserved for future tuning), week 14 test -- a stricter,
more realistic split than split_interactions.py's random 80/20, since a
production system never gets to peek at "future" interactions when
deciding what to recommend today.
"""
import os
import sys
import time
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

import setuptools  # noqa: F401,E402
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.ml.recommendation import ALS  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

from scripts.load_app_module import load_app_module  # noqa: E402
from experiments.common import record_result  # noqa: E402
from experiments.recommendation.metrics import evaluate  # noqa: E402
from experiments.recommendation.offline_models import build_cf_engine, cf_recommendations  # noqa: E402
from experiments.recommendation.catalog_als.train import train_and_evaluate, ALS_PARAMS  # noqa: E402

INTERACTIONS_PATH = REPO_ROOT / "data" / "interactions.parquet"
RESULTS_DIR = REPO_ROOT / "experiments" / "recommendation" / "results"
TOP_K = 10
TRAIN_WEEKS = range(1, 13)  # weeks 1-12
TEST_WEEK = 14


def temporal_split():
    model = load_app_module("recommendation-service", "model", "recsvc_app")
    event_weights = model.EVENT_WEIGHTS

    events = pd.read_parquet(INTERACTIONS_PATH)
    events["weight"] = events["event_type"].map(event_weights)

    def aggregate(subset):
        return subset.groupby(["user_id", "product_id"], as_index=False)["weight"].sum()

    train = aggregate(events[events["week"].isin(TRAIN_WEEKS)])
    test = aggregate(events[events["week"] == TEST_WEEK])
    return train, test


def measure_cf_update_latency(engine, n_samples=1000):
    """Time _apply_event -- the cost of folding one new interaction into
    the live model, excluding Kafka transport. This is CF's real
    freshness floor: how long after an event it's reflected in
    recommendations.
    """
    sample_event = {"user_id": 999999, "product_id": 1, "event_type": "view"}
    start = time.perf_counter()
    for _ in range(n_samples):
        engine._apply_event(sample_event)
    elapsed = time.perf_counter() - start
    return (elapsed / n_samples) * 1000  # ms


def measure_als_retrain_latency(train_df):
    """Time a full ALS retrain on the current dataset -- batch ALS's
    freshness floor: a new interaction isn't reflected in recommendations
    until the next full retrain finishes (and, in production, until
    hbase-loader reloads HBase afterward -- an additional real cost this
    doesn't measure, since it needs Docker).
    """
    als = ALS(
        userCol="user_id", itemCol="product_id", ratingCol="weight",
        implicitPrefs=True, coldStartStrategy="drop", **ALS_PARAMS,
    )
    start = time.perf_counter()
    als.fit(train_df)
    return time.perf_counter() - start  # seconds


def main():
    train, test = temporal_split()
    actuals = test.groupby("user_id")["product_id"].apply(set).to_dict()
    test_users = list(actuals.keys())
    print(
        f"Temporal split -- train (weeks 1-12): {len(train):,} rows, "
        f"test (week 14): {len(test):,} rows, {len(test_users):,} test users",
        flush=True,
    )

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    engine = build_cf_engine(train)
    cf_recs, _ = cf_recommendations(engine, test_users, k=TOP_K)
    cf_metrics = evaluate(cf_recs, actuals, k=TOP_K)
    print(f"[temporal] item_cf: {cf_metrics}", flush=True)
    record_result(
        RESULTS_DIR,
        name="temporal_eval",
        config={"k": TOP_K, "split": "temporal_week14_test"},
        dataset="data/interactions.parquet (synthetic, catalog-native)",
        model="item_cf_temporal",
        metric="precision@10,recall@10,map@10,ndcg@10",
        result=cf_metrics,
    )

    spark = SparkSession.builder.master("local[*]").appName("temporal-eval").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    train_spark = spark.createDataFrame(train)
    test_spark = spark.createDataFrame(test)
    train_spark.cache()

    _, als_metrics, _ = train_and_evaluate(spark, train_spark, test_spark, actuals, ALS_PARAMS, k=TOP_K)
    print(f"[temporal] catalog_als: {als_metrics}", flush=True)
    record_result(
        RESULTS_DIR,
        name="temporal_eval",
        config={"k": TOP_K, "split": "temporal_week14_test", **ALS_PARAMS},
        dataset="data/interactions.parquet (synthetic, catalog-native)",
        model="catalog_als_temporal",
        metric="precision@10,recall@10,map@10,ndcg@10,latency_ms_per_user",
        result=als_metrics,
    )

    cf_update_ms = measure_cf_update_latency(engine)
    als_retrain_s = measure_als_retrain_latency(train_spark)
    freshness = {
        "cf_update_latency_ms": cf_update_ms,
        "als_retrain_latency_s": als_retrain_s,
        "als_retrain_latency_ms": als_retrain_s * 1000,
        "freshness_gap_factor": (als_retrain_s * 1000) / cf_update_ms if cf_update_ms > 0 else None,
    }
    print(f"[freshness] {freshness}", flush=True)
    record_result(
        RESULTS_DIR,
        name="temporal_eval",
        config={"n_samples_cf": 1000, "train_rows": len(train)},
        dataset="data/interactions.parquet (synthetic, catalog-native)",
        model="freshness_comparison",
        metric="cf_update_latency_ms,als_retrain_latency_s",
        result=freshness,
    )

    spark.stop()


if __name__ == "__main__":
    main()
