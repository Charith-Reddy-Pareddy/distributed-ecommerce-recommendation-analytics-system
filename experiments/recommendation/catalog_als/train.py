"""Trains ALS on the catalog-native synthetic interaction log (not
RetailRocket), so its item ids are the same ones product-service and
recommendation-service use -- unlike jobs/als-training/train_als.py,
which stays as-is on the RetailRocket dataset as a separate, larger
sparsity/weighting study with its own id space.

Runs in local-mode PySpark (no HDFS, no Docker) -- reads the same
interactions_train.parquet / interactions_test.parquet split that
../offline_models.py evaluates popularity/item-CF/content-based
against, and scores with the same experiments/recommendation/metrics.py
functions, so all four models are compared on identical held-out data
with identical metric definitions.

Same ALS hyperparameters as the RetailRocket job (rank=10, maxIter=10,
regParam=0.1, alpha=1.0, seed=42) for a like-for-like comparison of the
algorithm itself, not a hyperparameter search.
"""
import os
import sys
import time
from pathlib import Path

import setuptools  # noqa: F401  -- registers the distutils compat shim pyspark.ml needs on Python 3.12

# Local-mode Spark spawns worker processes with whatever "python3" resolves
# to on PATH, which isn't necessarily this venv's interpreter -- pin both
# driver and worker to the interpreter actually running this script.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.recommendation import ALS

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))
from experiments.common import record_result  # noqa: E402
from experiments.recommendation.metrics import evaluate  # noqa: E402

TRAIN_PATH = REPO_ROOT / "data" / "interactions_train.parquet"
TEST_PATH = REPO_ROOT / "data" / "interactions_test.parquet"
MODEL_OUTPUT_PATH = str(Path(__file__).resolve().parent / "model")
RECS_OUTPUT_PATH = Path(__file__).resolve().parent / "recommendations.parquet"
RESULTS_DIR = REPO_ROOT / "experiments" / "recommendation" / "results"

TOP_K = 10
ALS_PARAMS = dict(rank=10, maxIter=10, regParam=0.1, alpha=1.0, seed=42)


def load_test_actuals(test_path):
    import pandas as pd

    test = pd.read_parquet(test_path)
    return test.groupby("user_id")["product_id"].apply(set).to_dict()


def top_k_unseen(model, train_df, test_users_df, k=TOP_K):
    # ALS doesn't exclude already-seen items, so ask for more than k and
    # drop those before taking the final top-k (same approach as the
    # RetailRocket job's precision_at_k).
    raw_recs = model.recommendForUserSubset(test_users_df, k * 3)
    exploded = raw_recs.selectExpr("user_id", "explode(recommendations) as rec").select(
        "user_id", col("rec.product_id").alias("product_id"), col("rec.rating").alias("score")
    )
    train_pairs = train_df.select("user_id", "product_id")
    unseen = exploded.join(train_pairs, on=["user_id", "product_id"], how="left_anti")

    rows = unseen.orderBy("user_id", col("score").desc()).collect()
    recs = {}
    for row in rows:
        recs.setdefault(row.user_id, [])
        if len(recs[row.user_id]) < k:
            recs[row.user_id].append(row.product_id)
    return recs


def main():
    spark = SparkSession.builder.master("local[*]").appName("catalog-als-training").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    train_df = spark.read.parquet(str(TRAIN_PATH))
    test_df = spark.read.parquet(str(TEST_PATH))
    train_df.cache()
    print(f"[catalog-ALS] Train rows: {train_df.count()}, Test rows: {test_df.count()}", flush=True)

    als = ALS(
        userCol="user_id",
        itemCol="product_id",
        ratingCol="weight",
        implicitPrefs=True,
        coldStartStrategy="drop",
        **ALS_PARAMS,
    )
    model = als.fit(train_df)
    print("[catalog-ALS] Model training complete.", flush=True)

    test_users_df = test_df.select("user_id").distinct()
    n_test_users = test_users_df.count()

    start = time.perf_counter()
    recs = top_k_unseen(model, train_df, test_users_df, TOP_K)
    elapsed = time.perf_counter() - start
    latency_ms_per_user = (elapsed / max(n_test_users, 1)) * 1000

    actuals = load_test_actuals(TEST_PATH)
    metrics = evaluate(recs, actuals, k=TOP_K)
    result = {**metrics, "latency_ms_per_user": latency_ms_per_user}
    print(f"[catalog-ALS] {result}", flush=True)

    model.write().overwrite().save(MODEL_OUTPUT_PATH)
    print(f"[catalog-ALS] Model saved to {MODEL_OUTPUT_PATH}", flush=True)

    all_recs_df = spark.createDataFrame(
        [(user_id, product_id, rank) for user_id, items in recs.items() for rank, product_id in enumerate(items)],
        ["user_id", "product_id", "rank"],
    )
    all_recs_df.write.mode("overwrite").parquet(str(RECS_OUTPUT_PATH))
    print(f"[catalog-ALS] Recommendations written to {RECS_OUTPUT_PATH}", flush=True)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    record_result(
        RESULTS_DIR,
        name="offline_models",
        config={"k": TOP_K, "test_users": n_test_users, **ALS_PARAMS},
        dataset="data/interactions.parquet (synthetic, catalog-native)",
        model="catalog_als",
        metric="precision@10,recall@10,map@10,ndcg@10,latency_ms_per_user",
        result=result,
    )
    spark.stop()


if __name__ == "__main__":
    main()
