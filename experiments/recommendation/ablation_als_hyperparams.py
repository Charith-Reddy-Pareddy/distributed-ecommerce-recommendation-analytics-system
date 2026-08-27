"""Point 5: does ALS's rank / regParam choice affect catalog-ALS quality,
independent of the weighting scheme (RQ1, ablation_weights.py) and the
hybrid blend (hybrid.py)? A small grid on the fixed production-weighted
train/test split, not a full hyperparameter search -- rank=10/regParam=0.1
is the existing RetailRocket job's own baseline (jobs/als-training/train_als.py).
"""
import json
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
from experiments.recommendation.catalog_als.train import (  # noqa: E402
    train_and_evaluate,
    load_test_actuals,
    TRAIN_PATH,
    TEST_PATH,
    TOP_K,
)

RESULTS_DIR = REPO_ROOT / "experiments" / "recommendation" / "results"
RANKS = [5, 10, 20]
REG_PARAMS = [0.01, 0.1, 0.5]


def already_recorded(results_path, model_name):
    if not results_path.exists():
        return False
    with results_path.open() as f:
        return any(json.loads(line)["model"] == model_name for line in f)


def main():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    results_path = RESULTS_DIR / "ablation_als_hyperparams.jsonl"

    spark = SparkSession.builder.master("local[*]").appName("als-hyperparam-ablation").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    train_df = spark.read.parquet(str(TRAIN_PATH))
    test_df = spark.read.parquet(str(TEST_PATH))
    train_df.cache()
    actuals = load_test_actuals(TEST_PATH)

    for rank in RANKS:
        for reg_param in REG_PARAMS:
            model_name = f"catalog_als_rank{rank}_reg{reg_param}"
            if already_recorded(results_path, model_name):
                print(f"[{model_name}] already recorded, skipping", flush=True)
                continue
            params = dict(rank=rank, maxIter=10, regParam=reg_param, alpha=1.0, seed=42)
            _, result, n_test_users = train_and_evaluate(spark, train_df, test_df, actuals, params, k=TOP_K)
            print(f"[{model_name}] {result}", flush=True)
            record_result(
                RESULTS_DIR,
                name="ablation_als_hyperparams",
                config={"k": TOP_K, "test_users": n_test_users, **params},
                dataset="data/interactions.parquet (synthetic, catalog-native)",
                model=model_name,
                metric="precision@10,recall@10,map@10,ndcg@10,latency_ms_per_user",
                result=result,
            )

    spark.stop()


if __name__ == "__main__":
    main()
