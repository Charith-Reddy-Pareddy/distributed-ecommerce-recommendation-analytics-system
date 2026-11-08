import os
from pathlib import Path

import pytest

from ecommerce_analytics.rating_sensitivity import run_rating_sensitivity


@pytest.mark.integration
def test_sensitivity_runs_use_the_real_reviews_for_each_cutoff_and_seed() -> None:
    candidates = (
        os.environ.get("AMAZON_REVIEWS_PATH"),
        "data/catalog_reviews.csv",
        "data/amazon_reviews_2023_subscription_boxes.jsonl.gz",
    )
    path = next((p for p in candidates if p and Path(p).is_file()), None)
    if path is None:
        pytest.skip(
            "real Amazon Reviews 2023 data is unavailable; set AMAZON_REVIEWS_PATH "
            "to the review CSV or obtain the official category file from "
            "https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023"
        )
    pytest.importorskip("pyspark")

    from pyspark.sql import SparkSession
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("real-rating-sensitivity-check")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        schema = StructType(
            [
                StructField("user_id", StringType()),
                StructField("asin", StringType()),
                StructField("rating", DoubleType()),
                StructField("timestamp", LongType()),
            ]
        )
        if path.endswith(".jsonl.gz"):
            reviews = spark.read.schema(schema).json(path)
        else:
            reviews = spark.read.option("header", "true").schema(schema).csv(path)
        results = run_rating_sensitivity(
            reviews,
            cutoffs=(0.75, 0.8),
            seeds=(23, 42),
            num_trees=3,
            max_depth=3,
            bootstrap_resamples=40,
            bootstrap_seed=99,
        )

        assert len(results) == 4
        assert {(row.cutoff_quantile, row.seed) for row in results} == {
            (0.75, 23),
            (0.75, 42),
            (0.8, 23),
            (0.8, 42),
        }
        for cutoff in (0.75, 0.8):
            rows = [row for row in results if row.cutoff_quantile == cutoff]
            assert len({row.cutoff_timestamp for row in rows}) == 1
            assert len({row.train_rows for row in rows}) == 1
            assert len({row.test_rows for row in rows}) == 1
            assert rows[0].baseline_mae == pytest.approx(rows[1].baseline_mae)
            assert rows[0].baseline_rmse == pytest.approx(rows[1].baseline_rmse)
        for row in results:
            assert row.train_rows > 0
            assert row.test_rows > 0
            assert row.model_mae >= 0
            assert row.model_rmse >= row.model_mae
            assert row.baseline_rmse >= row.baseline_mae
            assert row.bootstrap_resamples == 40
            assert row.bootstrap_seed == 99
            assert row.mae_delta == pytest.approx(row.model_mae - row.baseline_mae)
            assert row.rmse_delta == pytest.approx(row.model_rmse - row.baseline_rmse)
            assert row.mae_ci_low <= row.mae_ci_high
            assert row.rmse_ci_low <= row.rmse_ci_high
    finally:
        spark.stop()
