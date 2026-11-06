import os
from pathlib import Path

import pytest

from ecommerce_analytics.rating_bootstrap import bootstrap_rating_errors
from ecommerce_analytics.rating_evaluation import evaluate_rating_predictions
from ecommerce_analytics.rating_matrix import build_rating_matrix
from ecommerce_analytics.rating_model import fit_rating_model
from ecommerce_analytics.tabular_rating import build_rating_split


@pytest.mark.integration
def test_paired_bootstrap_uses_the_real_temporal_test_rows() -> None:
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
        .appName("real-rating-bootstrap-check")
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
        cutoff = int(reviews.approxQuantile("timestamp", [0.8], 0.01)[0])
        split = build_rating_split(reviews, cutoff)
        matrix = build_rating_matrix(split)
        predictions = fit_rating_model(matrix)
        result = bootstrap_rating_errors(predictions, resamples=200, seed=42)
        exact = evaluate_rating_predictions(predictions)

        assert result.rows == matrix.test.count()
        assert result.resamples == 200
        assert result.baseline_mae == pytest.approx(exact.baseline_mae, abs=1e-10)
        assert result.model_mae == pytest.approx(exact.model_mae, abs=1e-10)
        assert result.mae_ci_low <= result.mae_ci_high
        assert result.rmse_ci_low <= result.rmse_ci_high
    finally:
        spark.stop()
