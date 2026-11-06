import os
from pathlib import Path

import pytest

from ecommerce_analytics.rating_evaluation import evaluate_rating_predictions
from ecommerce_analytics.rating_matrix import build_rating_matrix
from ecommerce_analytics.rating_model import fit_rating_model
from ecommerce_analytics.tabular_rating import build_rating_split


@pytest.mark.integration
def test_rating_metrics_compare_model_and_baseline_on_real_reviews() -> None:
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
        .appName("real-rating-evaluation-check")
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
        metrics = evaluate_rating_predictions(predictions)

        for value in metrics:
            assert value >= 0
        assert metrics.baseline_rmse >= metrics.baseline_mae
        assert metrics.model_rmse >= metrics.model_mae
    finally:
        spark.stop()
