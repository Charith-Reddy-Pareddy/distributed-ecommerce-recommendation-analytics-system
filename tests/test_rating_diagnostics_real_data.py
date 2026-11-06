import os
from pathlib import Path

import pytest

from ecommerce_analytics.rating_diagnostics import summarize_rating_errors
from ecommerce_analytics.rating_matrix import build_rating_matrix
from ecommerce_analytics.rating_model import fit_rating_model
from ecommerce_analytics.tabular_rating import build_rating_split


@pytest.mark.integration
def test_rating_error_groups_cover_the_real_test_reviews() -> None:
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
        .appName("real-rating-diagnostics-check")
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
        summary = summarize_rating_errors(predictions)
        expected = matrix.test.count()

        for grouped in (summary.by_rating, summary.by_month):
            assert grouped.agg({"rows": "sum"}).first()[0] == expected
            assert grouped.filter(
                "baseline_mae IS NULL OR model_mae IS NULL "
                "OR baseline_rmse IS NULL OR model_rmse IS NULL"
            ).limit(1).count() == 0
            assert grouped.columns[-2:] == ["mae_delta", "rmse_delta"]
    finally:
        spark.stop()
