import os
from pathlib import Path

import pytest

from ecommerce_analytics.rating_diagnostics import summarize_rating_errors
from ecommerce_analytics.rating_group_bootstrap import bootstrap_rating_error_groups
from ecommerce_analytics.rating_matrix import build_rating_matrix
from ecommerce_analytics.rating_model import fit_rating_model
from ecommerce_analytics.tabular_rating import build_rating_split


@pytest.mark.integration
def test_group_intervals_match_real_rating_and_month_errors() -> None:
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
        .appName("real-rating-group-bootstrap-check")
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
        matrix = build_rating_matrix(build_rating_split(reviews, cutoff))
        predictions = fit_rating_model(matrix)
        expected = summarize_rating_errors(predictions)

        for key, frame in (
            ("rating", expected.by_rating),
            ("review_month", expected.by_month),
        ):
            got = bootstrap_rating_error_groups(
                predictions, key, resamples=200, seed=42
            )
            want = {row[key]: row for row in frame.collect()}
            assert {item.value for item in got} == set(want)
            for item in got:
                row = want[item.value]
                assert item.rows == row["rows"]
                assert item.baseline_mae == pytest.approx(row["baseline_mae"])
                assert item.model_mae == pytest.approx(row["model_mae"])
                assert item.mae_delta == pytest.approx(row["mae_delta"])
                assert item.baseline_rmse == pytest.approx(row["baseline_rmse"])
                assert item.model_rmse == pytest.approx(row["model_rmse"])
                assert item.rmse_delta == pytest.approx(row["rmse_delta"])
                assert item.mae_ci_low <= item.mae_ci_high
                assert item.rmse_ci_low <= item.rmse_ci_high
    finally:
        spark.stop()
