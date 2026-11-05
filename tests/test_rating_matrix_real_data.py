import os
from pathlib import Path

import pytest

from ecommerce_analytics.rating_matrix import FEATURES, build_rating_matrix
from ecommerce_analytics.tabular_rating import build_rating_split


@pytest.mark.integration
def test_rating_matrix_uses_only_real_point_in_time_fields() -> None:
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
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("real-rating-matrix-check")
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

        assert matrix.features == FEATURES
        assert set(matrix.train.columns) == {*FEATURES, "rating"}
        assert set(matrix.test.columns) == {*FEATURES, "rating"}
        assert matrix.train.filter("rating IS NULL").limit(1).count() == 0
        assert matrix.test.filter("rating IS NULL").limit(1).count() == 0
        null_features = " OR ".join(f"{col} IS NULL" for col in FEATURES)
        assert matrix.train.filter(null_features).limit(1).count() == 0
        assert matrix.test.filter(null_features).limit(1).count() == 0
        expected_months = (
            split.train.select(
                F.month(
                    F.to_timestamp(F.from_unixtime(F.floor(F.col("timestamp") / 1000)))
                ).alias("review_month")
            )
            .groupBy("review_month")
            .count()
        )
        actual_months = matrix.train.groupBy("review_month").count()
        assert expected_months.exceptAll(actual_months).limit(1).count() == 0
        assert actual_months.exceptAll(expected_months).limit(1).count() == 0
        assert matrix.baseline_mean == pytest.approx(split.train.agg({"rating": "avg"}).first()[0])
    finally:
        spark.stop()
