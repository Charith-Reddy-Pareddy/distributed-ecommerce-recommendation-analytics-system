import os
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ecommerce_analytics.tabular_rating import build_rating_split


@pytest.mark.integration
def test_rating_split_uses_real_reviews_and_prior_history() -> None:
    paths = [
        os.environ.get("AMAZON_REVIEWS_PATH"),
        "data/catalog_reviews.csv",
        "data/amazon_reviews_2023_subscription_boxes.jsonl.gz",
    ]
    path = next((p for p in paths if p and Path(p).is_file()), None)
    if path is None:
        pytest.skip(
            "real Amazon Reviews 2023 data is unavailable; set AMAZON_REVIEWS_PATH "
            "to the review CSV or download the official Subscription_Boxes.jsonl.gz "
            "file from https://datarepo.eng.ucsd.edu/mcauley_group/data/amazon_2023/"
            "raw/review_categories/Subscription_Boxes.jsonl.gz"
        )
    pytest.importorskip("pyspark")

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("real-tabular-rating-check")
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

        assert split.train.filter(F.col("timestamp") >= cutoff).limit(1).count() == 0
        assert split.test.filter(F.col("timestamp") < cutoff).limit(1).count() == 0
        assert split.train.filter(
            (F.col("user_review_count_before") < 0)
            | (F.col("item_review_count_before") < 0)
        ).limit(1).count() == 0

        month_row = split.train.select("timestamp", "review_month").first()
        expected_month = datetime.fromtimestamp(
            month_row["timestamp"] / 1000, tz=timezone.utc
        ).month
        assert month_row["review_month"] == expected_month

        sample = (
            split.train.select("user_id", "timestamp", "user_review_count_before")
            .dropDuplicates(["user_id", "timestamp"])
            .limit(50)
            .alias("s")
        )
        history = split.train.alias("h")
        counts = (
            sample.join(
                history,
                (F.col("s.user_id") == F.col("h.user_id"))
                & (F.col("h.timestamp") < F.col("s.timestamp")),
                "left",
            )
            .groupBy(
                F.col("s.user_id"),
                F.col("s.timestamp"),
                F.col("s.user_review_count_before"),
            )
            .agg(F.count(F.col("h.timestamp")).alias("expected_count"))
        )
        assert counts.filter(
            F.col("user_review_count_before") != F.col("expected_count")
        ).limit(1).count() == 0
    finally:
        spark.stop()
