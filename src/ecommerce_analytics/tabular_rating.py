"""Build leakage-safe features for real review-rating regression."""

from __future__ import annotations

from typing import Any, NamedTuple


class RatingSplit(NamedTuple):
    train: Any
    test: Any
    excluded: Any


def build_rating_split(reviews: Any, cutoff_ts: int) -> RatingSplit:
    """Split source review rows in time and build pre-review history features.

    ``rating`` is the target. User and ASIN identify rows but are not model
    features. Training rows use strictly earlier training reviews for history
    features; test rows use training-period aggregates only.
    """
    if type(cutoff_ts) is not int:
        raise ValueError("cutoff_ts must be an integer Unix timestamp")

    required = {"user_id", "asin", "rating", "timestamp"}
    missing = required.difference(reviews.columns)
    if missing:
        raise ValueError(f"reviews is missing columns: {', '.join(sorted(missing))}")

    from pyspark.sql import Window, functions as F

    reviews.sparkSession.conf.set("spark.sql.session.timeZone", "UTC")
    rows = reviews.select(
        F.trim(F.col("user_id").cast("string")).alias("user_id"),
        F.trim(F.col("asin").cast("string")).alias("asin"),
        F.col("rating").cast("double").alias("rating"),
        F.col("timestamp").cast("long").alias("timestamp"),
    )
    valid = F.coalesce(
        F.col("user_id").isNotNull()
        & (F.col("user_id") != "")
        & F.col("asin").isNotNull()
        & (F.col("asin") != "")
        & F.col("timestamp").isNotNull()
        & F.col("rating").isNotNull()
        & ~F.isnan("rating")
        & F.col("rating").between(1.0, 5.0),
        F.lit(False),
    )
    excluded = rows.filter(~valid)
    # Amazon Reviews 2023 stores timestamps in milliseconds, while Spark's
    # from_unixtime expects seconds. Keep the original value for split ordering.
    rows = rows.filter(valid).withColumn(
        "review_month",
        F.month(F.to_timestamp(F.from_unixtime(F.floor(F.col("timestamp") / 1000)))),
    )

    train_src = rows.filter(F.col("timestamp") < cutoff_ts)
    test_src = rows.filter(F.col("timestamp") >= cutoff_ts)

    user_win = (
        Window.partitionBy("user_id")
        .orderBy("timestamp")
        .rangeBetween(Window.unboundedPreceding, -1)
    )
    item_win = (
        Window.partitionBy("asin")
        .orderBy("timestamp")
        .rangeBetween(Window.unboundedPreceding, -1)
    )
    train = (
        train_src.withColumn(
            "user_review_count_before", F.coalesce(F.count("rating").over(user_win), F.lit(0))
        )
        .withColumn("user_mean_rating_before", F.avg("rating").over(user_win))
        .withColumn(
            "item_review_count_before", F.coalesce(F.count("rating").over(item_win), F.lit(0))
        )
        .withColumn("item_mean_rating_before", F.avg("rating").over(item_win))
    )

    user_hist = train_src.groupBy("user_id").agg(
        F.count("rating").alias("user_review_count_before"),
        F.avg("rating").alias("user_mean_rating_before"),
    )
    item_hist = train_src.groupBy("asin").agg(
        F.count("rating").alias("item_review_count_before"),
        F.avg("rating").alias("item_mean_rating_before"),
    )
    test = (
        test_src.join(user_hist, "user_id", "left")
        .join(item_hist, "asin", "left")
        .withColumn(
            "user_review_count_before", F.coalesce("user_review_count_before", F.lit(0))
        )
        .withColumn(
            "item_review_count_before", F.coalesce("item_review_count_before", F.lit(0))
        )
    )
    return RatingSplit(train=train, test=test, excluded=excluded)
