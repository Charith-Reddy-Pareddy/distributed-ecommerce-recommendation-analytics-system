"""Prepare leak-safe tabular inputs for review rating models."""

from __future__ import annotations

from typing import Any, NamedTuple

from ecommerce_analytics.tabular_rating import RatingSplit

FEATURES = (
    "user_review_count_before",
    "user_mean_rating_before",
    "item_review_count_before",
    "item_mean_rating_before",
    "review_month",
)


class RatingMatrix(NamedTuple):
    train: Any
    test: Any
    features: tuple[str, ...]
    baseline_mean: float


def build_rating_matrix(split: RatingSplit) -> RatingMatrix:
    """Select numeric, point-in-time features and the rating target.

    Missing history means are filled from the training target mean. Identifiers,
    raw timestamps, and the target are not included among the model features.
    """
    from pyspark.sql import functions as F

    required = {*FEATURES, "rating", "timestamp"}
    for name, frame in (("train", split.train), ("test", split.test)):
        missing = required.difference(frame.columns)
        if missing:
            raise ValueError(f"{name} split is missing columns: {', '.join(sorted(missing))}")

    avg = split.train.agg(F.avg("rating").alias("avg")).first()["avg"]
    if avg is None:
        raise ValueError("training split has no valid ratings")
    avg = float(avg)

    def select(frame: Any) -> Any:
        cols = []
        for name in FEATURES:
            if name == "review_month":
                val = F.month(
                    F.to_timestamp(
                        F.from_unixtime(F.floor(F.col("timestamp") / 1000))
                    )
                ).cast("double")
            else:
                val = F.col(name).cast("double")
            if name.endswith("mean_rating_before"):
                val = F.coalesce(val, F.lit(avg))
            cols.append(val.alias(name))
        cols.append(F.col("rating").cast("double").alias("rating"))
        return frame.select(*cols)

    return RatingMatrix(
        train=select(split.train),
        test=select(split.test),
        features=FEATURES,
        baseline_mean=avg,
    )
