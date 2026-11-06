"""Summarize rating model errors across real test-review groups."""

from __future__ import annotations

from typing import Any, NamedTuple

from ecommerce_analytics.rating_model import RatingPredictions


class RatingDiagnostics(NamedTuple):
    by_rating: Any
    by_month: Any


def summarize_rating_errors(predictions: RatingPredictions) -> RatingDiagnostics:
    """Compare baseline and model errors by actual rating and review month."""
    from pyspark.sql import functions as F

    required = {"rating", "review_month", "prediction"}
    frames = (("baseline", predictions.baseline), ("model", predictions.model))
    for name, frame in frames:
        missing = required.difference(frame.columns)
        if missing:
            cols = ", ".join(sorted(missing))
            raise ValueError(f"{name} predictions are missing columns: {cols}")
        if frame.limit(1).count() == 0:
            raise ValueError(f"{name} predictions contain no test rows")

    def summarize(key: str) -> Any:
        stats = []
        for name, frame in frames:
            err = F.col("prediction") - F.col("rating")
            stats.append(
                frame.groupBy(key)
                .agg(
                    F.count("*").alias(f"{name}_rows"),
                    F.avg(F.abs(err)).alias(f"{name}_mae"),
                    F.sqrt(F.avg(F.pow(err, 2))).alias(f"{name}_rmse"),
                )
            )

        base, model = stats
        joined = base.join(model, key, "full")
        mismatch = joined.filter(
            F.col("baseline_rows").isNull()
            | F.col("model_rows").isNull()
            | (F.col("baseline_rows") != F.col("model_rows"))
        )
        if mismatch.limit(1).count():
            raise ValueError(f"baseline and model test rows differ by {key}")

        return joined.select(
            key,
            F.col("baseline_rows").alias("rows"),
            "baseline_mae",
            "baseline_rmse",
            "model_mae",
            "model_rmse",
            (F.col("model_mae") - F.col("baseline_mae")).alias("mae_delta"),
            (F.col("model_rmse") - F.col("baseline_rmse")).alias("rmse_delta"),
        )

    return RatingDiagnostics(
        by_rating=summarize("rating"),
        by_month=summarize("review_month"),
    )
