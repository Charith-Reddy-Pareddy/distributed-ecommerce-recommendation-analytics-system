"""Compare review-rating predictions with the training-mean baseline."""

from __future__ import annotations

import math
from typing import Any, NamedTuple

from ecommerce_analytics.rating_model import RatingPredictions


class RatingMetrics(NamedTuple):
    baseline_mae: float
    baseline_rmse: float
    model_mae: float
    model_rmse: float


def evaluate_rating_predictions(predictions: RatingPredictions) -> RatingMetrics:
    """Calculate MAE and RMSE for both predictions on the same test reviews."""
    from pyspark.ml.evaluation import RegressionEvaluator

    def measure(frame: Any, name: str) -> tuple[float, float]:
        missing = {"rating", "prediction"}.difference(frame.columns)
        if missing:
            cols = ", ".join(sorted(missing))
            raise ValueError(f"{name} predictions are missing columns: {cols}")
        if frame.limit(1).count() == 0:
            raise ValueError(f"{name} predictions contain no test rows")

        mae = RegressionEvaluator(
            labelCol="rating", predictionCol="prediction", metricName="mae"
        ).evaluate(frame)
        rmse = RegressionEvaluator(
            labelCol="rating", predictionCol="prediction", metricName="rmse"
        ).evaluate(frame)
        if not math.isfinite(mae) or not math.isfinite(rmse):
            raise ValueError(f"{name} evaluation produced a non-finite metric")
        return float(mae), float(rmse)

    base_mae, base_rmse = measure(predictions.baseline, "baseline")
    model_mae, model_rmse = measure(predictions.model, "model")
    return RatingMetrics(base_mae, base_rmse, model_mae, model_rmse)
