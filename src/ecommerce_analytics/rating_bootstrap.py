"""Paired bootstrap intervals for held-out rating prediction errors."""

from __future__ import annotations

import math
import random
from typing import NamedTuple

from ecommerce_analytics.rating_model import RatingPredictions


class RatingBootstrap(NamedTuple):
    rows: int
    resamples: int
    baseline_mae: float
    model_mae: float
    mae_delta: float
    mae_ci_low: float
    mae_ci_high: float
    baseline_rmse: float
    model_rmse: float
    rmse_delta: float
    rmse_ci_low: float
    rmse_ci_high: float


def bootstrap_rating_errors(
    predictions: RatingPredictions,
    resamples: int = 1000,
    seed: int = 42,
) -> RatingBootstrap:
    """Estimate paired percentile intervals by resampling real test reviews."""
    if type(resamples) is not int or resamples < 1:
        raise ValueError("resamples must be a positive integer")
    if type(seed) is not int:
        raise ValueError("seed must be an integer")

    rows = predictions.paired.select(
        "rating", "baseline_prediction", "model_prediction"
    ).collect()
    if not rows:
        raise ValueError("paired predictions contain no test reviews")

    base_abs: list[float] = []
    model_abs: list[float] = []
    base_sq: list[float] = []
    model_sq: list[float] = []
    for row in rows:
        rating = float(row["rating"])
        base_err = float(row["baseline_prediction"]) - rating
        model_err = float(row["model_prediction"]) - rating
        values = (rating, base_err, model_err)
        if not all(math.isfinite(value) for value in values):
            raise ValueError("paired predictions contain non-finite values")
        base_abs.append(abs(base_err))
        model_abs.append(abs(model_err))
        base_sq.append(base_err * base_err)
        model_sq.append(model_err * model_err)

    n = len(rows)
    base_mae = sum(base_abs) / n
    model_mae = sum(model_abs) / n
    base_rmse = math.sqrt(sum(base_sq) / n)
    model_rmse = math.sqrt(sum(model_sq) / n)
    rng = random.Random(seed)
    mae_deltas = []
    rmse_deltas = []

    for _ in range(resamples):
        picks = [rng.randrange(n) for _ in range(n)]
        mae_deltas.append(
            sum(model_abs[i] - base_abs[i] for i in picks) / n
        )
        base_sample_rmse = math.sqrt(sum(base_sq[i] for i in picks) / n)
        model_sample_rmse = math.sqrt(sum(model_sq[i] for i in picks) / n)
        rmse_deltas.append(model_sample_rmse - base_sample_rmse)

    def interval(values: list[float]) -> tuple[float, float]:
        ordered = sorted(values)

        def at(q: float) -> float:
            pos = (len(ordered) - 1) * q
            low = math.floor(pos)
            high = math.ceil(pos)
            return ordered[low] + (ordered[high] - ordered[low]) * (pos - low)

        return at(0.025), at(0.975)

    mae_low, mae_high = interval(mae_deltas)
    rmse_low, rmse_high = interval(rmse_deltas)
    return RatingBootstrap(
        rows=n,
        resamples=resamples,
        baseline_mae=base_mae,
        model_mae=model_mae,
        mae_delta=model_mae - base_mae,
        mae_ci_low=mae_low,
        mae_ci_high=mae_high,
        baseline_rmse=base_rmse,
        model_rmse=model_rmse,
        rmse_delta=model_rmse - base_rmse,
        rmse_ci_low=rmse_low,
        rmse_ci_high=rmse_high,
    )
