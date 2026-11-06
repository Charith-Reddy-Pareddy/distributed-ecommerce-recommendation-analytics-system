"""Paired error intervals for rating and month groups."""

from __future__ import annotations

import math
import random
from typing import Any, NamedTuple

from ecommerce_analytics.rating_model import RatingPredictions


class RatingGroupInterval(NamedTuple):
    value: Any
    rows: int
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


def bootstrap_rating_error_groups(
    predictions: RatingPredictions,
    group_by: str,
    resamples: int = 1000,
    seed: int = 42,
) -> list[RatingGroupInterval]:
    """Estimate paired 95% intervals within real rating or month groups."""
    if group_by not in {"rating", "review_month"}:
        raise ValueError("group_by must be 'rating' or 'review_month'")
    if type(resamples) is not int or resamples < 1:
        raise ValueError("resamples must be a positive integer")
    if type(seed) is not int:
        raise ValueError("seed must be an integer")

    rows = predictions.paired.select(
        group_by, "rating", "baseline_prediction", "model_prediction"
    ).collect()
    groups: dict[Any, list[tuple[float, float]]] = {}
    for row in rows:
        value = row[group_by]
        rating = float(row["rating"])
        base_err = float(row["baseline_prediction"]) - rating
        model_err = float(row["model_prediction"]) - rating
        if not all(math.isfinite(x) for x in (rating, base_err, model_err)):
            raise ValueError("paired predictions contain non-finite values")
        groups.setdefault(value, []).append((base_err, model_err))
    if not groups:
        raise ValueError("paired predictions contain no test reviews")

    rng = random.Random(seed)
    result = []
    for value in sorted(groups):
        pairs = groups[value]
        n = len(pairs)
        base_abs = [abs(base) for base, _ in pairs]
        model_abs = [abs(model) for _, model in pairs]
        base_sq = [base * base for base, _ in pairs]
        model_sq = [model * model for _, model in pairs]
        base_mae = sum(base_abs) / n
        model_mae = sum(model_abs) / n
        base_rmse = math.sqrt(sum(base_sq) / n)
        model_rmse = math.sqrt(sum(model_sq) / n)
        mae_deltas = []
        rmse_deltas = []

        for _ in range(resamples):
            picks = [rng.randrange(n) for _ in range(n)]
            mae_deltas.append(sum(model_abs[i] - base_abs[i] for i in picks) / n)
            base_sample = math.sqrt(sum(base_sq[i] for i in picks) / n)
            model_sample = math.sqrt(sum(model_sq[i] for i in picks) / n)
            rmse_deltas.append(model_sample - base_sample)

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
        result.append(
            RatingGroupInterval(
                value=value,
                rows=n,
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
        )
    return result
