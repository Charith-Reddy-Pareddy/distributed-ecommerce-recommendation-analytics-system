"""Percentile bootstrap confidence intervals over per-user metric values.

The sampling unit is the user. Paired comparisons resample the same user
indices for each model so uncertainty in their difference retains the
within-user pairing.
"""
import math
import random


def _validate(values, n_resamples, ci):
    if isinstance(n_resamples, bool) or not isinstance(n_resamples, int) or n_resamples < 2:
        raise ValueError("n_resamples must be an integer of at least 2")
    if not isinstance(ci, (int, float)) or not math.isfinite(ci) or not 0 < ci < 1:
        raise ValueError("ci must be a finite confidence level strictly between 0 and 1")
    try:
        finite = all(math.isfinite(value) for value in values)
    except (TypeError, ValueError):
        finite = False
    if not finite:
        raise ValueError("bootstrap values must be finite numbers")


def _percentile_interval(replicates, ci):
    replicates.sort()
    tail = (1 - ci) / 2

    def quantile(probability):
        position = probability * (len(replicates) - 1)
        lower = math.floor(position)
        upper = math.ceil(position)
        fraction = position - lower
        return replicates[lower] * (1 - fraction) + replicates[upper] * fraction

    return quantile(tail), quantile(1 - tail)


def bootstrap_ci(values, n_resamples=1000, ci=0.95, seed=42):
    """Return (mean, lower, upper) for the mean via percentile bootstrap."""
    _validate(values, n_resamples, ci)
    n = len(values)
    if n == 0:
        return 0.0, 0.0, 0.0
    point_estimate = sum(values) / n

    rng = random.Random(seed)
    replicate_means = [
        sum(values[rng.randrange(n)] for _ in range(n)) / n
        for _ in range(n_resamples)
    ]
    lower, upper = _percentile_interval(replicate_means, ci)
    return point_estimate, lower, upper


def paired_bootstrap_diff(values_a, values_b, n_resamples=1000, ci=0.95, seed=42):
    """Return the mean difference and percentile CI for paired user values.

    The sign is mean(values_a) - mean(values_b); both vectors must be
    aligned to the same non-empty users in the same order.
    """
    if len(values_a) == 0 or len(values_a) != len(values_b):
        raise ValueError("values_a and values_b must be non-empty and the same length (paired)")
    _validate(values_a, n_resamples, ci)
    _validate(values_b, n_resamples, ci)
    n = len(values_a)
    point_diff = sum(values_a) / n - sum(values_b) / n

    rng = random.Random(seed)
    diffs = []
    for _ in range(n_resamples):
        indices = [rng.randrange(n) for _ in range(n)]
        diffs.append(sum(values_a[i] - values_b[i] for i in indices) / n)
    lower, upper = _percentile_interval(diffs, ci)
    return point_diff, lower, upper
