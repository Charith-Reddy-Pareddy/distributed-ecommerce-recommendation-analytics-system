"""Percentile bootstrap confidence intervals over per-user metric values.

The sampling unit is the *user*, not the raw metric value -- each
bootstrap iteration resamples which users are included (with
replacement) and recomputes the mean over that resample, since a
model's Precision@10 for one user isn't an independent draw the way a
single coin flip is; the user is.
"""
import random


def bootstrap_ci(values, n_resamples=1000, ci=0.95, seed=42):
    """values: list of per-user metric values (already computed, e.g. via
    metrics.evaluate_per_user). Returns (point_estimate, lower, upper) for
    the mean, via the percentile method.
    """
    n = len(values)
    if n == 0:
        return 0.0, 0.0, 0.0
    point_estimate = sum(values) / n

    rng = random.Random(seed)
    replicate_means = []
    for _ in range(n_resamples):
        resample_sum = sum(values[rng.randrange(n)] for _ in range(n))
        replicate_means.append(resample_sum / n)
    replicate_means.sort()

    alpha = (1 - ci) / 2
    lower = replicate_means[int(alpha * n_resamples)]
    upper = replicate_means[int((1 - alpha) * n_resamples) - 1]
    return point_estimate, lower, upper


def paired_bootstrap_diff(values_a, values_b, n_resamples=1000, ci=0.95, seed=42):
    """values_a/values_b: per-user metric values for two models, aligned
    index-for-index to the *same* users (paired -- same held-out test
    set for both). Returns (mean_diff, lower, upper) for mean(a) -
    mean(b), resampling the same user indices for both each iteration so
    the pairing is preserved rather than treating the two as independent
    samples.
    """
    n = len(values_a)
    if n == 0 or len(values_b) != n:
        raise ValueError("values_a and values_b must be non-empty and the same length (paired)")
    point_diff = sum(values_a) / n - sum(values_b) / n

    rng = random.Random(seed)
    diffs = []
    for _ in range(n_resamples):
        idx = [rng.randrange(n) for _ in range(n)]
        a_mean = sum(values_a[i] for i in idx) / n
        b_mean = sum(values_b[i] for i in idx) / n
        diffs.append(a_mean - b_mean)
    diffs.sort()

    alpha = (1 - ci) / 2
    lower = diffs[int(alpha * n_resamples)]
    upper = diffs[int((1 - alpha) * n_resamples) - 1]
    return point_diff, lower, upper
