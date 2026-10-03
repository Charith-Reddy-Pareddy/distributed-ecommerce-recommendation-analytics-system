"""Confidence-weight transforms for real 1-5 star review interactions."""
import math

WEIGHTING_SCHEMES = ('raw', 'binary', 'squared', 'uniform')
BINARY_POSITIVE_THRESHOLD = 4.0


def transform_rating(rating, scheme):
    if scheme not in WEIGHTING_SCHEMES:
        raise ValueError(f"unknown weighting scheme: {scheme}")
    if isinstance(rating, bool) or not isinstance(rating, (int, float)):
        raise TypeError("rating must be numeric")
    if not math.isfinite(rating) or not 1 <= rating <= 5:
        raise ValueError("rating must be finite and between 1 and 5")
    if scheme == 'raw':
        return float(rating)
    if scheme == 'binary':
        return float(rating >= BINARY_POSITIVE_THRESHOLD)
    if scheme == 'squared':
        return float(rating ** 2)
    return 1.0


def reweight_rows(rows, scheme):
    """Preserve IDs/timestamps while transforming the confidence field."""
    return [(user, item, transform_rating(weight, scheme), timestamp)
            for user, item, weight, timestamp in rows]
