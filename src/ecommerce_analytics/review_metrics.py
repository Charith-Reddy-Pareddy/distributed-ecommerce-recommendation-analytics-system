"""Aggregate Amazon review ratings from the configured real review export."""

import csv
import os
from collections import Counter
from functools import lru_cache
from pathlib import Path

from ecommerce_analytics.dashboard_metrics import (
    AmazonRatingDistribution,
    RatingBucket,
)


@lru_cache(maxsize=8)
def _rating_counts(
    path: str, modified_ns: int, size: int
) -> tuple[tuple[float, int], ...]:
    counts: Counter[float] = Counter()
    try:
        with Path(path).open(newline="", encoding="utf-8") as stream:
            rows = csv.DictReader(stream)
            if not rows.fieldnames or not {"asin", "rating"}.issubset(rows.fieldnames):
                raise ValueError("Amazon reviews must include asin and rating columns")
            for row in rows:
                try:
                    rating = float(row["rating"])
                except (TypeError, ValueError) as exc:
                    raise ValueError("Amazon review contains an invalid rating") from exc
                if not 1 <= rating <= 5:
                    raise ValueError("Amazon review rating must be between 1 and 5")
                counts[rating] += 1
    except csv.Error as exc:
        raise ValueError("Amazon review CSV is invalid") from exc
    return tuple(sorted(counts.items()))


def amazon_rating_distribution(
    path: str | Path | None = None,
) -> AmazonRatingDistribution:
    """Return counts from real review rows; never substitute sample values."""
    review_path = Path(
        path or os.environ.get("AMAZON_REVIEWS_PATH", "data/catalog_reviews.csv")
    ).resolve()
    stat = review_path.stat()
    counts = _rating_counts(str(review_path), stat.st_mtime_ns, stat.st_size)
    return AmazonRatingDistribution(
        buckets=[RatingBucket(rating=rating, count=count) for rating, count in counts]
    )
