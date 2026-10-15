import os
from pathlib import Path

import pytest

from ecommerce_analytics.research_report import render_report, summarize_reviews


def test_report_uses_real_amazon_review_data() -> None:
    path = os.environ.get("AMAZON_REVIEWS_PATH")
    if not path or not Path(path).is_file():
        pytest.skip("set AMAZON_REVIEWS_PATH to the real Amazon review CSV")

    summary = summarize_reviews(path)
    report = render_report(summary)

    assert "catalog_reviews.csv" in report
    assert summary["review_rows"] > 0
    assert summary["reviewed_products"] > 0
    assert sum(summary["rating_counts"].values()) == summary["review_rows"]
    assert summary["timestamp_range_utc"][0] <= summary["timestamp_range_utc"][1]
    assert "time-ordered held-out evaluation" in report
    assert "not estimates of sales" in report
