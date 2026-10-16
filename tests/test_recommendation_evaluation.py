import os
from pathlib import Path

import pytest


@pytest.mark.integration
def test_als_evaluation_on_real_reviews() -> None:
    reviews = os.environ.get("AMAZON_REVIEWS_PATH")
    catalog_map = os.environ.get("AMAZON_CATALOG_MAP_PATH")
    if (
        not reviews
        or not catalog_map
        or not Path(reviews).is_file()
        or not Path(catalog_map).is_file()
    ):
        pytest.skip("set Amazon review and catalog-map paths for the real ALS evaluation")

    from ecommerce_analytics.als_export import load_catalog_map
    from ecommerce_analytics.recommendation_evaluation import evaluate_reviews

    result = evaluate_reviews(
        reviews,
        load_catalog_map(catalog_map),
        top_k=10,
        user_sample_percent=1,
        max_iter=2,
    )

    assert result["eligible_users"] > 0
    for method in ("als", "popularity"):
        assert 0 <= result[method]["recall_at_10"] <= 1
        assert 0 <= result[method]["ndcg_at_10"] <= 1
        assert 0 <= result[method]["catalog_coverage_at_10"] <= 1
    assert result["user_sample_percent"] == 1
    assert result["als_iterations"] == 2
