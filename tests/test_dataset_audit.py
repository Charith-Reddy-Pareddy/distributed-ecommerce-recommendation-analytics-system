import os
from pathlib import Path

import pytest

from ecommerce_analytics.dataset_audit import audit_amazon_sources


@pytest.mark.integration
def test_real_amazon_sources_have_a_consistent_catalog_map() -> None:
    reviews = os.getenv("AMAZON_REVIEWS_PATH")
    catalog = os.getenv("AMAZON_CATALOG_PATH")
    catalog_map = os.getenv("AMAZON_CATALOG_MAP_PATH")
    if not all(path and Path(path).is_file() for path in (reviews, catalog, catalog_map)):
        pytest.skip("set the real Amazon review, catalog, and catalog-map paths")

    report = audit_amazon_sources(reviews, catalog, catalog_map)

    assert report["review_rows"] > 0
    assert report["invalid_review_rows"] == 0
    assert report["catalog_products"] == report["map_product_ids"]
    assert report["catalog_asins_missing_from_map"] == 0
    assert report["map_asins_missing_from_catalog"] == 0
    assert report["mapped_review_rows"] > 0
    assert report["timestamp_min_utc"] < report["timestamp_max_utc"]
