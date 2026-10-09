import json
import pytest

from ecommerce_analytics.als_export import (
    export_recommendations,
    load_catalog_map,
)


def test_loads_catalog_map(tmp_path) -> None:
    path = tmp_path / "catalog.json"
    path.write_text('{"B00L0YLRUW": 17}', encoding="utf-8")

    assert load_catalog_map(path) == {"B00L0YLRUW": 17}


def test_rejects_duplicate_catalog_ids(tmp_path) -> None:
    path = tmp_path / "catalog.json"
    path.write_text('{"B00L0YLRUW": 17, "B017T99JPG": 17}', encoding="utf-8")

    with pytest.raises(ValueError, match="unique"):
        load_catalog_map(path)


def test_rejects_empty_catalog_map_before_spark(tmp_path) -> None:
    with pytest.raises(ValueError, match="non-empty"):
        export_recommendations("reviews.csv", {}, tmp_path / "output")


def test_rejects_non_positive_top_k(tmp_path) -> None:
    with pytest.raises(ValueError, match="positive"):
        export_recommendations(
            "reviews.csv", {"B00L0YLRUW": 17}, tmp_path / "output", top_k=0
        )
