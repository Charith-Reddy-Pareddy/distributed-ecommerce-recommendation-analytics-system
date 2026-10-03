import json
from pathlib import Path

from experiments.recommendation.build_interactions import (
    build_interactions,
    load_asin_product_id_map,
    write_asin_product_id_map,
)


def test_build_interactions_maps_asin_to_product_id(tmp_path):
    reviews = tmp_path / "reviews.csv"
    reviews.write_text(
        "user_id,asin,rating,timestamp\n"
        "u1,A1,5.0,100\n"
        "u2,A2,3.0,200\n"
    )
    rows = build_interactions({"A1": 10, "A2": 20}, reviews_path=reviews)
    assert rows == [("u1", 10, 5.0, 100), ("u2", 20, 3.0, 200)]


def test_build_interactions_drops_unmapped_asins(tmp_path):
    reviews = tmp_path / "reviews.csv"
    reviews.write_text(
        "user_id,asin,rating,timestamp\n"
        "u1,A1,5.0,100\n"
        "u2,UNKNOWN,3.0,200\n"
    )
    rows = build_interactions({"A1": 10}, reviews_path=reviews)
    assert rows == [("u1", 10, 5.0, 100)]


def test_build_interactions_empty_reviews(tmp_path):
    reviews = tmp_path / "reviews.csv"
    reviews.write_text("user_id,asin,rating,timestamp\n")
    assert build_interactions({"A1": 10}, reviews_path=reviews) == []


def test_asin_product_id_mapping_round_trips(tmp_path):
    path = tmp_path / "asin_product_id_map.json"
    mapping = {"A2": 20, "A1": 10}
    write_asin_product_id_map(mapping, path)
    assert load_asin_product_id_map(path) == mapping


def test_asin_product_id_map_rejects_duplicate_product_ids(tmp_path):
    import pytest

    with pytest.raises(ValueError, match="unique"):
        write_asin_product_id_map({"A1": 10, "A2": 10}, tmp_path / "mapping.json")


def test_asin_product_id_map_rejects_empty_mapping(tmp_path):
    import pytest

    with pytest.raises(ValueError, match="empty"):
        write_asin_product_id_map({}, tmp_path / "mapping.json")


def test_archived_mapping_matches_recommendation_catalog_snapshot():
    root = Path(__file__).resolve().parents[1]
    mapping = load_asin_product_id_map(root / "data" / "asin_product_id_map.json")
    snapshot = json.loads((root / "data" / "recommendation_catalog_snapshot.json").read_text())
    assert len(mapping) == sum(bool(p.get("asin")) for p in snapshot)
    assert all(mapping[p["asin"]] == p["id"] for p in snapshot if p.get("asin"))
