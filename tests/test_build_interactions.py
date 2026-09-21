from experiments.recommendation.build_interactions import build_interactions


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
