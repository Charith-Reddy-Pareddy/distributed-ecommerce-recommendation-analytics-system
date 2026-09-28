from conftest import load_app_module


def test_popularity_fallback_excludes_seen_items_when_no_neighbors():
    model = load_app_module("recommendation-service", "model", "recsvc_app")
    engine = model.RecommendationEngine()
    for user_id, product_id in [(1, 10), (2, 20)]:
        engine._apply_event({"user_id": user_id, "product_id": product_id, "event_type": "purchase"})
    assert engine.recommend_for_user(1) == [(20, 5.0)]
