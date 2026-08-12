from conftest import load_app_module

model = load_app_module("recommendation-service", "model", "recsvc_app")
RecommendationEngine = model.RecommendationEngine


def engine_with_events(events):
    engine = RecommendationEngine()
    for user_id, product_id, event_type in events:
        engine._apply_event({"user_id": user_id, "product_id": product_id, "event_type": event_type})
    return engine


def test_event_weights_applied_correctly():
    engine = engine_with_events(
        [
            (1, 100, "view"),
            (1, 100, "view"),
            (1, 100, "add_to_cart"),
            (1, 100, "purchase"),
        ]
    )
    # 2 views (1.0 each) + 1 add_to_cart (3.0) + 1 purchase (5.0) = 10.0
    assert engine.user_item[1][100] == 10.0


def test_unknown_event_type_defaults_to_weight_one():
    engine = engine_with_events([(1, 100, "click")])
    assert engine.user_item[1][100] == 1.0


def test_apply_event_updates_both_indexes():
    engine = engine_with_events([(1, 100, "purchase")])
    assert engine.user_item[1][100] == 5.0
    assert engine.item_users[100][1] == 5.0


def test_cosine_zero_for_items_with_no_shared_users():
    engine = engine_with_events([(1, 100, "view"), (2, 200, "view")])
    score = engine._cosine(engine.item_users[100], engine.item_users[200])
    assert score == 0.0


def test_cosine_one_for_identical_vectors():
    engine = engine_with_events([(1, 100, "purchase"), (1, 200, "purchase")])
    score = engine._cosine(engine.item_users[100], engine.item_users[200])
    assert score == 1.0


def test_similar_items_excludes_the_queried_item():
    engine = engine_with_events([(1, 100, "purchase"), (1, 200, "purchase")])
    results = engine.similar_items(100)
    assert all(pid != 100 for pid, _ in results)


def test_similar_items_ranked_by_score_descending():
    engine = engine_with_events(
        [
            (1, 100, "purchase"),
            (1, 200, "purchase"),
            (2, 100, "purchase"),
            (2, 300, "view"),
        ]
    )
    results = engine.similar_items(100)
    scores = [score for _, score in results]
    assert scores == sorted(scores, reverse=True)


def test_recommend_for_new_user_falls_back_to_popular():
    engine = engine_with_events([(1, 100, "purchase"), (2, 100, "purchase")])
    recs = engine.recommend_for_user(999)
    assert recs == engine.popular_items()


def test_recommend_for_user_excludes_already_interacted_items():
    engine = engine_with_events(
        [
            (1, 100, "purchase"),
            (1, 200, "purchase"),
            (2, 100, "purchase"),
            (2, 300, "purchase"),
        ]
    )
    recs = engine.recommend_for_user(1)
    recommended_ids = {pid for pid, _ in recs}
    assert 100 not in recommended_ids
    assert 200 not in recommended_ids


def test_popular_items_ranked_by_total_weight():
    engine = engine_with_events(
        [
            (1, 100, "view"),
            (2, 100, "view"),
            (1, 200, "purchase"),
        ]
    )
    # product 200: one purchase = 5.0; product 100: two views = 2.0
    top = engine.popular_items(top_n=2)
    assert top[0][0] == 200
    assert top[1][0] == 100
