from experiments.recommendation.offline_models import build_actuals, content_based_recommend, eligible_users


class FakeEngine:
    def __init__(self, user_item):
        self.user_item = user_item


def test_build_actuals_groups_by_user():
    rows = [("u1", 1, 5.0, 100), ("u1", 2, 4.0, 101), ("u2", 3, 5.0, 102)]
    actuals = build_actuals(rows)
    assert actuals == {"u1": {1, 2}, "u2": {3}}


def test_eligible_users_requires_min_interactions_within_engine():
    actuals = {"u1": {1}, "u2": {2}}
    engine = FakeEngine({"u1": {1: 5.0, 2: 4.0, 3: 3.0, 4: 2.0, 5: 1.0}, "u2": {1: 5.0}})
    assert eligible_users(actuals, engine) == {"u1"}


def test_eligible_users_excludes_users_engine_has_never_seen():
    actuals = {"u1": {1}, "u3": {5}}
    engine = FakeEngine({"u1": {i: 1.0 for i in range(5)}})
    assert eligible_users(actuals, engine) == {"u1"}


def test_content_based_recommend_excludes_already_interacted_items():
    vectors = {1: {"a": 1.0}, 2: {"a": 1.0}, 3: {"b": 1.0}}
    recs = content_based_recommend({1: 5.0}, vectors, top_n=10)
    assert 1 not in recs
    assert recs == [2]  # shares term "a" with the profile; item 3 shares nothing, scores 0


def test_content_based_recommend_empty_when_no_seed_items_have_vectors():
    assert content_based_recommend({99: 5.0}, {1: {"a": 1.0}}, top_n=10) == []


def test_content_based_recommend_respects_top_n():
    vectors = {i: {"a": 1.0} for i in range(20)}
    recs = content_based_recommend({0: 5.0}, vectors, top_n=5)
    assert len(recs) == 5
