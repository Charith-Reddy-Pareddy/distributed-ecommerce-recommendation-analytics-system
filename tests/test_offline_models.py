from experiments.recommendation.offline_models import build_actuals, eligible_users


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
