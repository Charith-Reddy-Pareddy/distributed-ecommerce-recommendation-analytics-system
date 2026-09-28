import math
import pytest
from experiments.recommendation.hybrid import blend_scores, normalize_scores, rank_scores


def test_normalization_handles_negative_and_constant_scores():
    assert normalize_scores({1: -3, 2: 1}) == {1: 0., 2: 1.}
    assert normalize_scores({1: 7, 2: 7}) == {1: 1., 2: 1.}
    assert normalize_scores({}) == {}


def test_blend_balances_different_scales_and_excludes_seen_and_outside_items():
    scores = blend_scores({1: 1000, 2: 500, 3: 100}, {1: 0.1, 2: 0.2, 3: 0.9, 9: 100},
                          .75, {1, 2, 3}, {1})
    assert set(scores) == {2, 3}
    assert rank_scores(scores) == [3, 2]


def test_endpoints_preserve_component_rankings_and_support():
    first, second = {1: 5, 2: 2}, {3: -2, 2: -1}
    assert rank_scores(blend_scores(first, second, 0, {1, 2, 3}, set())) == [1, 2]
    assert rank_scores(blend_scores(first, second, 1, {1, 2, 3}, set())) == [2, 3]
    assert blend_scores({}, second, 0, {1, 2, 3}, set()) == {}


def test_missing_branch_and_ties_are_deterministic():
    assert rank_scores(blend_scores({2: 1, 1: 1}, {}, .5, {1, 2}, set())) == [1, 2]


@pytest.mark.parametrize('alpha', [-.1, 1.1, math.nan])
def test_invalid_weight_rejected(alpha):
    with pytest.raises(ValueError):
        blend_scores({}, {}, alpha, set(), set())


def test_nonfinite_scores_rejected():
    with pytest.raises(ValueError):
        normalize_scores({1: math.inf})


def test_production_cf_scores_match_recommendation_ranking():
    from scripts.load_app_module import load_app_module
    model = load_app_module('recommendation-service', 'model', 'recsvc_app')
    engine = model.RecommendationEngine()
    for user, item in [(1, 1), (2, 1), (2, 2), (3, 1), (3, 3)]:
        engine._apply_event({'user_id': user, 'product_id': item, 'event_type': 'view'})
    engine.refresh_neighbor_cache()
    assert rank_scores(engine.scores_for_user(1)) == [i for i, _ in engine.recommend_for_user(1)]
    assert 1 not in engine.scores_for_user(1)
