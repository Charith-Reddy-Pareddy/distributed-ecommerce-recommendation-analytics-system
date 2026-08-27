from conftest import load_app_module

hybrid = load_app_module("recommendation-service", "hybrid", "recsvc_app")


def test_pure_cf_when_alpha_zero():
    cf = [(1, 10.0), (2, 5.0), (3, 1.0)]
    als = [(4, 100.0), (5, 50.0)]
    result = hybrid.blend(cf, als, alpha=0.0, top_n=3)
    ranked_ids = [item_id for item_id, _ in result]
    assert ranked_ids == [1, 2, 3]


def test_pure_als_when_alpha_one():
    cf = [(1, 10.0), (2, 5.0)]
    als = [(4, 100.0), (5, 50.0), (6, 1.0)]
    result = hybrid.blend(cf, als, alpha=1.0, top_n=3)
    ranked_ids = [item_id for item_id, _ in result]
    assert ranked_ids == [4, 5, 6]


def test_blend_combines_both_models():
    # Item 1 is CF's top pick but ALS ignores it (score 0); item 2 is
    # ALS's top pick but CF ignores it. A 50/50 blend should rank
    # whichever item both models agree on, if any, above one only one
    # model likes -- here neither is favored by both, so the higher
    # single-model normalized score should still surface.
    cf = [(1, 10.0), (2, 1.0)]
    als = [(2, 10.0), (1, 1.0)]
    result = hybrid.blend(cf, als, alpha=0.5, top_n=2)
    scores = dict(result)
    # Both items are each other's near-mirror, so their blended scores
    # should end up close to each other -- neither model dominates.
    assert abs(scores[1] - scores[2]) < 0.2


def test_missing_item_scores_as_zero_in_other_model():
    cf = [(1, 10.0)]
    als = [(2, 10.0)]
    result = hybrid.blend(cf, als, alpha=0.5, top_n=5)
    ranked_ids = [item_id for item_id, _ in result]
    assert set(ranked_ids) == {1, 2}


def test_empty_als_falls_back_to_normalized_cf():
    cf = [(1, 10.0), (2, 5.0)]
    result = hybrid.blend(cf, [], alpha=0.25, top_n=2)
    ranked_ids = [item_id for item_id, _ in result]
    assert ranked_ids == [1, 2]


def test_top_n_limits_result_length():
    cf = [(i, float(i)) for i in range(20)]
    result = hybrid.blend(cf, [], alpha=0.0, top_n=5)
    assert len(result) == 5


def test_default_alpha_matches_experiment_result():
    assert hybrid.DEFAULT_ALPHA == 0.25
