import pytest

from experiments.recommendation.compare_hybrids import (
    DEFAULT_CATALOG_SNAPSHOT,
    compare,
    content_scores,
)
from experiments.recommendation.evaluation_scope import build_scope


def test_content_scores_excludes_seen_items_and_zero_similarity():
    assert content_scores({1: 5.}, {1: {'a': 1.}, 2: {'a': 1.}, 3: {'b': 1.}}) == {2: 1.}


def test_comparison_keeps_identical_users_and_component_endpoints(monkeypatch):
    train = [('a', 1, 5., 1), ('b', 2, 5., 1), ('c', 1, 4., 1), ('c', 2, 4., 1)]
    scope = build_scope(train, [('a', 2, 5., 2), ('b', 1, 5., 2)], min_history=1)
    monkeypatch.setattr('experiments.recommendation.compare_hybrids.als_scores',
                        lambda scope, **kw: {'a': {2: .2}, 'b': {}})
    results, _, uncertainty = compare(scope, {1: 'guitar music', 2: 'guitar string'}, k=1,
                                     bootstrap_resamples=50)
    assert all(r['n_users'] == 2 for r in results.values())
    assert results['als']['precision'] == .5
    assert results['cf_als_alpha_0'] == results['item_cf']
    assert results['cf_als_alpha_1'] == results['als']
    assert results['cf_content_based_alpha_1'] == results['content_based']
    assert results['popularity']['precision'] == 1.
    assert uncertainty['n_users'] == 2
    assert uncertainty['paired_differences_vs_reference']['cf_als_alpha_0.5']['precision']['reference'] == 'item_cf'


def test_comparison_rejects_incomplete_metadata():
    scope = build_scope([('a', 1, 1., 1), ('b', 2, 1., 1)], [('a', 2, 1., 2)], min_history=1)
    with pytest.raises(ValueError, match='Missing product text'):
        compare(scope, {1: 'music'})


def test_default_catalog_snapshot_is_archived():
    assert DEFAULT_CATALOG_SNAPSHOT.is_file()
