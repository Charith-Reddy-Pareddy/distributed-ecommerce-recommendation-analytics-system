from experiments.recommendation.evaluation_scope import build_scope
from experiments.recommendation.weighting_ablation import run_ablation


def test_weight_ablation_uses_same_user_scope_and_raw_reference(monkeypatch):
    train = [('a', 1, 5., 1), ('a', 2, 4., 1), ('b', 1, 2., 1),
             ('b', 3, 3., 1), ('c', 2, 5., 1), ('c', 3, 4., 1)]
    test = [('a', 3, 3., 2), ('b', 2, 5., 2), ('c', 1, 4., 2)]
    scope = build_scope(train, test, max_items=3, max_users=3, min_history=2)

    def fake_scores(weighted_scope):
        assert weighted_scope.users == scope.users
        return {u: {i: float(i) for i in scope.items if i not in scope.seen[u]}
                for u in scope.users}

    monkeypatch.setattr('experiments.recommendation.weighting_ablation._cf_scores', fake_scores)
    monkeypatch.setattr('experiments.recommendation.weighting_ablation.als_scores',
                        lambda weighted_scope, seed: fake_scores(weighted_scope))
    result, _ = run_ablation(scope, schemes=('binary', 'raw'), n_resamples=50)
    for family in ('item_cf', 'als'):
        assert result[family]['metrics']['raw']['n_users'] == 3
        assert result[family]['uncertainty']['n_users'] == 3
        assert result[family]['uncertainty']['paired_differences_vs_reference']['binary']['precision']['reference'] == 'raw'
