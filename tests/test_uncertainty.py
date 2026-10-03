import pytest
from experiments.recommendation.uncertainty import METRICS, summarize_user_metrics


def rows(values):
    return {u: dict(zip(METRICS, [v] * 4)) for u, v in values.items()}


def test_paired_summary_reports_shared_user_intervals_and_difference():
    result = summarize_user_metrics({'cf': rows({'a': 0., 'b': 0.}),
                                     'hybrid': rows({'a': .1, 'b': .3})},
                                    'cf', n_resamples=100, seed=4)
    assert result['n_users'] == 2
    assert result['models']['hybrid']['precision']['mean'] == .2
    assert result['paired_differences_vs_reference']['hybrid']['precision']['mean_difference'] == .2
    assert result['paired_differences_vs_reference']['hybrid']['precision']['lower'] > 0


def test_summary_rejects_different_user_populations():
    with pytest.raises(ValueError, match='different user population'):
        summarize_user_metrics({'cf': rows({'a': 0}), 'als': rows({'b': .2})}, 'cf')


def test_summary_requires_all_user_metrics():
    with pytest.raises(ValueError, match='missing user metrics'):
        summarize_user_metrics({'cf': {'a': {'precision': .1}}}, 'cf')
