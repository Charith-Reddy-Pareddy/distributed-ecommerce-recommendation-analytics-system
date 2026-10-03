import math
import pytest
from experiments.recommendation.weighting import WEIGHTING_SCHEMES, reweight_rows, transform_rating


def test_rating_weight_schemes_match_documented_transforms():
    assert WEIGHTING_SCHEMES == ('binary', 'linear', 'squared', 'exponential')
    assert [transform_rating(4, scheme) for scheme in WEIGHTING_SCHEMES] == [1., 4., 16., 8.]


def test_all_weight_schemes_are_monotone_over_valid_ratings():
    for scheme in WEIGHTING_SCHEMES:
        values = [transform_rating(rating, scheme) for rating in (1, 2, 3, 4, 5)]
        assert values == sorted(values)


def test_reweight_preserves_interaction_identity_and_timestamp():
    row = ('user-x', 47, 4.0, 1712345678)
    assert reweight_rows([row], 'squared') == [('user-x', 47, 16., 1712345678)]


@pytest.mark.parametrize('rating', [0, 5.1, math.nan, math.inf, True, '5'])
def test_rejects_invalid_rating(rating):
    with pytest.raises(ValueError, match='rating'):
        transform_rating(rating, 'linear')


def test_rejects_unknown_scheme():
    with pytest.raises(ValueError, match='unknown'):
        transform_rating(4, 'mystery')
