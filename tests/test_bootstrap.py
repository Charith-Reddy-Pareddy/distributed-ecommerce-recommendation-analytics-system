from experiments.recommendation.bootstrap import bootstrap_ci, paired_bootstrap_diff


def test_bootstrap_ci_point_estimate_matches_mean():
    values = [0.1, 0.3, 0.5, 0.7, 0.9]
    point, lower, upper = bootstrap_ci(values, n_resamples=500, seed=1)
    assert point == sum(values) / len(values)
    assert lower <= point <= upper


def test_bootstrap_ci_constant_values_has_zero_width():
    values = [0.42] * 20
    point, lower, upper = bootstrap_ci(values, n_resamples=200, seed=1)
    assert abs(point - 0.42) < 1e-9
    assert abs(upper - lower) < 1e-9


def test_bootstrap_ci_is_deterministic_for_same_seed():
    values = [0.1, 0.4, 0.6, 0.2, 0.9, 0.3]
    first = bootstrap_ci(values, n_resamples=300, seed=7)
    second = bootstrap_ci(values, n_resamples=300, seed=7)
    assert first == second


def test_bootstrap_ci_narrows_with_more_users():
    small_sample = [0.0, 1.0] * 5
    large_sample = [0.0, 1.0] * 500
    _, small_lower, small_upper = bootstrap_ci(small_sample, n_resamples=1000, seed=42)
    _, large_lower, large_upper = bootstrap_ci(large_sample, n_resamples=1000, seed=42)
    assert (large_upper - large_lower) < (small_upper - small_lower)


def test_bootstrap_ci_empty_values_returns_zeros():
    assert bootstrap_ci([], n_resamples=100, seed=1) == (0.0, 0.0, 0.0)


def test_paired_bootstrap_diff_zero_when_identical():
    values = [0.2, 0.4, 0.6, 0.8]
    diff, lower, upper = paired_bootstrap_diff(values, values, n_resamples=200, seed=1)
    assert diff == lower == upper == 0.0


def test_paired_bootstrap_diff_matches_constant_offset():
    base = [0.1, 0.2, 0.3, 0.4, 0.5]
    offset = [v + 0.05 for v in base]
    diff, lower, upper = paired_bootstrap_diff(offset, base, n_resamples=500, seed=3)
    assert abs(diff - 0.05) < 1e-9
    assert abs(upper - lower) < 1e-9


def test_paired_bootstrap_diff_rejects_mismatched_lengths():
    try:
        paired_bootstrap_diff([0.1, 0.2], [0.1], n_resamples=100, seed=1)
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_paired_bootstrap_diff_ci_excludes_zero_for_clear_effect():
    # A large, consistent gap between two arrays should show up as a CI
    # that doesn't straddle zero -- the paired-bootstrap analogue of the
    # significance check bootstrap_ci.py runs for CF vs. hybrid.
    better = [0.9] * 50
    worse = [0.1] * 50
    diff, lower, upper = paired_bootstrap_diff(better, worse, n_resamples=500, seed=9)
    assert lower > 0
    assert diff > 0.7
