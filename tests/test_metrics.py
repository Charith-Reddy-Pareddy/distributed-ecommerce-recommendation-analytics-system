"""Regression tests for experiments/recommendation/metrics.py -- the sole
implementation behind every quality number in docs/RESEARCH_REPORT.md
(RQ2 model comparison, RQ3 hybrid sweep, every ablation, every bootstrap
CI). docs/RESEARCH_REPORT.md claims these were "hand-verified against
known examples before use," but no such verification existed anywhere
in the repo before this file -- these are those hand-worked examples,
made real. A silent regression here would corrupt every headline result
with nothing to catch it.
"""
import importlib.util
import math
import sys
from pathlib import Path

# metrics.py lives under experiments/, not services/<name>/app/, so
# scripts.load_app_module's assumed layout doesn't fit -- load it
# directly instead.
REPO_ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location(
    "recommendation_metrics", REPO_ROOT / "experiments" / "recommendation" / "metrics.py"
)
metrics = importlib.util.module_from_spec(spec)
sys.modules["recommendation_metrics"] = metrics
spec.loader.exec_module(metrics)


# ---- precision_at_k -------------------------------------------------

def test_precision_k_zero_returns_zero_not_division_error():
    assert metrics.precision_at_k(["A", "B"], {"A"}, 0) == 0.0


def test_precision_perfect_hit():
    assert metrics.precision_at_k(["A", "B"], {"A", "B"}, 2) == 1.0


def test_precision_no_hits():
    assert metrics.precision_at_k(["A", "B"], {"X", "Y"}, 2) == 0.0


def test_precision_partial_hits():
    # 2 of 5 recommended are relevant.
    assert metrics.precision_at_k(["A", "B", "C", "D", "E"], {"A", "C"}, 5) == 0.4


def test_precision_truncates_to_k_ignoring_hits_beyond_it():
    # A relevant item at rank 4 must not count when k=2.
    assert metrics.precision_at_k(["A", "B", "C", "D"], {"D"}, 2) == 0.0


# ---- recall_at_k -------------------------------------------------

def test_recall_empty_actual_returns_zero_not_division_error():
    assert metrics.recall_at_k(["A", "B"], set(), 2) == 0.0


def test_recall_finds_all_relevant_items():
    assert metrics.recall_at_k(["A", "B", "C"], {"A", "C"}, 3) == 1.0


def test_recall_is_fraction_of_actual_found_not_fraction_of_k():
    # Only 1 of 4 relevant items appears in the top-k -- recall is
    # relative to len(actual) (4), not to k (2).
    assert metrics.recall_at_k(["A", "B"], {"A", "W", "X", "Y"}, 2) == 0.25


# ---- average_precision_at_k (MAP) ------------------------------------

def test_map_hand_worked_example():
    # recommended = [A, B, C, D], actual = {A, C}, k=4.
    # Hit at rank 1 (A): running precision 1/1 = 1.0
    # Hit at rank 3 (C): running precision 2/3
    # score = 1.0 + 2/3 = 5/3; normalizer = min(len(actual)=2, k=4) = 2
    # MAP = (5/3) / 2 = 5/6
    result = metrics.average_precision_at_k(["A", "B", "C", "D"], {"A", "C"}, 4)
    assert math.isclose(result, 5 / 6, rel_tol=1e-9)


def test_map_empty_actual_returns_zero():
    assert metrics.average_precision_at_k(["A"], set(), 5) == 0.0


def test_map_rewards_earlier_hits_over_later_ones():
    # Same two hits, different rank -- earlier hits must score higher.
    early = metrics.average_precision_at_k(["A", "C", "B", "D"], {"A", "C"}, 4)
    late = metrics.average_precision_at_k(["B", "D", "A", "C"], {"A", "C"}, 4)
    assert early > late


def test_map_normalizes_by_k_not_by_total_relevant_when_actual_exceeds_k():
    # actual has 5 relevant items but k=2 and both top-2 slots are hits --
    # the normalizer must be min(len(actual), k) = min(5, 2) = 2, giving a
    # perfect MAP of 1.0, not 2/5 (which would be the result of wrongly
    # normalizing by len(actual) instead of k).
    result = metrics.average_precision_at_k(["A", "B"], {"A", "B", "C", "D", "E"}, 2)
    assert result == 1.0


# ---- ndcg_at_k -------------------------------------------------

def test_ndcg_hand_worked_example():
    # recommended = [A, B, C], actual = {A, C}, k=3.
    # DCG = 1/log2(1+1) [A at rank 1] + 1/log2(3+1) [C at rank 3]
    # IDCG = 1/log2(1+1) + 1/log2(2+1) [ideal: both hits ranked first]
    dcg = 1 / math.log2(2) + 1 / math.log2(4)
    idcg = 1 / math.log2(2) + 1 / math.log2(3)
    expected = dcg / idcg
    result = metrics.ndcg_at_k(["A", "B", "C"], {"A", "C"}, 3)
    assert math.isclose(result, expected, rel_tol=1e-9)


def test_ndcg_perfect_ranking_is_one():
    assert math.isclose(metrics.ndcg_at_k(["A", "C"], {"A", "C"}, 2), 1.0, rel_tol=1e-9)


def test_ndcg_empty_actual_returns_zero():
    assert metrics.ndcg_at_k(["A"], set(), 5) == 0.0


def test_ndcg_k_zero_returns_zero_not_division_error():
    # ideal_hits = min(len(actual), 0) = 0 -> idcg = 0 -- must hit the
    # `if idcg > 0 else 0.0` guard, not raise ZeroDivisionError.
    assert metrics.ndcg_at_k(["A", "B"], {"A"}, 0) == 0.0


def test_ndcg_penalizes_relevant_items_ranked_lower():
    higher = metrics.ndcg_at_k(["A", "B", "C"], {"A"}, 3)
    lower = metrics.ndcg_at_k(["B", "C", "A"], {"A"}, 3)
    assert higher > lower


# ---- evaluate_per_user / evaluate -------------------------------------

def test_evaluate_only_scores_users_present_in_actuals():
    recs = {1: ["A", "B"], 2: ["X"]}
    actuals = {1: {"A"}}  # user 2 has no held-out interactions
    result = metrics.evaluate(recs, actuals, k=2)
    assert result["n_users"] == 1


def test_evaluate_missing_recommendations_scores_as_zero_not_a_crash():
    # A user present in actuals but absent from user_recommendations
    # (e.g. a cold-start user the model never scored) must fall back to
    # an empty recommendation list, not raise a KeyError.
    recs = {}
    actuals = {1: {"A", "B"}}
    result = metrics.evaluate(recs, actuals, k=5)
    assert result == {"precision": 0.0, "recall": 0.0, "map": 0.0, "ndcg": 0.0, "n_users": 1}


def test_evaluate_zero_users_returns_zeros_not_division_error():
    result = metrics.evaluate({}, {}, k=10)
    assert result == {"precision": 0.0, "recall": 0.0, "map": 0.0, "ndcg": 0.0, "n_users": 0}


def test_evaluate_averages_across_users():
    recs = {1: ["A", "X"], 2: ["Y", "B"]}
    actuals = {1: {"A"}, 2: {"B"}}
    # Both users: 1 hit out of 2 recommended -> precision@2 = 0.5 each.
    result = metrics.evaluate(recs, actuals, k=2)
    assert result["precision"] == 0.5
    assert result["n_users"] == 2


def test_evaluate_per_user_exposes_individual_scores():
    recs = {1: ["A"], 2: ["Z"]}
    actuals = {1: {"A"}, 2: {"A"}}
    per_user = metrics.evaluate_per_user(recs, actuals, k=1)
    assert per_user[1]["precision"] == 1.0
    assert per_user[2]["precision"] == 0.0
