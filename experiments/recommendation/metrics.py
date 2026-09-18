"""Standard top-K recommendation metrics: Precision, Recall, MAP, NDCG.

All four take a per-user ranked list of recommended item ids and the set
of held-out (actual) item ids the user interacted with, and are averaged
across users the same way for every model under evaluation -- so scores
are comparable across popularity/CF/ALS/content-based.
"""
import math


def precision_at_k(recommended, actual, k):
    if k == 0:
        return 0.0
    hits = len(set(recommended[:k]) & actual)
    return hits / k


def recall_at_k(recommended, actual, k):
    if not actual:
        return 0.0
    hits = len(set(recommended[:k]) & actual)
    return hits / len(actual)


def average_precision_at_k(recommended, actual, k):
    if not actual:
        return 0.0
    hits = 0
    score = 0.0
    for i, item in enumerate(recommended[:k], start=1):
        if item in actual:
            hits += 1
            score += hits / i
    return score / min(len(actual), k)


def ndcg_at_k(recommended, actual, k):
    if not actual:
        return 0.0
    dcg = sum(
        1.0 / math.log2(i + 1)
        for i, item in enumerate(recommended[:k], start=1)
        if item in actual
    )
    ideal_hits = min(len(actual), k)
    idcg = sum(1.0 / math.log2(i + 1) for i in range(1, ideal_hits + 1))
    return dcg / idcg if idcg > 0 else 0.0


def evaluate_per_user(user_recommendations, user_actuals, k=10):
    """Same inputs as evaluate(). Returns dict[user_id -> {"precision",
    "recall", "map", "ndcg"}] -- the per-user values evaluate() averages,
    exposed separately so callers that need the underlying distribution
    (e.g. bootstrap confidence intervals, experiments/recommendation/
    bootstrap_ci.py) don't have to recompute it or reimplement scoring.
    """
    per_user = {}
    for user_id, actual in user_actuals.items():
        recommended = user_recommendations.get(user_id, [])
        per_user[user_id] = {
            "precision": precision_at_k(recommended, actual, k),
            "recall": recall_at_k(recommended, actual, k),
            "map": average_precision_at_k(recommended, actual, k),
            "ndcg": ndcg_at_k(recommended, actual, k),
        }
    return per_user


def evaluate(user_recommendations, user_actuals, k=10):
    """user_recommendations: dict[user_id -> ranked list of item ids].
    user_actuals: dict[user_id -> set of held-out item ids].

    Only evaluates users present in user_actuals. Returns per-metric
    averages plus the number of users evaluated.
    """
    per_user = evaluate_per_user(user_recommendations, user_actuals, k)
    n = len(per_user)
    if n == 0:
        return {"precision": 0.0, "recall": 0.0, "map": 0.0, "ndcg": 0.0, "n_users": 0}
    return {
        "precision": sum(v["precision"] for v in per_user.values()) / n,
        "recall": sum(v["recall"] for v in per_user.values()) / n,
        "map": sum(v["map"] for v in per_user.values()) / n,
        "ndcg": sum(v["ndcg"] for v in per_user.values()) / n,
        "n_users": n,
    }
