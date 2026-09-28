"""Score-level offline fusion; alpha is the weight of the second model."""
import math


def normalize_scores(scores):
    """Min-max normalize each user's finite candidate scores to [0, 1].

    Missing candidates carry zero mass. A constant observed score vector
    carries equal mass, retaining its candidates rather than erasing them.
    """
    if not all(math.isfinite(s) for s in scores.values()):
        raise ValueError("Scores must be finite")
    if not scores:
        return {}
    lo, hi = min(scores.values()), max(scores.values())
    if hi == lo:
        return {item: 1.0 for item in scores}
    return {item: (score - lo) / (hi - lo) for item, score in scores.items()}


def blend_scores(first, second, alpha, candidates, seen):
    if not math.isfinite(alpha) or not 0 <= alpha <= 1:
        raise ValueError("alpha must be between zero and one")
    allowed = set(candidates) - set(seen)
    a = normalize_scores({i: s for i, s in first.items() if i in allowed})
    b = normalize_scores({i: s for i, s in second.items() if i in allowed})
    # Preserve endpoint support exactly: the zero-weight model must not
    # contribute extra zero-score recommendations when the other is empty.
    support = set(a) if alpha == 0 else set(b) if alpha == 1 else set(a) | set(b)
    return {i: (1 - alpha) * a.get(i, 0.) + alpha * b.get(i, 0.) for i in support}


def rank_scores(scores, k=10):
    if k < 1:
        raise ValueError("k must be positive")
    return sorted(scores, key=lambda i: (-scores[i], i))[:k]
