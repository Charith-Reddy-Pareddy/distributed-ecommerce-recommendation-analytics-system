"""Blends live item-CF with precomputed catalog-ALS scores.

alpha=0.25 is the empirically best blend from the offline evaluation in
experiments/recommendation/hybrid.py (CF+ALS alpha sweep on held-out
data) -- it beat both pure CF (alpha=0) and pure ALS (alpha=1) on
Precision@10, Recall@10, and NDCG@10. See experiments/recommendation/
results/hybrid_cf_als.jsonl for the full sweep.

Both inputs are normalized per request to [0,1] (min-max) before
blending, same as the offline sweep -- CF's weighted-cosine sums and
ALS's implicit-feedback confidence scores live on different, unbounded
scales, so alpha is only a meaningful mixing weight once both are on
the same footing.
"""

DEFAULT_ALPHA = 0.25

# Min-max normalization always puts a model's own worst-ranked item at
# exactly 0.0 -- using that same 0.0 as the default for an item the model
# never scored at all would tie "this model's least favorite pick" with
# "this model has no opinion whatsoever," which isn't the same thing. A
# tiny negative epsilon breaks that tie in favor of whichever model
# actually considered the item, without materially changing any real,
# non-tied score.
_NO_SIGNAL = -1e-9


def _minmax_normalize(scored: dict[int, float]) -> dict[int, float]:
    if not scored:
        return {}
    values = list(scored.values())
    lo, hi = min(values), max(values)
    if hi == lo:
        return {item_id: 0.0 for item_id in scored}
    return {item_id: (v - lo) / (hi - lo) for item_id, v in scored.items()}


def blend(
    cf_scored: list[tuple[int, float]],
    als_scored: list[tuple[int, float]],
    alpha: float = DEFAULT_ALPHA,
    top_n: int = 10,
) -> list[tuple[int, float]]:
    """cf_scored/als_scored: [(item_id, score), ...] from each model, in
    any order. Returns the blended top_n as [(item_id, blended_score), ...].
    """
    cf_norm = _minmax_normalize(dict(cf_scored))
    als_norm = _minmax_normalize(dict(als_scored))

    item_ids = set(cf_norm) | set(als_norm)
    combined = {
        item_id: alpha * als_norm.get(item_id, _NO_SIGNAL) + (1 - alpha) * cf_norm.get(item_id, _NO_SIGNAL)
        for item_id in item_ids
    }
    ranked = sorted(combined.items(), key=lambda pair: pair[1], reverse=True)
    return ranked[:top_n]
