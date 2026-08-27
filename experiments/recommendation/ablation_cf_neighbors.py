"""Point 5: does the number of similar-item neighbors item-CF blends over
per seed item affect quality? RecommendationEngine.recommend_for_user
hardcodes this at top_n=20 (services/recommendation-service/app/model.py)
and doesn't expose it as a parameter, so recommend_with_neighbor_count()
below mirrors its exact scoring logic with that made explicit -- at
neighbor_count=20 it should reproduce production's own numbers exactly
(a built-in sanity check against offline_models.py's item_cf result).
"""
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from experiments.common import record_result  # noqa: E402
from experiments.recommendation.metrics import evaluate  # noqa: E402
from experiments.recommendation.offline_models import load_split, test_actuals, build_cf_engine  # noqa: E402

RESULTS_DIR = REPO_ROOT / "experiments" / "recommendation" / "results"
TOP_K = 10
NEIGHBOR_COUNTS = [5, 10, 20, 50]


def recommend_with_neighbor_count(engine, user_id, neighbor_count, top_n=TOP_K):
    interacted = dict(engine.user_item.get(user_id, {}))
    if not interacted:
        return [pid for pid, _ in engine.popular_items(top_n)]

    candidate_scores = {}
    for product_id, weight in interacted.items():
        for similar_id, sim_score in engine.similar_items(product_id, top_n=neighbor_count):
            if similar_id in interacted:
                continue
            candidate_scores[similar_id] = candidate_scores.get(similar_id, 0.0) + sim_score * weight

    if not candidate_scores:
        return [pid for pid, _ in engine.popular_items(top_n)]

    ranked = sorted(candidate_scores.items(), key=lambda x: x[1], reverse=True)
    return [pid for pid, _ in ranked[:top_n]]


def main():
    train, test = load_split()
    actuals = test_actuals(test)
    test_users = list(actuals.keys())
    engine = build_cf_engine(train)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    for neighbor_count in NEIGHBOR_COUNTS:
        recs = {u: recommend_with_neighbor_count(engine, u, neighbor_count) for u in test_users}
        metrics = evaluate(recs, actuals, k=TOP_K)
        print(f"[neighbor_count={neighbor_count}] {metrics}", flush=True)
        record_result(
            RESULTS_DIR,
            name="ablation_cf_neighbors",
            config={"neighbor_count": neighbor_count, "k": TOP_K},
            dataset="data/interactions.parquet (synthetic, catalog-native)",
            model=f"item_cf_neighbors_{neighbor_count}",
            metric="precision@10,recall@10,map@10,ndcg@10",
            result=metrics,
        )


if __name__ == "__main__":
    main()
