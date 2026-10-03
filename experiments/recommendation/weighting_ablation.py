"""Real-review confidence-weight ablation on the fixed Day B scope."""
import argparse
from dataclasses import replace
from pathlib import Path
import time

from experiments.common import record_result
from experiments.recommendation.compare_hybrids import als_scores, fingerprint, load_scope
from experiments.recommendation.hybrid import rank_scores
from experiments.recommendation.metrics import evaluate_per_user
from experiments.recommendation.uncertainty import summarize_user_metrics
from experiments.recommendation.weighting import WEIGHTING_SCHEMES, reweight_rows
from scripts.load_app_module import load_app_module


def _cf_scores(scope):
    model = load_app_module('recommendation-service', 'model', 'recsvc_weighting')
    engine = model.RecommendationEngine()
    for user, item, weight, _ in scope.train:
        engine.user_item[user][item] += weight
        engine.item_users[item][user] += weight
    engine.refresh_neighbor_cache()
    return {user: engine.scores_for_user(user) for user in scope.users}


def _user_metrics(scores, scope, k=10):
    recs = {user: rank_scores(scores[user], k) for user in scope.users}
    for user, ranked in recs.items():
        if set(ranked) & scope.seen[user] or not set(ranked) <= set(scope.items):
            raise ValueError('weight ablation emitted a seen or out-of-scope item')
    return evaluate_per_user(recs, scope.actuals, k)


def run_ablation(scope, schemes=WEIGHTING_SCHEMES, n_resamples=5000, ci=.95, seed=42):
    if 'linear' not in schemes:
        raise ValueError("schemes must include 'linear' as the reference")
    if len(set(schemes)) != len(schemes):
        raise ValueError('schemes must be unique')
    cf_users, als_users, timings = {}, {}, {}
    for scheme in schemes:
        weighted_scope = replace(scope, train=reweight_rows(scope.train, scheme))
        start = time.perf_counter()
        cf_users[scheme] = _user_metrics(_cf_scores(weighted_scope), scope)
        timings[f'cf_{scheme}_seconds'] = time.perf_counter() - start
        start = time.perf_counter()
        als_users[scheme] = _user_metrics(als_scores(weighted_scope, seed=seed), scope)
        timings[f'als_{scheme}_fit_and_score_seconds'] = time.perf_counter() - start
    results = {}
    for family, per_model in (('item_cf', cf_users), ('als', als_users)):
        means = {}
        for scheme, users in per_model.items():
            means[scheme] = {metric: sum(row[metric] for row in users.values()) / len(users)
                             for metric in ('precision', 'recall', 'map', 'ndcg')}
            means[scheme]['n_users'] = len(users)
        results[family] = {'metrics': means,
                           'uncertainty': summarize_user_metrics(per_model, 'linear',
                               n_resamples=n_resamples, ci=ci, seed=seed)}
    return results, timings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--max-items', type=int, default=150)
    parser.add_argument('--max-users', type=int, default=150)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--bootstrap-resamples', type=int, default=5000)
    parser.add_argument('--confidence-level', type=float, default=.95)
    parser.add_argument('--output-dir', type=Path, default=Path(__file__).resolve().parent / 'results')
    args = parser.parse_args()
    scope = load_scope(args.data_dir, args.max_items, args.max_users, args.seed)
    results, timings = run_ablation(scope, n_resamples=args.bootstrap_resamples,
                                    ci=args.confidence_level, seed=args.seed)
    config = {'max_items': args.max_items, 'max_users': args.max_users, 'seed': args.seed,
              'k': 10, 'weighting_schemes': {
                  'binary': '1', 'linear': 'rating', 'squared': 'rating^2',
                  'exponential': '2^(rating-1)'},
              'candidate_selection': 'top training-frequency items before transformation',
              'split': 'existing random split; held-out relevance is unchanged',
              'n_resamples': args.bootstrap_resamples, 'confidence_level': args.confidence_level,
              'train_sha256': fingerprint(args.data_dir / 'interactions_train.parquet'),
              'test_sha256': fingerprint(args.data_dir / 'interactions_test.parquet')}
    result = {'n_items': len(scope.items), 'n_users': len(scope.users),
              'n_train_rows': len(scope.train), 'families': results, 'timings': timings}
    record_result(args.output_dir, 'weighting_ablation', config,
                  'Real Amazon review interactions, fixed Day B candidate and user scope',
                  'item_cf,implicit_als', 'precision,recall,map,ndcg', result)
    import json
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
