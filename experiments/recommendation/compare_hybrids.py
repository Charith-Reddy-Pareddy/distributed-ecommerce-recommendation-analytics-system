"""Matched-scope offline comparison on real review train/test Parquet files.

All models train on the same top-N items selected using training frequency.
The fixed alpha grid is descriptive, not a test-set-tuned production choice.
"""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import time

import pyarrow.parquet as pq

from experiments.common import record_result
from experiments.recommendation.evaluation_scope import build_scope
from experiments.recommendation.hybrid import blend_scores, rank_scores
from experiments.recommendation.metrics import evaluate
from experiments.recommendation.text_similarity import add_vectors, build_tfidf, cosine
from scripts.load_app_module import load_app_module


def fingerprint(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def load_scope(data_dir, max_items, max_users, seed):
    train_path = data_dir / 'interactions_train.parquet'
    counts = Counter()
    for batch in pq.ParquetFile(train_path).iter_batches(columns=['product_id']):
        counts.update(batch.column(0).to_pylist())
    items = sorted(counts, key=lambda i: (-counts[i], i))[:max_items]
    def rows(path):
        cols = pq.read_table(path, filters=[('product_id', 'in', items)]).to_pydict()
        return list(zip(cols['user_id'], cols['product_id'], cols['weight'], cols['timestamp']))
    return build_scope(rows(train_path), rows(data_dir / 'interactions_test.parquet'),
                       max_items=max_items, max_users=max_users, seed=seed)


def content_scores(interacted, vectors):
    seeds = sorted(i for i in interacted if i in vectors)
    profile = add_vectors([vectors[i] for i in seeds], [interacted[i] for i in seeds])
    return {i: score for i, vec in vectors.items() if i not in interacted
            and (score := cosine(profile, vec)) > 0}


def als_scores(scope, rank=10, iterations=10, seed=42):
    # Reuse the project's Python 3.12 compatibility shim before pyspark.ml.
    from experiments.recommendation import train_catalog_als  # noqa: F401
    from pyspark.ml.recommendation import ALS
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import sum as spark_sum
    spark = (SparkSession.builder.master('local[2]').appName('matched-hybrids')
             .config('spark.driver.memory', '4g')
             .config('spark.sql.shuffle.partitions', '8').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    try:
        # Stable training-only index, not a separately fitted index per model.
        user_index = {u: i for i, u in enumerate(sorted({r[0] for r in scope.train}))}
        train = spark.createDataFrame(
            [(user_index[u], i, float(w)) for u, i, w, _ in scope.train],
            'user int, item int, weight double')
        train = train.groupBy('user', 'item').agg(spark_sum('weight').alias('weight'))
        model = ALS(userCol='user', itemCol='item', ratingCol='weight', implicitPrefs=True,
                    rank=rank, maxIter=iterations, regParam=.1, alpha=1., seed=seed,
                    coldStartStrategy='drop').fit(train)
        pairs = [(user_index[u], i) for u in scope.users for i in scope.items if i not in scope.seen[u]]
        output = {u: {} for u in scope.users}
        if pairs:
            inverse = {user_index[u]: u for u in scope.users}
            candidates = spark.createDataFrame(pairs, 'user int, item int')
            for row in model.transform(candidates).select('user', 'item', 'prediction').collect():
                output[inverse[row.user]][row.item] = float(row.prediction)
        return output
    finally:
        spark.stop()


def compare(scope, products, alphas=(0., .25, .5, .75, 1.), k=10, seed=42):
    missing = set(scope.items) - set(products)
    if missing:
        raise ValueError(f'Missing product text for {len(missing)} candidate items')
    model = load_app_module('recommendation-service', 'model', 'recsvc_app')
    engine = model.RecommendationEngine()
    for u, i, w, _ in scope.train:
        engine.user_item[u][i] += w
        engine.item_users[i][u] += w
    start = time.perf_counter()
    engine.refresh_neighbor_cache()
    cache_seconds = time.perf_counter() - start
    vectors = build_tfidf({i: products[i] for i in scope.items})
    scores = {'popularity': {}, 'item_cf': {}, 'content_based': {}}
    for u in scope.users:
        scores['popularity'][u] = dict(engine.popular_items(len(scope.items), exclude=scope.seen[u]))
        scores['item_cf'][u] = engine.scores_for_user(u)
        scores['content_based'][u] = content_scores(engine.user_item[u], vectors)
    print(f'CF cache built in {cache_seconds:.2f}s; training matched ALS', flush=True)
    start = time.perf_counter()
    scores['als'] = als_scores(scope, seed=seed)
    als_seconds = time.perf_counter() - start
    for other in ('als', 'content_based'):
        for alpha in alphas:
            scores[f'cf_{other}_alpha_{alpha:g}'] = {
                u: blend_scores(scores['item_cf'][u], scores[other][u], alpha, scope.items, scope.seen[u])
                for u in scope.users}
    results = {}
    for name, by_user in scores.items():
        recs = {u: rank_scores(s, k) for u, s in by_user.items()}
        for u, ranked in recs.items():
            if set(ranked) & scope.seen[u] or not set(ranked) <= set(scope.items):
                raise ValueError(f'{name} violated the candidate/exclusion contract')
        results[name] = evaluate(recs, scope.actuals, k)
        results[name]['recommendation_coverage'] = sum(bool(v) for v in recs.values()) / len(scope.users)
    return results, {'cf_cache_build_seconds': cache_seconds, 'als_fit_and_score_seconds': als_seconds}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--catalog-snapshot', type=Path, required=True,
                        help='JSON list of product-service records; ids must match the interaction ETL')
    parser.add_argument('--max-items', type=int, default=150)
    parser.add_argument('--max-users', type=int, default=150)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--output-dir', type=Path, default=Path(__file__).resolve().parent / 'results')
    args = parser.parse_args()
    scope = load_scope(args.data_dir, args.max_items, args.max_users, args.seed)
    catalog = json.loads(args.catalog_snapshot.read_text())
    products = {p['id']: f"{p['category']} {p.get('specifications', {}).get('brand', '')} {p['description']}"
                for p in catalog}
    print(f'Matched scope: {len(scope.items)} items, {len(scope.users)} users, {len(scope.train)} train rows', flush=True)
    results, timings = compare(scope, products, seed=args.seed)
    config = {'max_items': args.max_items, 'max_users': args.max_users, 'seed': args.seed,
              'k': 10, 'min_train_history': 5, 'split': 'existing random split',
              'candidate_selection': 'top training-frequency items; ties by id',
              'heldout_scope': 'unseen candidate items only', 'alphas': [0, .25, .5, .75, 1],
              'alpha_selection': 'fixed descriptive grid; no best-alpha tuning',
              'als': {'rank': 10, 'maxIter': 10, 'regParam': .1, 'alpha': 1.},
              'cf_neighbors': 50, 'normalization': 'per-user candidate min-max',
              'train_sha256': fingerprint(args.data_dir / 'interactions_train.parquet'),
              'test_sha256': fingerprint(args.data_dir / 'interactions_test.parquet'),
              'catalog_snapshot_sha256': fingerprint(args.catalog_snapshot)}
    result = {'n_items': len(scope.items), 'n_users': len(scope.users), 'n_train_rows': len(scope.train),
              'models': results, 'timings': timings}
    record_result(args.output_dir, 'matched_hybrids', config,
                  'Real Amazon review interactions, bounded shared candidate/user scope',
                  'popularity,item_cf,content,als,cf_als,cf_content', 'precision,recall,map,ndcg,coverage', result)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
