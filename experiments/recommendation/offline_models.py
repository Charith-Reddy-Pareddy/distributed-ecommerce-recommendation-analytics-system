"""Evaluates popularity, item-CF, and content-based recommenders on the
same train/test split catalog-ALS uses (split_interactions.py), reporting
Precision@K/Recall@K/MAP@K/NDCG@K and per-recommendation latency -- RQ2's
model comparison.

Item-CF runs the actual production RecommendationEngine
(services/recommendation-service/app/model.py), not a reimplementation --
its `user_item`/`item_users` state is set directly from the pre-aggregated
train weights, which is equivalent to replaying the underlying events
through `_apply_event` one at a time.
"""
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))
from scripts.load_app_module import load_app_module  # noqa: E402
from experiments.common import record_result  # noqa: E402
from experiments.recommendation.metrics import evaluate  # noqa: E402

TRAIN_PATH = REPO_ROOT / "data" / "interactions_train.parquet"
TEST_PATH = REPO_ROOT / "data" / "interactions_test.parquet"
CATALOG_PATH = REPO_ROOT / "data" / "amazon_products.json"
RESULTS_DIR = REPO_ROOT / "experiments" / "recommendation" / "results"

TOP_K = 10


def load_split():
    return pd.read_parquet(TRAIN_PATH), pd.read_parquet(TEST_PATH)


def test_actuals(test):
    return test.groupby("user_id")["product_id"].apply(set).to_dict()


def popularity_recommendations(train, test_users, k=TOP_K):
    ranked = train.groupby("product_id")["weight"].sum().sort_values(ascending=False).index.tolist()[:k]
    start = time.perf_counter()
    recs = {user_id: list(ranked) for user_id in test_users}
    latency = (time.perf_counter() - start) / max(len(test_users), 1)
    return recs, latency


def build_cf_engine(train):
    model = load_app_module("recommendation-service", "model", "recsvc_app")
    engine = model.RecommendationEngine()
    for row in train.itertuples(index=False):
        engine.user_item[row.user_id][row.product_id] += row.weight
        engine.item_users[row.product_id][row.user_id] += row.weight
    return engine


def cf_recommendations(engine, test_users, k=TOP_K):
    recs = {}
    start = time.perf_counter()
    for user_id in test_users:
        recs[user_id] = [pid for pid, _ in engine.recommend_for_user(user_id, top_n=k)]
    latency = (time.perf_counter() - start) / max(len(test_users), 1)
    return recs, latency


def load_catalog_texts():
    products = json.loads(CATALOG_PATH.read_text())
    return {
        i: " ".join([p.get("category", ""), p.get("brand", ""), p.get("description", "")])
        for i, p in enumerate(products, start=1)
    }


def build_content_similarity():
    texts = load_catalog_texts()
    product_ids = sorted(texts)
    corpus = [texts[pid] for pid in product_ids]
    vectorizer = TfidfVectorizer(stop_words="english", max_features=2000)
    matrix = vectorizer.fit_transform(corpus)
    sim = cosine_similarity(matrix)
    index = {pid: i for i, pid in enumerate(product_ids)}
    return sim, index, product_ids


def content_recommendations(train, test_users, sim, index, product_ids, k=TOP_K):
    user_items = (
        train.groupby("user_id")
        .apply(lambda df: dict(zip(df["product_id"], df["weight"])), include_groups=False)
        .to_dict()
    )
    recs = {}
    start = time.perf_counter()
    for user_id in test_users:
        interacted = user_items.get(user_id, {})
        if not interacted:
            recs[user_id] = []
            continue
        scores = np.zeros(len(product_ids))
        for pid, weight in interacted.items():
            if pid in index:
                scores += sim[index[pid]] * weight
        for pid in interacted:
            if pid in index:
                scores[index[pid]] = -1.0
        top_idx = np.argsort(scores)[::-1][:k]
        recs[user_id] = [product_ids[i] for i in top_idx]
    latency = (time.perf_counter() - start) / max(len(test_users), 1)
    return recs, latency


def main():
    train, test = load_split()
    actuals = test_actuals(test)
    test_users = list(actuals.keys())

    models = {}

    pop_recs, pop_latency = popularity_recommendations(train, test_users)
    models["popularity"] = (pop_recs, pop_latency)

    engine = build_cf_engine(train)
    cf_recs, cf_latency = cf_recommendations(engine, test_users)
    models["item_cf"] = (cf_recs, cf_latency)

    sim, index, product_ids = build_content_similarity()
    content_recs, content_latency = content_recommendations(train, test_users, sim, index, product_ids)
    models["content_based"] = (content_recs, content_latency)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    for name, (recs, latency) in models.items():
        metrics = evaluate(recs, actuals, k=TOP_K)
        result = {**metrics, "latency_ms_per_user": latency * 1000}
        print(f"{name}: {result}")
        record_result(
            RESULTS_DIR,
            name="offline_models",
            config={"k": TOP_K, "test_users": len(test_users)},
            dataset="data/interactions.parquet (synthetic, catalog-native)",
            model=name,
            metric="precision@10,recall@10,map@10,ndcg@10,latency_ms_per_user",
            result=result,
        )


if __name__ == "__main__":
    main()
