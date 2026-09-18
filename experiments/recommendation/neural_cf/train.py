"""Trains NeuMF (model.py) on the same interactions_train/test.parquet
split every other RQ2 model evaluates against, and reports it through
the identical experiments/recommendation/metrics.py functions -- so
Precision@10/Recall@10/MAP@10/NDCG@10 are directly comparable to
popularity/item-CF/content-based (../offline_models.py) and catalog-ALS
(../catalog_als/train.py) on identical held-out data.

Implicit feedback, same confidence framing as catalog-ALS: a
(user, item) pair observed in train is a positive example, weighted by
`1 + alpha * r_ui` in the loss (r_ui is the aggregated event weight --
1/3/5 for view/cart/purchase, summed); `NEG_SAMPLES_PER_POSITIVE`
items the user never interacted with are negative examples per
positive, weight 1.0 -- the unobserved-entry confidence in the same
c_ui = 1 + alpha*r_ui formula at r_ui=0. This mirrors ALS's own
implicitPrefs weighting (see docs/RESEARCH_REPORT.md's Algorithm
definitions) rather than inventing a separate scheme for the neural
model.

Runs on CPU -- this catalog (2,000 users, 300 items, ~25K training
pairs) trains in well under a minute; no GPU or Docker needed, same
"local-mode, no Docker" pattern ../catalog_als/train.py already uses.
"""
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import torch
from torch import nn
from torch.utils.data import DataLoader, Dataset

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))
from experiments.common import record_result  # noqa: E402
from experiments.recommendation.metrics import evaluate  # noqa: E402
from experiments.recommendation.neural_cf.model import NeuMF  # noqa: E402

TRAIN_PATH = REPO_ROOT / "data" / "interactions_train.parquet"
TEST_PATH = REPO_ROOT / "data" / "interactions_test.parquet"
MODEL_OUTPUT_PATH = Path(__file__).resolve().parent / "model.pt"
RESULTS_DIR = REPO_ROOT / "experiments" / "recommendation" / "results"

TOP_K = 10
NEG_SAMPLES_PER_POSITIVE = 4
ALPHA = 1.0  # same implicit-feedback confidence scaling ALS uses
EPOCHS = 20
BATCH_SIZE = 256
LEARNING_RATE = 1e-3
SEED = 42


class ImplicitFeedbackDataset(Dataset):
    """Re-draws negatives fresh each epoch (see __init__'s call site in
    train_one_epoch) rather than fixing one negative set up front --
    standard NCF practice, since a fixed negative set lets the model
    partly memorize "these specific items are never positive" instead
    of learning the general user/item interaction.
    """

    def __init__(self, positives: pd.DataFrame, n_items: int, rng: np.random.Generator):
        self.n_items = n_items
        self.rng = rng
        self.user_positive_items = positives.groupby("user_id")["product_id"].apply(set).to_dict()

        pos_users = positives["user_id"].to_numpy()
        pos_items = positives["product_id"].to_numpy()
        pos_weight = (1.0 + ALPHA * positives["weight"].to_numpy()).astype(np.float32)

        neg_users, neg_items = self._sample_negatives(pos_users)

        self.users = np.concatenate([pos_users, neg_users])
        self.items = np.concatenate([pos_items, neg_items])
        self.labels = np.concatenate([
            np.ones(len(pos_users), dtype=np.float32),
            np.zeros(len(neg_users), dtype=np.float32),
        ])
        self.sample_weights = np.concatenate([
            pos_weight,
            np.ones(len(neg_users), dtype=np.float32),
        ])

    def _sample_negatives(self, pos_users: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        neg_users, neg_items = [], []
        for user_id in pos_users:
            seen = self.user_positive_items.get(user_id, set())
            drawn = 0
            while drawn < NEG_SAMPLES_PER_POSITIVE:
                candidate = self.rng.integers(1, self.n_items + 1)
                if candidate not in seen:
                    neg_users.append(user_id)
                    neg_items.append(candidate)
                    drawn += 1
        return np.array(neg_users), np.array(neg_items)

    def __len__(self):
        return len(self.labels)

    def __getitem__(self, idx):
        return self.users[idx], self.items[idx], self.labels[idx], self.sample_weights[idx]


def train_one_epoch(model, dataset_factory, optimizer, device) -> float:
    dataset = dataset_factory()  # fresh negative sample each epoch
    loader = DataLoader(dataset, batch_size=BATCH_SIZE, shuffle=True)
    loss_fn = nn.BCEWithLogitsLoss(reduction="none")

    model.train()
    total_loss, n_examples = 0.0, 0
    for users, items, labels, sample_weights in loader:
        users, items = users.long().to(device), items.long().to(device)
        labels, sample_weights = labels.float().to(device), sample_weights.float().to(device)

        optimizer.zero_grad()
        logits = model(users, items)
        loss = (loss_fn(logits, labels) * sample_weights).mean()
        loss.backward()
        optimizer.step()

        total_loss += loss.item() * len(labels)
        n_examples += len(labels)
    return total_loss / n_examples


def recommend_for_test_users(model, train, test_users, n_items, k=TOP_K, device="cpu"):
    """Scores every item for every test user (score_all_items) and drops
    already-seen (train) items before taking the top-k -- the same
    seen-item exclusion ../catalog_als/train.py's top_k_unseen applies,
    so neural_cf isn't credited for re-recommending what a user already
    interacted with.
    """
    train_seen = train.groupby("user_id")["product_id"].apply(set).to_dict()
    recs = {}
    start = time.perf_counter()
    for user_id in test_users:
        scores = model.score_all_items(user_id, n_items, device=device)
        seen = train_seen.get(user_id, set())
        ranked_items = torch.argsort(scores, descending=True).cpu().numpy() + 1  # item ids are 1-indexed
        top = [int(item_id) for item_id in ranked_items if item_id not in seen][:k]
        recs[user_id] = top
    latency = (time.perf_counter() - start) / max(len(test_users), 1)
    return recs, latency


def main():
    device = "cpu"
    rng = np.random.default_rng(SEED)
    torch.manual_seed(SEED)

    train = pd.read_parquet(TRAIN_PATH)
    test = pd.read_parquet(TEST_PATH)
    n_items = int(max(train["product_id"].max(), test["product_id"].max()))
    n_users = int(max(train["user_id"].max(), test["user_id"].max()))
    print(f"[neural-cf] train pairs={len(train)} test pairs={len(test)} "
          f"n_users={n_users} n_items={n_items}", flush=True)

    model = NeuMF(n_users=n_users, n_items=n_items).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=LEARNING_RATE)

    for epoch in range(1, EPOCHS + 1):
        avg_loss = train_one_epoch(
            model, lambda: ImplicitFeedbackDataset(train, n_items, rng), optimizer, device
        )
        print(f"[neural-cf] epoch {epoch}/{EPOCHS} loss={avg_loss:.4f}", flush=True)

    actuals = test.groupby("user_id")["product_id"].apply(set).to_dict()
    test_users = list(actuals.keys())

    recs, latency = recommend_for_test_users(model, train, test_users, n_items, device=device)
    metrics = evaluate(recs, actuals, k=TOP_K)
    result = {**metrics, "latency_ms_per_user": latency * 1000}
    print(f"[neural-cf] {result}", flush=True)

    torch.save(model.state_dict(), MODEL_OUTPUT_PATH)
    print(f"[neural-cf] Model saved to {MODEL_OUTPUT_PATH}", flush=True)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    record_result(
        RESULTS_DIR,
        name="offline_models",
        config={
            "k": TOP_K, "test_users": len(test_users), "epochs": EPOCHS,
            "batch_size": BATCH_SIZE, "lr": LEARNING_RATE,
            "neg_samples_per_positive": NEG_SAMPLES_PER_POSITIVE, "alpha": ALPHA,
        },
        dataset="data/interactions.parquet (synthetic, catalog-native)",
        model="neural_cf",
        metric="precision@10,recall@10,map@10,ndcg@10,latency_ms_per_user",
        result=result,
    )


if __name__ == "__main__":
    main()
