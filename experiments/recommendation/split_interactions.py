"""Aggregates the raw interaction log to one weighted row per (user, item)
and splits it into train/test, the same way for every model under
evaluation -- popularity, item-CF, catalog-ALS, content-based all read
these same two files, so RQ2's model comparison is apples-to-apples.

Mirrors jobs/als-training/train_als.py's evaluation methodology: only
users with >= MIN_INTERACTIONS_FOR_EVAL distinct items get a held-out
test split (too little history and the split is mostly noise); everyone
else's interactions go entirely into train.
"""
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))
from scripts.load_app_module import load_app_module  # noqa: E402

INTERACTIONS_PATH = REPO_ROOT / "data" / "interactions.parquet"
TRAIN_PATH = REPO_ROOT / "data" / "interactions_train.parquet"
TEST_PATH = REPO_ROOT / "data" / "interactions_test.parquet"

MIN_INTERACTIONS_FOR_EVAL = 5
TEST_FRACTION = 0.2
SEED = 42


def weighted_interactions(event_weights=None):
    """event_weights defaults to production's view/cart/purchase weights
    (services/recommendation-service/app/model.py); pass a different dict
    to run the same pipeline under an alternative weighting scheme (RQ1).
    """
    if event_weights is None:
        model = load_app_module("recommendation-service", "model", "recsvc_app")
        event_weights = model.EVENT_WEIGHTS

    events = pd.read_parquet(INTERACTIONS_PATH)
    events["weight"] = events["event_type"].map(event_weights)
    return (
        events.groupby(["user_id", "product_id"], as_index=False)["weight"]
        .sum()
    )


def split(interactions, seed=SEED):
    counts = interactions.groupby("user_id").size()
    eligible_users = set(counts[counts >= MIN_INTERACTIONS_FOR_EVAL].index)

    eligible = interactions[interactions["user_id"].isin(eligible_users)]
    ineligible = interactions[~interactions["user_id"].isin(eligible_users)]

    test = eligible.sample(frac=TEST_FRACTION, random_state=seed)
    train_eligible = eligible.drop(test.index)
    train = pd.concat([train_eligible, ineligible], ignore_index=True)
    return train, test.reset_index(drop=True)


def main():
    interactions = weighted_interactions()
    train, test = split(interactions)

    train.to_parquet(TRAIN_PATH, index=False)
    test.to_parquet(TEST_PATH, index=False)

    print(f"Interactions (user, item) pairs: {len(interactions):,}")
    print(f"Train: {len(train):,}  Test: {len(test):,}")
    print(f"Eligible (test) users: {test['user_id'].nunique():,}")
    print(f"Wrote {TRAIN_PATH}")
    print(f"Wrote {TEST_PATH}")


if __name__ == "__main__":
    main()
