"""Splits experiments/recommendation/interactions.parquet into
train/test sets, two ways:

- **Random 80/20** -- matches the RetailRocket ALS job's own
  methodology (`jobs/als-training/train_als.py`'s `MIN_INTERACTIONS_FOR_EVAL`):
  users need >=5 distinct interactions to get a held-out test split,
  since with fewer a held-out item is mostly noise; every interaction
  still trains the model regardless.
- **Temporal** -- real review timestamps as the causal boundary
  (RQ3): reviews before a cutoff percentile train, reviews after it
  test. A production system never gets to peek at future interactions,
  so this is the more realistic evaluation the random split can't be.

Usage:

    python -m experiments.recommendation.split_interactions
"""
import random
from collections import defaultdict
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

INTERACTIONS_PATH = Path(__file__).resolve().parent / "interactions.parquet"
MIN_INTERACTIONS_FOR_EVAL = 5
RANDOM_SPLIT_SEED = 42
RANDOM_TEST_FRACTION = 0.2
TEMPORAL_TRAIN_FRACTION = 0.8


def load_rows(path: Path = INTERACTIONS_PATH) -> list[tuple[str, int, float, int]]:
    table = pq.read_table(path)
    cols = table.to_pydict()
    return list(zip(cols["user_id"], cols["product_id"], cols["weight"], cols["timestamp"]))


def random_split(
    rows: list[tuple[str, int, float, int]], seed: int = RANDOM_SPLIT_SEED, min_interactions: int = MIN_INTERACTIONS_FOR_EVAL
) -> tuple[list, list]:
    by_user: dict[str, list] = defaultdict(list)
    for row in rows:
        by_user[row[0]].append(row)

    rng = random.Random(seed)
    train, test = [], []
    for user_rows in by_user.values():
        if len(user_rows) < min_interactions:
            train.extend(user_rows)
            continue
        shuffled = user_rows[:]
        rng.shuffle(shuffled)
        n_test = max(1, int(len(shuffled) * RANDOM_TEST_FRACTION))
        test.extend(shuffled[:n_test])
        train.extend(shuffled[n_test:])
    return train, test


def temporal_split(
    rows: list[tuple[str, int, float, int]], train_fraction: float = TEMPORAL_TRAIN_FRACTION
) -> tuple[list, list]:
    ordered = sorted(rows, key=lambda r: r[3])
    cutoff = int(len(ordered) * train_fraction)
    return ordered[:cutoff], ordered[cutoff:]


def write_split(rows: list[tuple[str, int, float, int]], path: Path) -> None:
    table = pa.table(
        {
            "user_id": [r[0] for r in rows],
            "product_id": [r[1] for r in rows],
            "weight": [r[2] for r in rows],
            "timestamp": [r[3] for r in rows],
        }
    )
    pq.write_table(table, path)


def main() -> None:
    rows = load_rows()
    print(f"Loaded {len(rows)} interactions", flush=True)

    train, test = random_split(rows)
    write_split(train, INTERACTIONS_PATH.parent / "interactions_train.parquet")
    write_split(test, INTERACTIONS_PATH.parent / "interactions_test.parquet")
    print(f"Random split: {len(train)} train / {len(test)} test")

    t_train, t_test = temporal_split(rows)
    write_split(t_train, INTERACTIONS_PATH.parent / "interactions_temporal_train.parquet")
    write_split(t_test, INTERACTIONS_PATH.parent / "interactions_temporal_test.parquet")
    print(f"Temporal split: {len(t_train)} train / {len(t_test)} test")


if __name__ == "__main__":
    main()
