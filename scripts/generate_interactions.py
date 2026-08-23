"""Generate a synthetic, catalog-native interaction log.

This is documented, structured synthetic data -- not real user behavior --
built so the offline recommendation experiments in `experiments/recommendation/`
have something with real collaborative structure to learn from, over the
*actual* demo catalog (so item ids line up with product-service, unlike the
RetailRocket dataset used in `jobs/als-training/`).

Generative model, per user:
  - Each user prefers 1-3 catalog categories, picked up front.
  - Each user browses a handful of sessions spread across WEEKS simulated
    weeks. Within a session, items are drawn mostly from the user's
    preferred categories (some off-category exploration), and within a
    category, popularity follows a Zipf distribution -- a few items get
    most of the traffic, not uniform random.
  - Each item touch is a view, and independently may also become an
    add_to_cart, and then a purchase, at decreasing probability -- the
    same view/cart/purchase funnel `recommendation-service` already
    weights 1/3/5.

Product ids match what product-service would assign: 1-indexed position
in data/amazon_products.json (see scripts/seed_data.py).

Output: one parquet file, one row per event
  (user_id, product_id, event_type, timestamp, week).
"""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
CATALOG_PATH = REPO_ROOT / "data" / "amazon_products.json"

ZIPF_S = 1.1
P_OFF_CATEGORY = 0.2
P_ADD_TO_CART = 0.35
P_PURCHASE_GIVEN_CART = 0.4
SESSIONS_PER_USER_MEAN = 6
ITEMS_PER_SESSION_RANGE = (1, 6)


def load_catalog(path):
    products = json.loads(path.read_text())
    product_ids = list(range(1, len(products) + 1))
    categories = {}
    for product_id, product in zip(product_ids, products):
        categories.setdefault(product["category"], []).append(product_id)
    return product_ids, categories


def zipf_weights(n):
    ranks = np.arange(1, n + 1)
    weights = 1.0 / ranks**ZIPF_S
    return weights / weights.sum()


def generate_events(n_users, weeks, seed):
    rng = np.random.default_rng(seed)
    product_ids, categories = load_catalog(CATALOG_PATH)
    category_names = list(categories.keys())
    category_weights = {
        name: zipf_weights(len(ids)) for name, ids in categories.items()
    }

    window_seconds = weeks * 7 * 24 * 3600
    now = pd.Timestamp.now(tz="UTC")
    window_start = now - pd.Timedelta(seconds=window_seconds)

    rows = []
    for user_id in range(1, n_users + 1):
        n_preferred = rng.integers(1, 4)
        preferred = rng.choice(category_names, size=n_preferred, replace=False)

        n_sessions = rng.poisson(SESSIONS_PER_USER_MEAN) + 1
        for _ in range(n_sessions):
            session_start = window_start + pd.Timedelta(
                seconds=rng.uniform(0, window_seconds)
            )
            n_items = rng.integers(*ITEMS_PER_SESSION_RANGE)
            for i in range(n_items):
                if rng.random() < P_OFF_CATEGORY or len(preferred) == 0:
                    category = rng.choice(category_names)
                else:
                    category = rng.choice(preferred)

                item_ids = categories[category]
                product_id = rng.choice(item_ids, p=category_weights[category])
                event_time = session_start + pd.Timedelta(seconds=i * rng.uniform(5, 60))

                rows.append((user_id, int(product_id), "view", event_time))
                if rng.random() < P_ADD_TO_CART:
                    cart_time = event_time + pd.Timedelta(seconds=rng.uniform(10, 120))
                    rows.append((user_id, int(product_id), "add_to_cart", cart_time))
                    if rng.random() < P_PURCHASE_GIVEN_CART:
                        purchase_time = cart_time + pd.Timedelta(
                            seconds=rng.uniform(30, 300)
                        )
                        rows.append(
                            (user_id, int(product_id), "purchase", purchase_time)
                        )

    df = pd.DataFrame(rows, columns=["user_id", "product_id", "event_type", "timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["week"] = (
        (df["timestamp"] - window_start).dt.total_seconds() // (7 * 24 * 3600)
    ).astype(int) + 1
    df["week"] = df["week"].clip(upper=weeks)
    return df


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n-users", type=int, default=2000)
    parser.add_argument("--weeks", type=int, default=14)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--out", default=str(REPO_ROOT / "data" / "interactions.parquet")
    )
    args = parser.parse_args()

    df = generate_events(args.n_users, args.weeks, args.seed)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)

    print(f"Wrote {len(df):,} events to {out_path}")
    print(df["event_type"].value_counts().to_string())
    print(f"Users: {df['user_id'].nunique():,}  Products touched: {df['product_id'].nunique():,}")
    print(f"Weeks: {df['week'].min()}-{df['week'].max()}")


if __name__ == "__main__":
    main()
