"""Fetches real user reviews from McAuley-Lab/Amazon-Reviews-2023,
filtered to whatever ASINs are in data/amazon_products.json, and
reports how much real collaborative-filtering signal that intersection
actually contains.

The review data is NOT under `raw_review_<category>` like the dataset
card's naming convention for metadata (`raw_meta_<category>`) might
suggest -- the tree is `raw/review_categories/<Category>.jsonl`, one
huge (1.5-23GB) unsharded file per category with every review ever
written for it. Rather than stream-filtering that whole file, this
pulls the dataset's own `benchmark/5core/rating_only/<Category>.csv`
instead: McAuley Lab's own standard k-core benchmark preprocessing
(users and items each guaranteed >=5 ratings, real rows only --
user_id, parent_asin, rating, timestamp), 15-30x smaller per category
and a completely standard, citable preprocessing choice in recommender
research, not something invented for this project.

`parent_asin` is the same field scripts/fetch_amazon_products.py
already stores as each catalog product's `asin` (see its `"asin":
row["parent_asin"]` line) -- so filtering review rows to
`row["parent_asin"] in catalog_asins` is an exact join, no fuzzy
matching needed.

Usage:

    pip install requests
    python scripts/fetch_amazon_reviews.py
"""
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path

import requests

CATEGORIES = ["Electronics", "Toys_and_Games", "Musical_Instruments", "Cell_Phones_and_Accessories"]
CATALOG_PATH = Path(__file__).resolve().parent.parent / "data" / "amazon_products.json"
OUTPUT_PATH = Path(__file__).resolve().parent.parent / "data" / "catalog_reviews.csv"
FIVE_CORE_URL = (
    "https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/"
    "resolve/main/benchmark/5core/rating_only/{category}.csv"
)


def load_catalog_asins(catalog_path=CATALOG_PATH) -> set[str]:
    products = json.loads(catalog_path.read_text())
    return {p["asin"] for p in products}


def fetch_category_rows(category: str, catalog_asins: set[str]) -> list[tuple[str, str, float, int]]:
    """Streams one category's 5-core CSV and keeps only rows whose
    parent_asin is in the catalog -- the file never touches disk in
    full, only the matched rows are kept in memory.
    """
    url = FIVE_CORE_URL.format(category=category)
    matched = []
    with requests.get(url, stream=True, timeout=600) as resp:
        resp.raise_for_status()
        lines = resp.iter_lines(decode_unicode=True)
        reader = csv.reader(lines)
        next(reader)  # header: user_id,parent_asin,rating,timestamp
        for user_id, parent_asin, rating, timestamp in reader:
            if parent_asin in catalog_asins:
                matched.append((user_id, parent_asin, float(rating), int(timestamp)))
    return matched


def report_cf_viability(rows: list[tuple[str, str, float, int]], catalog_asins: set[str]) -> dict:
    """The metric that actually matters for collaborative filtering
    isn't row count or matched-ASIN count -- it's how many distinct
    users reviewed *more than one* catalog item. Two items with zero
    shared users have cosine similarity 0 by definition
    (services/recommendation-service/app/model.py's own `_cosine`),
    so a dataset with near-zero multi-item users has near-zero CF
    signal regardless of its total row count.
    """
    user_items = defaultdict(set)
    matched_asins = set()
    for user_id, asin, _rating, _ts in rows:
        user_items[user_id].add(asin)
        matched_asins.add(asin)

    multi_item_users = {u: items for u, items in user_items.items() if len(items) >= 2}
    overlap_distribution = Counter(len(items) for items in multi_item_users.values())

    return {
        "matched_rows": len(rows),
        "matched_asins": len(matched_asins),
        "catalog_asins": len(catalog_asins),
        "distinct_users": len(user_items),
        "multi_item_users": len(multi_item_users),
        "multi_item_overlap_distribution": dict(sorted(overlap_distribution.items())),
    }


def main() -> None:
    catalog_asins = load_catalog_asins()
    print(f"Catalog: {len(catalog_asins)} ASINs across {len(CATEGORIES)} categories\n")

    all_rows = []
    for category in CATEGORIES:
        print(f"Fetching {category} 5-core rating_only file...", flush=True)
        rows = fetch_category_rows(category, catalog_asins)
        print(f"  matched {len(rows)} review rows", flush=True)
        all_rows.extend(rows)

    stats = report_cf_viability(all_rows, catalog_asins)
    print("\n=== Coverage report (current 300-item catalog) ===")
    for key, value in stats.items():
        print(f"{key}: {value}")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "asin", "rating", "timestamp"])
        writer.writerows(all_rows)
    print(f"\nWrote {len(all_rows)} matched rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
