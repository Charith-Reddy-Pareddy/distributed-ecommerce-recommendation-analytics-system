"""Builds the demo catalog from real review density instead of
arbitrary sampling.

experiments/recommendation/REVIEWS_DATA.md found that
picking 300 items first and hoping real reviews existed for them left
almost no cross-item overlap -- 22 users out of 4,818 reviewed two of
the 300 items, none reviewed three. `scripts/rank_review_items.py`
showed why, and what fixes it: ranking each category's items by their
own real review count and taking the top of that ranking gives items
real shoppers *do* buy together -- at the top 2,000 per category, 69
-88% of users who touched the candidate set touched two or more items
in it (see rank_review_items.py's module docstring and this project's
README for the validated numbers).

This fetches real metadata (title, brand, price, description, image)
for those top-ranked ASINs from raw_meta_<category> -- *every* shard
per category, not just the first one, since items ranked by review
popularity are scattered throughout the metadata file in no particular
order. Requests a buffer above TOP_N_PER_CATEGORY (some ASINs fail the
same quality filters scripts/fetch_amazon_products.py always applied
-- missing price, no image), then trims back down to TOP_N_PER_CATEGORY,
preserving review-rank order.

Usage:

    pip install pyarrow requests
    python scripts/build_catalog_from_reviews.py
"""
import io
import json
from pathlib import Path

import pyarrow.parquet as pq
import requests

from scripts.fetch_amazon_products import MAX_PRICE, product_from_row  # noqa: F401 (MAX_PRICE re-exported for callers)
from scripts.rank_review_items import count_item_popularity

CATEGORIES = ["Electronics", "Toys_and_Games", "Musical_Instruments", "Cell_Phones_and_Accessories"]
TOP_N_PER_CATEGORY = 2000
CANDIDATE_BUFFER = 1.3  # fetch 30% more ranked ASINs than needed, since some fail metadata quality filters
OUTPUT_PATH = Path(__file__).resolve().parent.parent / "data" / "amazon_products.json"


def shard_urls(category: str) -> list[str]:
    resp = requests.get(
        f"https://huggingface.co/api/datasets/McAuley-Lab/Amazon-Reviews-2023/tree/main/raw_meta_{category}"
    )
    resp.raise_for_status()
    files = sorted(f["path"] for f in resp.json())
    return [f"https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/{f}" for f in files]


def fetch_metadata_for_asins(category: str, target_asins: set[str]) -> dict[str, dict]:
    """Scans every raw_meta shard for a category, row-group by row-group
    (not the whole multi-hundred-MB shard as one DataFrame), keeping
    only rows whose parent_asin is in target_asins. Returns {asin:
    product_dict}, stopping early once every target ASIN is found.
    """
    found: dict[str, dict] = {}
    remaining = set(target_asins)

    for url in shard_urls(category):
        if not remaining:
            break
        print(f"  scanning {url.rsplit('/', 1)[-1]} ({len(remaining)} ASINs still needed)...", flush=True)
        resp = requests.get(url, timeout=300)
        resp.raise_for_status()
        parquet_file = pq.ParquetFile(io.BytesIO(resp.content))

        for i in range(parquet_file.num_row_groups):
            if not remaining:
                break
            table = parquet_file.read_row_group(i, columns=[
                "title", "price", "images", "store", "details",
                "description", "average_rating", "rating_number", "parent_asin",
            ])
            df = table.to_pandas()
            matches = df[df["parent_asin"].isin(remaining)]
            for _, row in matches.iterrows():
                product = product_from_row(row, category)
                if product is not None:
                    found[row["parent_asin"]] = product
                remaining.discard(row["parent_asin"])

    if remaining:
        print(f"  {len(remaining)} target ASINs had no usable metadata (no price/image) -- dropped")
    return found


def build_category(category: str, top_n: int = TOP_N_PER_CATEGORY) -> list[dict]:
    print(f"=== {category} ===", flush=True)
    print("  ranking items by real review count...", flush=True)
    counts = count_item_popularity(category)
    candidate_n = int(top_n * CANDIDATE_BUFFER)
    ranked_asins = [asin for asin, _count in counts.most_common(candidate_n)]

    found = fetch_metadata_for_asins(category, set(ranked_asins))

    # Preserve review-rank order, keep only the top_n that actually
    # got usable metadata back.
    ordered = [found[asin] for asin in ranked_asins if asin in found]
    kept = ordered[:top_n]
    print(f"  {len(kept)}/{top_n} target items kept (real title+price+image)\n")
    return kept


def main() -> None:
    all_products: list[dict] = []
    for category in CATEGORIES:
        all_products.extend(build_category(category))

    print(f"Total: {len(all_products)} products across {len(CATEGORIES)} categories")
    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(all_products, indent=2))
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
