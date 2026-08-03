"""Extracts a sample of real Amazon product records from the
McAuley-Lab/Amazon-Reviews-2023 dataset (a real, public academic
dataset -- see https://amazon-reviews-2023.github.io/) and writes them
to data/amazon_products.json for scripts/seed_data.py to load.

This replaces the earlier synthetic, made-up demo catalog
(fabricated names like "Electronics Item 1", fake brands, stock
placeholder images) with real product titles, brands, prices, images,
ratings, and Amazon Standard Identification Numbers (ASINs).

Doesn't download full category files (some are 1.6M+ items and
multiple GB): reads only the first Parquet row group of each category
via HTTP range requests (pyarrow + fsspec), which is enough rows to
sample a few hundred products without pulling gigabytes of data.

Usage:

    pip install pyarrow fsspec aiohttp requests
    python scripts/fetch_amazon_products.py

Only needs to be re-run if you want a different sample; the output is
already committed to the repo at data/amazon_products.json.
"""
import json
from pathlib import Path

import fsspec
import pyarrow.parquet as pq
import requests

CATEGORIES = ["Electronics", "Toys_and_Games", "Musical_Instruments", "Cell_Phones_and_Accessories"]
PER_CATEGORY = 75
MAX_PRICE = 2000.0
OUTPUT_PATH = Path(__file__).resolve().parent.parent / "data" / "amazon_products.json"


def first_shard_url(category: str) -> str:
    resp = requests.get(
        f"https://huggingface.co/api/datasets/McAuley-Lab/Amazon-Reviews-2023/tree/main/raw_meta_{category}"
    )
    resp.raise_for_status()
    files = sorted(f["path"] for f in resp.json())
    return f"https://huggingface.co/datasets/McAuley-Lab/Amazon-Reviews-2023/resolve/main/{files[0]}"


def extract_category(category: str, fs) -> list[dict]:
    url = first_shard_url(category)
    print(f"Reading {category} from {url.rsplit('/', 1)[-1]}...")

    with fs.open(url) as f:
        parquet_file = pq.ParquetFile(f)
        table = parquet_file.read_row_group(0)

    df = table.to_pandas()
    candidates = df[(df["title"].str.len() > 5) & df["price"].notna() & (df["price"] != "None")]
    candidates = candidates[candidates["images"].apply(lambda x: x is not None and len(x["large"]) > 0)]

    products = []
    for _, row in candidates.iterrows():
        try:
            price = float(row["price"])
        except (ValueError, TypeError):
            continue
        if not (0 < price <= MAX_PRICE):
            continue

        brand = row["store"] or ""
        if not brand:
            try:
                details = json.loads(row["details"]) if row["details"] else {}
                brand = details.get("Brand", "")
            except (json.JSONDecodeError, TypeError):
                pass

        products.append(
            {
                "name": row["title"][:200],
                "category": category.lower().replace("_", " "),
                "price": round(price, 2),
                "description": (
                    row["description"][0]
                    if row["description"] is not None and len(row["description"]) > 0
                    else row["title"]
                )[:500],
                "brand": brand[:80] if brand else "Unknown",
                "average_rating": float(row["average_rating"]) if row["average_rating"] else 0.0,
                "rating_number": int(row["rating_number"]) if row["rating_number"] else 0,
                "image": row["images"]["large"][0],
                "asin": row["parent_asin"],
            }
        )
        if len(products) >= PER_CATEGORY:
            break

    print(f"  extracted {len(products)} from {category}")
    return products


def main() -> None:
    fs = fsspec.filesystem("https")
    all_products: list[dict] = []
    for category in CATEGORIES:
        all_products.extend(extract_category(category, fs))

    print(f"\nTotal: {len(all_products)} products")
    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(all_products, indent=2))
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
