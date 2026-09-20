"""Extracts a sample of real Amazon product records from the
McAuley-Lab/Amazon-Reviews-2023 dataset and writes them to
data/amazon_products.json for scripts/seed_data.py to load.

Downloads one Parquet shard per category (tens of MB, not the full
multi-GB category file) and reads only its first row group -- enough
to sample a few hundred products without needing the whole shard in
memory as a DataFrame.

Used to stream this via fsspec's HTTP filesystem + range requests
directly against the `resolve/main/...` URL, without downloading the
whole shard first. That stopped working -- HuggingFace now serves
large files through a redirect to a signed, content-addressed (Xet)
CDN URL, and fsspec's HTTP filesystem can't get a file size back from
that CDN's response headers the way it could from the old direct LFS
URLs (confirmed: even re-pointing it at the already-resolved final CDN
URL still raises FileNotFoundError). A plain `requests.get` following
the redirect, read into memory, works reliably -- these shards are
tens of MB, not the multi-GB full category files, so buffering one
fully is fine.

Usage:

    pip install pyarrow requests
    python scripts/fetch_amazon_products.py
"""
import io
import json
from pathlib import Path

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


def product_from_row(row, category: str) -> dict | None:
    """Converts one raw_meta parquet row into a catalog product dict,
    or None if it fails the same quality filters extract_category()
    always applied (real title, real price, at least one real image)
    -- shared so scripts/build_catalog_from_reviews.py can apply the
    identical extraction logic to a targeted set of ASINs instead of
    just the first shard's first row group.
    """
    if not row["title"] or len(row["title"]) <= 5:
        return None
    if row["price"] is None or row["price"] == "None":
        return None
    images = row["images"]
    if images is None or len(images["large"]) == 0:
        return None
    try:
        price = float(row["price"])
    except (ValueError, TypeError):
        return None
    if not (0 < price <= MAX_PRICE):
        return None

    brand = row["store"] or ""
    if not brand:
        try:
            details = json.loads(row["details"]) if row["details"] else {}
            brand = details.get("Brand", "")
        except (json.JSONDecodeError, TypeError):
            pass

    return {
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


def extract_category(category: str, limit: int = PER_CATEGORY) -> list[dict]:
    url = first_shard_url(category)
    print(f"Reading {category} from {url.rsplit('/', 1)[-1]}...")

    resp = requests.get(url, timeout=180)
    resp.raise_for_status()
    parquet_file = pq.ParquetFile(io.BytesIO(resp.content))
    table = parquet_file.read_row_group(0)

    df = table.to_pandas()
    products = []
    for _, row in df.iterrows():
        product = product_from_row(row, category)
        if product is not None:
            products.append(product)
        if len(products) >= limit:
            break

    print(f"  extracted {len(products)} from {category}")
    return products


def main() -> None:
    all_products: list[dict] = []
    for category in CATEGORIES:
        all_products.extend(extract_category(category))

    print(f"\nTotal: {len(all_products)} products")
    OUTPUT_PATH.parent.mkdir(exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(all_products, indent=2))
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
