"""Joins real review data (scripts/fetch_amazon_reviews.py's output)
to the live catalog's actual product ids, producing the
(user_id, product_id, weight, timestamp) interactions table every
downstream recommendation experiment in this directory trains and
evaluates on.

**Why query product-service instead of just using the JSON array
index.** `data/amazon_products.json`'s array order happens to match
product-service's sequential ids (index 0 -> id 1, etc.) as of the
last `scripts/seed_data.py` run, but that's an artifact of insertion
order, not a guarantee -- a future reseed, a partial reseed, or a
products table that stops being purely sequential would silently break
a hardcoded `index + 1` mapping with no error, just wrong joins. Paging
through `GET /products` and reading each product's own `asin` field
back is the actual source of truth, and it's cheap (a few dozen
requests for ~7,675 products at limit=100).

**Weight scheme.** Real Amazon star ratings (1.0-5.0) are used
directly as the ALS/CF confidence weight, not remapped onto the
production view=1/cart=3/purchase=5 scale -- a review rating isn't an
engagement-funnel stage the way a view or a cart-add is (Amazon
reviews require a purchase already), it's a quality signal on an
interaction that already happened, and rescaling it onto a funnel
metaphor it doesn't fit would be an arbitrary transform dressed up as
a methodology choice. Using the raw rating as confidence, following
the same "explicit rating as implicit confidence" framing Hu-Koren-
Volinsky's own ALS formulation supports, is the simplest defensible
default -- and the one RQ1's ablation (binary / squared / exponential
alternatives) will be measured against, not assumed to be optimal.

Usage (needs the live stack up and seeded, and
scripts/fetch_amazon_reviews.py already run):

    pip install pyarrow requests
    python -m experiments.recommendation.build_interactions
"""
import csv
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

PRODUCT_SERVICE_URL = "http://localhost:8001"
CATALOG_REVIEWS_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "catalog_reviews.csv"
OUTPUT_PATH = Path(__file__).resolve().parent / "interactions.parquet"


def fetch_asin_to_product_id(product_service_url: str = PRODUCT_SERVICE_URL) -> dict[str, int]:
    """Pages through every product this project's live catalog
    actually has, reading each one's own `asin` field back rather than
    assuming any particular insertion order.
    """
    mapping: dict[str, int] = {}
    skip = 0
    limit = 100
    while True:
        resp = requests.get(f"{product_service_url}/products", params={"skip": skip, "limit": limit}, timeout=30)
        resp.raise_for_status()
        page = resp.json()
        if not page:
            break
        for product in page:
            if product.get("asin"):
                mapping[product["asin"]] = product["id"]
        skip += limit
    return mapping


def build_interactions(
    asin_to_product_id: dict[str, int], reviews_path: Path = CATALOG_REVIEWS_PATH
) -> list[tuple[str, int, float, int]]:
    """Reads catalog_reviews.csv (user_id, asin, rating, timestamp),
    drops any row whose asin isn't in the live catalog (there
    shouldn't be any, since fetch_amazon_reviews.py already filtered
    to catalog ASINs, but a reseed between the two runs could change
    that), and returns (user_id, product_id, weight, timestamp) rows
    with the asin resolved to product-service's actual integer id.
    """
    rows: list[tuple[str, int, float, int]] = []
    with reviews_path.open() as f:
        reader = csv.DictReader(f)
        for record in reader:
            product_id = asin_to_product_id.get(record["asin"])
            if product_id is None:
                continue
            rows.append((record["user_id"], product_id, float(record["rating"]), int(record["timestamp"])))
    return rows


def write_parquet(rows: list[tuple[str, int, float, int]], output_path: Path = OUTPUT_PATH) -> None:
    table = pa.table(
        {
            "user_id": [r[0] for r in rows],
            "product_id": [r[1] for r in rows],
            "weight": [r[2] for r in rows],
            "timestamp": [r[3] for r in rows],
        }
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, output_path)


def main() -> None:
    print("Fetching asin -> product_id mapping from the live catalog...", flush=True)
    asin_to_product_id = fetch_asin_to_product_id()
    print(f"  {len(asin_to_product_id)} products in the live catalog")

    print(f"Joining {CATALOG_REVIEWS_PATH.name} against it...", flush=True)
    rows = build_interactions(asin_to_product_id)
    print(f"  {len(rows)} interactions joined")

    write_parquet(rows)
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
