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
Volinsky's own ALS formulation supports, is the reference for the
matched raw / binary / squared / uniform ablation.

The ASIN mapping is archived in `data/asin_product_id_map.json`, and
the recommendation metadata snapshot is archived in
`data/recommendation_catalog_snapshot.json`. To refresh the mapping from
the live catalog, pass `--refresh-mapping` explicitly.

Usage (after scripts/fetch_amazon_reviews.py has run):

    pip install pyarrow requests
    python -m experiments.recommendation.build_interactions
"""
import argparse
import csv
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

PRODUCT_SERVICE_URL = "http://localhost:8001"
CATALOG_REVIEWS_PATH = Path(__file__).resolve().parent.parent.parent / "data" / "catalog_reviews.csv"
OUTPUT_PATH = Path(__file__).resolve().parent / "interactions.parquet"
MAPPING_PATH = Path(__file__).resolve().parents[2] / "data" / "asin_product_id_map.json"


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


def write_asin_product_id_map(mapping: dict[str, int], output_path: Path = MAPPING_PATH) -> None:
    """Persist the exact catalog join as a sorted, reviewable JSON object."""
    if not mapping:
        raise ValueError("mapping must not be empty")
    if any(not isinstance(asin, str) or not asin for asin in mapping):
        raise ValueError("mapping keys must be non-empty ASIN strings")
    if any(isinstance(product_id, bool) or not isinstance(product_id, int) or product_id <= 0
           for product_id in mapping.values()):
        raise ValueError("product IDs must be positive integers")
    if len(set(mapping.values())) != len(mapping):
        raise ValueError("product IDs must be unique")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(dict(sorted(mapping.items())), indent=2) + "\n")


def load_asin_product_id_map(mapping_path: Path = MAPPING_PATH) -> dict[str, int]:
    """Load and validate an archived ASIN-to-product-ID mapping."""
    data = json.loads(mapping_path.read_text())
    if not isinstance(data, dict) or not data:
        raise ValueError("mapping file must contain an ASIN-to-product-ID object")
    if any(not isinstance(asin, str) or not asin for asin in data):
        raise ValueError("mapping keys must be non-empty ASIN strings")
    if any(isinstance(product_id, bool) or not isinstance(product_id, int) or product_id <= 0
           for product_id in data.values()):
        raise ValueError("product IDs must be positive integers")
    if len(set(data.values())) != len(data):
        raise ValueError("product IDs must be unique")
    return data


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
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mapping-file", type=Path, default=MAPPING_PATH)
    parser.add_argument("--refresh-mapping", action="store_true",
                        help="fetch the current live catalog and replace the archived mapping")
    args = parser.parse_args()
    if args.refresh_mapping:
        print("Fetching asin -> product_id mapping from the live catalog...", flush=True)
        asin_to_product_id = fetch_asin_to_product_id()
        write_asin_product_id_map(asin_to_product_id, args.mapping_file)
    else:
        asin_to_product_id = load_asin_product_id_map(args.mapping_file)
    print(f"  {len(asin_to_product_id)} ASINs in mapping {args.mapping_file}")

    print(f"Joining {CATALOG_REVIEWS_PATH.name} against it...", flush=True)
    rows = build_interactions(asin_to_product_id)
    print(f"  {len(rows)} interactions joined")

    write_parquet(rows)
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
