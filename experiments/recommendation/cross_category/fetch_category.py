"""RQ5: does the RQ2 model comparison (popularity vs. item-CF vs.
content-based) generalize to a different Amazon product category, or is
it specific to this project's own demo catalog?

Fetches "All_Beauty" -- a real McAuley-Lab/Amazon-Reviews-2023 category
none of the demo catalog's four categories touch (Electronics,
Toys_and_Games, Musical_Instruments, Cell_Phones_and_Accessories) --
into its own, separate catalog file. Reuses
scripts/fetch_amazon_products.py's extraction logic directly (same
source, same product schema, same filtering) rather than
reimplementing it, so this is genuinely the same pipeline pointed at
different raw data, not a parallel one that could quietly drift from it.

This catalog is standalone research data -- it is never loaded into
product-service or the live demo catalog, only used by
experiments/recommendation/cross_category/run.py.

Usage:

    pip install pyarrow requests
    python experiments/recommendation/cross_category/fetch_category.py
"""
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))
from scripts.fetch_amazon_products import extract_category  # noqa: E402

CATEGORY = "All_Beauty"
SAMPLE_SIZE = 300  # comparable scale to the demo catalog's own ~300-600 products
OUTPUT_PATH = Path(__file__).resolve().parent / "catalog.json"


def main() -> None:
    products = extract_category(CATEGORY, limit=SAMPLE_SIZE)

    print(f"\nTotal: {len(products)} products from {CATEGORY}")
    OUTPUT_PATH.write_text(json.dumps(products, indent=2))
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
