"""Check how well real Amazon reviews, catalog rows, and IDs join."""

import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ecommerce_analytics.als_export import load_catalog_map


def audit_amazon_sources(
    reviews_path: str | Path,
    catalog_path: str | Path,
    catalog_map_path: str | Path,
) -> dict[str, Any]:
    """Report source quality and join coverage without changing source data."""
    catalog = json.loads(Path(catalog_path).read_text(encoding="utf-8"))
    if not isinstance(catalog, list):
        raise ValueError("Amazon catalog must be a JSON array")

    catalog_asins = [
        str(row.get("asin", "")).strip().upper()
        for row in catalog
        if isinstance(row, dict) and row.get("asin")
    ]
    if len(catalog_asins) != len(catalog):
        raise ValueError("Amazon catalog contains a row without an ASIN")
    if len(set(catalog_asins)) != len(catalog_asins):
        raise ValueError("Amazon catalog contains duplicate ASINs")

    id_map = load_catalog_map(catalog_map_path)
    map_asins = {asin.strip().upper() for asin in id_map}
    if len(map_asins) != len(id_map):
        raise ValueError("ASIN-to-product-ID map contains duplicate normalized ASINs")

    review_rows = 0
    valid_rows = 0
    mapped_rows = 0
    invalid_rows = 0
    review_asins: set[str] = set()
    mapped_asins: set[str] = set()
    min_ts: int | None = None
    max_ts: int | None = None

    with Path(reviews_path).open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        required = {"user_id", "asin", "rating", "timestamp"}
        missing = required - set(reader.fieldnames or ())
        if missing:
            raise ValueError(f"Amazon reviews are missing columns: {', '.join(sorted(missing))}")

        for row in reader:
            review_rows += 1
            asin = (row.get("asin") or "").strip().upper()
            user = (row.get("user_id") or "").strip()
            try:
                rating = float(row.get("rating", ""))
                timestamp = int(row.get("timestamp", ""))
            except (TypeError, ValueError):
                invalid_rows += 1
                continue
            if not asin or not user or not 1 <= rating <= 5 or timestamp < 0:
                invalid_rows += 1
                continue

            valid_rows += 1
            review_asins.add(asin)
            if asin in map_asins:
                mapped_rows += 1
                mapped_asins.add(asin)
            min_ts = timestamp if min_ts is None else min(min_ts, timestamp)
            max_ts = timestamp if max_ts is None else max(max_ts, timestamp)

    def utc(ts: int | None) -> str | None:
        if ts is None:
            return None
        return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")

    catalog_set = set(catalog_asins)
    return {
        "reviews_file": Path(reviews_path).name,
        "catalog_file": Path(catalog_path).name,
        "catalog_map_file": Path(catalog_map_path).name,
        "review_rows": review_rows,
        "valid_review_rows": valid_rows,
        "invalid_review_rows": invalid_rows,
        "review_asins": len(review_asins),
        "mapped_review_rows": mapped_rows,
        "unmapped_review_rows": valid_rows - mapped_rows,
        "review_asins_missing_from_map": len(review_asins - map_asins),
        "catalog_products": len(catalog),
        "catalog_asins_missing_from_map": len(catalog_set - map_asins),
        "map_asins_missing_from_catalog": len(map_asins - catalog_set),
        "mapped_review_asins": len(mapped_asins),
        "map_product_ids": len(set(id_map.values())),
        "timestamp_unit": "milliseconds",
        "timestamp_min_utc": utc(min_ts),
        "timestamp_max_utc": utc(max_ts),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reviews", type=Path, required=True)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--catalog-map", type=Path, required=True)
    args = parser.parse_args()
    report = audit_amazon_sources(args.reviews, args.catalog, args.catalog_map)
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
