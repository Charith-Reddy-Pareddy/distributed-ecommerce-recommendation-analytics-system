"""Ranks every item in a category's real review data by how many real
interactions it has, and reports the smallest "top-N most-reviewed"
cutoff that gives real collaborative-filtering signal.

experiments/recommendation/REVIEWS_DATA.md found that
picking 300 arbitrary items first and hoping real reviews existed for
them produces almost no cross-item overlap: with items chosen
independently of review density, the odds any given reviewer's several
purchases land on two of *our* items are near zero, no matter how many
total review rows exist.

This inverts that: rank items by their own real review count within
the category, and build the candidate catalog from the top of that
ranking -- exactly the items real shoppers *do* buy together -- instead
of picking first and checking density after. `report_cf_viability` is
the same function scripts/fetch_amazon_reviews.py already uses and
already has tests for; it's imported here rather than re-implemented.

Usage:

    pip install requests
    python scripts/rank_review_items.py Musical_Instruments
    python scripts/rank_review_items.py Musical_Instruments --top 3000
"""
import argparse
import csv
from collections import Counter

import requests

from scripts.fetch_amazon_reviews import FIVE_CORE_URL, report_cf_viability


def count_item_popularity(category: str) -> Counter:
    """Pass 1: one streamed read, O(distinct_items) memory -- never
    materializes the full row list, since a category's 5-core file can
    be hundreds of MB and hundreds of thousands of rows.
    """
    url = FIVE_CORE_URL.format(category=category)
    counts = Counter()
    with requests.get(url, stream=True, timeout=600) as resp:
        resp.raise_for_status()
        reader = csv.reader(resp.iter_lines(decode_unicode=True))
        next(reader)
        for _user_id, parent_asin, _rating, _timestamp in reader:
            counts[parent_asin] += 1
    return counts


def fetch_rows_for_asins(category: str, asins: set[str]) -> list[tuple[str, str, float, int]]:
    """Pass 2: a second streamed read, this time keeping only rows for
    the candidate top-N items, to measure real multi-item overlap
    against that specific candidate set.
    """
    url = FIVE_CORE_URL.format(category=category)
    rows = []
    with requests.get(url, stream=True, timeout=600) as resp:
        resp.raise_for_status()
        reader = csv.reader(resp.iter_lines(decode_unicode=True))
        next(reader)
        for user_id, parent_asin, rating, timestamp in reader:
            if parent_asin in asins:
                rows.append((user_id, parent_asin, float(rating), int(timestamp)))
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("category", help="e.g. Musical_Instruments")
    parser.add_argument("--top", type=int, default=None, help="candidate catalog size to validate; omit to just print the popularity distribution")
    args = parser.parse_args()

    print(f"Pass 1: counting item popularity in {args.category}...", flush=True)
    counts = count_item_popularity(args.category)
    ranked = counts.most_common()
    print(f"{len(ranked)} distinct items, {sum(counts.values())} total interactions\n")

    print("Popularity distribution (cumulative interaction share at each top-N cutoff):")
    total = sum(counts.values())
    running = 0
    for n in [100, 500, 1000, 2000, 3000, 5000, 10000, len(ranked)]:
        if n > len(ranked):
            continue
        running = sum(c for _asin, c in ranked[:n])
        print(f"  top {n:>6}: {running:>8} interactions ({100*running/total:5.1f}% of all {args.category} interactions)")

    if args.top:
        candidate_asins = {asin for asin, _count in ranked[: args.top]}
        print(f"\nPass 2: validating CF viability of top {args.top} items...", flush=True)
        rows = fetch_rows_for_asins(args.category, candidate_asins)
        stats = report_cf_viability(rows, candidate_asins)
        print(f"\n=== CF viability at top {args.top} ===")
        for key, value in stats.items():
            print(f"{key}: {value}")


if __name__ == "__main__":
    main()
