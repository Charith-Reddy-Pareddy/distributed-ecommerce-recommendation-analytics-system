# The catalog, rebuilt from real review density

**Chosen path (from the options in [REVIEWS_DATA.md](REVIEWS_DATA.md)):
grow the demo catalog to match real review density.** Not a hand-picked
sample checked for coverage
after the fact -- items selected *because* they have real review
density, which also happens to fix the live id-mismatch problem (item
#3 from the external review), since the catalog now shares an id space
with data that actually has collaborative structure.

## Selection method

`scripts/rank_review_items.py` ranks every item in a category's real
5-core review data by its own interaction count (`count_item_popularity`),
then validates a candidate cutoff's real collaborative signal with the
same `report_cf_viability` function used in
[REVIEWS_DATA.md](REVIEWS_DATA.md) (`scripts/fetch_amazon_reviews.py`,
already tested in `tests/test_fetch_amazon_reviews.py`). Two categories
were checked directly:

| Category | Candidate size | Distinct users | Users w/ 2+ items | Overlap rate |
|---|---|---|---|---|
| Musical_Instruments | top 2,000 | 54,462 | 48,036 | 88.2% |
| Electronics | top 2,000 | 1,360,474 | 934,711 | 68.7% |

Compare to the earlier arbitrary-300-item result: 4,818 users, 22 with
2+ items (0.5%). Selecting by review density instead of picking first
turns "almost no signal" into "most users who touch the candidate set
touch multiple items in it."

## What got built

`scripts/build_catalog_from_reviews.py` takes the top 2,000
most-reviewed items per category (a 30% buffer over that, since some
ASINs lack the price/image data `scripts/fetch_amazon_products.py`'s
quality filters already required), fetches their real metadata from
every `raw_meta_<category>` shard (not just the first one -- items
ranked by review popularity are scattered throughout the metadata
file, not concentrated in any single shard), and writes
`data/amazon_products.json`.

`scripts/fetch_amazon_products.py` was refactored first (`product_from_row`
extracted as a standalone, now-tested function) so both scripts share
one extraction path rather than duplicating the quality-filter logic.

**Result:**

| Category | Target | Kept (passed quality filters) |
|---|---|---|
| Electronics | 2,000 | 1,921 |
| Toys_and_Games | 2,000 | 2,000 |
| Musical_Instruments | 2,000 | 2,000 |
| Cell_Phones_and_Accessories | 2,000 | 1,754 |
| **Total** | 8,000 | **7,675** |

Every product is real: real title, real brand, real price, real
description, real image, real ASIN, real Amazon rating count -- the
\#1 Electronics item by review count is the 2nd-gen Echo Dot, 233,715
ratings, exactly as recognizable a real product as that implies.

## Live stack

Reseeding meant a full reset (`docker compose down -v` + fresh
`up` + `seed_data.py`), not layering the new catalog on top of the
old one: the old 300-item catalog's product ids were referenced by
~29 hours of accumulated Kafka event history, and leaving that mixed
in would give `recommendation-service`'s in-memory CF model a set of
phantom old-id items it could never enrich once the old products were
gone -- the same orphaned-data problem the stale HBase table cleanup
already dealt with earlier, not something worth reintroducing here.

Verified against the fresh stack, not just asserted:
- `python scripts/seed_data.py` -- 7,675 products, 20 users, ~7,675
  simulated events (scaled with catalog size; was a hardcoded 300)
- `curl /products/search?q=headphones` -- real matches (Behringer
  HPX2000, etc.)
- `curl /recommendations/popular`, `/recommendations/1` -- both return
  real enriched products from the new catalog
- `pytest tests/integration` -- 13/13 passing against the reseeded stack

## What's still stale (deliberately deferred)

`README.md`, `docs/ARCHITECTURE.md`, `docs/RUNNING_LOCALLY.md`,
`site/index.html`, and the dashboard template still say "300 Amazon
products" -- left as-is intentionally. A full documentation pass
happens once the rest of the real-data pipeline (interactions ETL,
CF/ALS/hybrid models, live serving) is complete, so numbers get
written once against final results instead of being updated
piecemeal and rewritten again in a few days.
`experiments/recommendation/scalability_benchmark.py`'s "matches this
project's real catalog size (300 items)" framing is similarly
unchanged for now -- rerunning that experiment at the new catalog
scale is real follow-up work, not a find-and-replace.
