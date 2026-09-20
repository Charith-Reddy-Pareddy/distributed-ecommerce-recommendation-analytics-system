# Real review coverage for the demo catalog: findings

**Go/no-go call: no-go, as scoped.** Real user reviews for this project's
300-product catalog exist and are cheap to fetch, but contain essentially
no collaborative-filtering signal -- not because the data is small, but
because 300 items is a vanishingly thin slice of a real product category,
and real reviewers' purchases are spread across the *whole* category, not
concentrated on this specific curated subset. This isn't a workaround-able
sparsity problem the way RetailRocket's is; it's structural. See
[Numbers](#numbers) below, and the options at the end for how to proceed.

## What was explored

McAuley-Lab/Amazon-Reviews-2023's review data is not laid out the way its
dataset card's own naming convention for metadata (`raw_meta_<category>`)
might suggest. The actual tree is:

```
raw/review_categories/<Category>.jsonl   # every review ever written, 1.5-23GB/category, unsharded
benchmark/5core/rating_only/<Category>.csv   # McAuley Lab's own k-core benchmark subset
```

`rating_only` 5-core files are `user_id,parent_asin,rating,timestamp` --
real rows, no fabrication, and a completely standard, citable
preprocessing choice in recommender-systems research (not invented for
this project). They're also 15-30x smaller per category than the raw
files (e.g. Musical_Instruments: 1.56GB raw vs. 29.7MB 5-core), which is
what `scripts/fetch_amazon_reviews.py` fetches.

`parent_asin` is the exact same value `scripts/fetch_amazon_products.py`
already stores as each catalog product's `asin` -- confirmed by reading
its own `"asin": row["parent_asin"]` line -- so the join between review
rows and catalog products is exact, not fuzzy.

## Numbers

Run via `python scripts/fetch_amazon_reviews.py`, filtered to the current
300-product catalog (all 4 categories: Electronics, Toys_and_Games,
Musical_Instruments, Cell_Phones_and_Accessories):

| Metric | Value |
|---|---|
| Matched review rows | 4,840 |
| Catalog ASINs with >=1 real review | 114 / 300 |
| Distinct users who reviewed >=1 catalog item | 4,818 |
| Users who reviewed >=2 catalog items | **22** |
| Users who reviewed >=3 catalog items | **0** |

That last two rows are the number that actually matters. Item-item
cosine similarity (`services/recommendation-service/app/model.py`'s
`_cosine`) is exactly 0 for any two items with zero shared users. With
only 22 users touching 2 catalog items each (and none touching 3+), the
vast majority of the 114x114 possible item pairs share zero users --
there's essentially no co-occurrence structure for item-CF to learn, and
no shared structure for ALS to factor either.

**This is not a 5-core artifact.** To rule out "the 5-core threshold is
filtering out our sparse items," the full *raw* (non-5-core)
Musical_Instruments review file was also streamed and checked directly
(3,017,439 total reviews for that category alone, matched without ever
writing the file to disk):

| Metric (raw, Musical_Instruments only) | Value |
|---|---|
| Matched review rows | 5,026 |
| Distinct users touching our 75 MI items | 4,906 |
| Users touching >=2 of our 75 MI items | **17** |

Same picture. Using unfiltered data instead of the 5-core benchmark
roughly doubled row counts but didn't meaningfully change the multi-item
overlap -- confirming the limiting factor is the catalog's size relative
to the category (75-300 items out of a real category with tens of
thousands), not the 5-core preprocessing.

For scale context, Musical_Instruments' full 5-core file alone --
*before* filtering to our 75 items -- has 511,836 real interactions
across 57,439 users and 24,587 items. The signal is there in the real
data; it's just not concentrated on this project's specific 300-item
sample.

## What this rules out, and what it doesn't

- **Rules out:** building RQ1-3 / the hybrid / the ablations / temporal
  eval on real reviews *filtered to the current 300-item catalog, as-is*.
  There isn't enough shared-user structure among those 300 items for any
  model comparison to mean anything.
- **Doesn't rule out:** real, honest recommendation-quality research using
  this same real dataset. The 24,587-item, 57,439-user Musical_Instruments
  5-core population above is real, large, and has actual collaborative
  structure -- the RetailRocket study already proves a large real dataset
  disjoint from the live catalog is a legitimate thing for this project to
  report. The open question is whether to also try to close the id gap for
  *live serving* by growing the demo catalog itself, or to keep a
  same-dataset-not-same-catalog study like RetailRocket already is.

## Options going forward

1. **Grow the demo catalog to match real review density.** Reseed
   `product-service` from the most-reviewed items in one or more
   categories (drawn from the review data's own natural population,
   not picked first and hoped for) instead of an arbitrary 300-item
   sample. Real collaborative structure *and* a real fix for the
   live-serving id mismatch (item #3) -- but touches the live system,
   `seed_data.py`, and the dashboard demo. Biggest scope, biggest payoff.
2. **A real, same-category study, catalog left untouched.** Use one
   category's full real population (e.g. all 24,587 Musical_Instruments
   5-core items) as its own self-contained dataset -- structurally the
   same move this project already made once for RetailRocket, except this
   dataset also has real product text (title/brand/category), so unlike
   RetailRocket it can support a real content-based model and a real
   CF+content hybrid, not just CF+ALS. Satisfies RQ1-3, ablations, and
   temporal eval on 100% real data. Does **not** fix the live-serving id
   mismatch -- this dataset's items would be exactly as disjoint from the
   demo catalog as RetailRocket's are.
3. **Both:** grow the catalog *from* the same category chosen for the
   study in (2), so the offline research and the live demo end up sharing
   one real id space. Fixes item #3 for real. Larger version of (1),
   scoped by (2)'s category choice.

Smallest real dataset to start from if (2) or (3) is chosen:
Musical_Instruments -- 1.56GB raw / 29.7MB 5-core, 24,587 items, most
tractable to work with locally (ALS already trains in local-mode PySpark
for the RetailRocket study; a 24K-item, 511K-row dataset is comfortably
within that same regime, a small fraction of RetailRocket's own 2.75M
events / 235K items).
