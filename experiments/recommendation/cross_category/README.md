# Cross-category generalization (RQ5)

Does RQ2's model comparison -- item-CF beats popularity, which beats
content-based, by a wide margin -- hold on a different Amazon category,
or is it an artifact specific to this project's own demo catalog (a mix
of electronics, toys and games, musical instruments, and cell phones and
accessories)?

Runs the exact same production code as RQ2 (the real
`RecommendationEngine` via `offline_models.build_cf_engine`, the same
`metrics.py`) against a genuinely different, separately-fetched Amazon
category -- **All_Beauty**, 201 real products, none of the main
catalog's four categories -- with its own independently-generated
synthetic interaction log at a comparable users-per-product density
(2,000 users / 300 products in the main catalog ~= 6.67 users/product;
this 201-product catalog uses 1,340 users to match).

No Docker needed -- same as `offline_models.py`, this only needs
local-mode PySpark-free code (ALS and NeuMF are intentionally out of
scope here; see below).

```bash
python experiments/recommendation/cross_category/fetch_category.py  # writes catalog.json
python experiments/recommendation/cross_category/run.py
```

## Results (2026-09-20)

| Model | Precision@10 | Recall@10 | MAP@10 | NDCG@10 | Latency/request |
|---|---|---|---|---|---|
| Popularity | 0.1181 | 0.3918 | 0.1864 | 0.2905 | ~0ms |
| Content-based (TF-IDF) | 0.0143 | 0.0462 | 0.0152 | 0.0303 | 0.05ms |
| Item-CF (production code) | **0.1424** | **0.4739** | **0.3158** | **0.4269** | 116ms |

For comparison, RQ2's numbers on the main (4-category) catalog:
popularity 0.087 / 0.190, content-based 0.014 / 0.028, item-CF 0.118 /
0.327 (precision@10 / NDCG@10).

**The comparison generalizes.** Item-CF beats popularity, which beats
content-based, by a wide margin on both catalogs -- the ranking isn't
an artifact of the main catalog's particular category mix.

**Every model scores higher on the single-category catalog than on the
main one**, popularity and item-CF especially (NDCG@10 0.29 vs. 0.19,
and 0.43 vs. 0.33). The plausible mechanism: with only one category, a
user's session has no "off-category exploration" branch diluting the
signal (`generate_interactions.py`'s `P_OFF_CATEGORY` draw always
resolves to the same category here), so both the popularity ranking
and item-CF's co-occurrence structure are less noisy per user.

**Content-based stays weak either way, but for a different reason.**
On the main catalog its similarity signal is built from
category+brand+description text; on this single-category catalog,
category is now a *constant* across every product -- a real feature
became a no-op one, and TF-IDF similarity is left riding on brand and
description alone. Content-based's score barely moves (0.014 -> 0.014
precision@10), which is itself informative: category wasn't doing much
useful work for content-based even when it *did* vary, on either
catalog.

## What's deliberately out of scope

ALS and NeuMF need Spark/PyTorch training infrastructure this check
doesn't reuse -- the question here is specifically whether the
*comparison* between popularity/CF/content-based holds on different
data, not re-deriving every RQ2 number from scratch on a second
dataset. Extending ALS/NeuMF to this catalog too is real future work,
not done here.

## A real bug this surfaced

Building this exposed two genuine bugs in code every other
recommendation experiment also depends on, both now fixed (see git
history, "debug: fix fetch script and single-category gen"):

1. `scripts/fetch_amazon_products.py` (used to build the *main* demo
   catalog too) silently stopped working -- HuggingFace now serves
   large dataset files through a redirect to a signed, content-addressed
   CDN URL, and the fsspec-based streaming range-read this script used
   can't get a file size back from that CDN's response headers. Not
   something this experiment could route around; the underlying fetch
   script needed fixing.
2. `generate_interactions.py`'s per-user "preferred categories" draw
   (`rng.choice(category_names, size=n_preferred, replace=False)`,
   `n_preferred` up to 3) crashes on any catalog with fewer than 3
   categories -- fine for the main catalog's 4, silently unguarded for
   a single-category one like this.
