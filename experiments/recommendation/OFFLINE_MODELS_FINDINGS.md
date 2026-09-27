# Offline model comparison: real reviews, real catalog

First same-catalog recommendation-quality comparison this project has
had that isn't RetailRocket (disjoint ids) or the removed synthetic
pipeline. Popularity, item-CF, and content-based, all evaluated on the
same real interaction data built in
[CATALOG_REBUILD.md](CATALOG_REBUILD.md) and joined in
`build_interactions.py`.

## Results (k=10, 150-item / 150-user sample, seed=42)

| Model | Precision@10 | Recall@10 | MAP@10 | NDCG@10 | Latency/request |
|---|---|---|---|---|---|
| Popularity | 0.0127 | 0.0485 | 0.0177 | 0.0306 | ~0ms |
| Item-CF (production code) | **0.0260** | **0.1011** | **0.0316** | **0.0630** | 1,670ms |
| Content-based (TF-IDF) | 0.0087 | 0.0280 | 0.0073 | 0.0170 | 1.8ms |

Item-CF roughly doubles popularity on every quality metric; content-based
trails even popularity, same pattern this project's earlier (now-removed)
evaluations also found -- category/brand/description text alone is a
weak proxy for what real people actually buy together.

## Why 150 items / 150 users, not the full 7,675-item catalog

This is the real finding, not just a methodology footnote. The
production `RecommendationEngine`'s pairwise `_cosine`
(services/recommendation-service/app/model.py) is fine at this
project's live traffic and catalog scale, but this dataset's
interactions are **dense**, not sparse -- popular items have tens of
thousands to (for the #1 Electronics item, the 2nd-gen Echo Dot)
233,715 shared users. That's a fundamentally different cost profile
than `scalability_benchmark.py` measured: that benchmark's "5,000
items, 357s to build the cache" result was for *sparse* synthetic
traffic. Two things were tried against the real data and both were
impractical within a reasonable session:

1. **The full O(n_items²) cache build** (`refresh_neighbor_cache()`)
   over all 7,675 items -- didn't finish in several minutes.
2. **Falling back to live per-request lookups** (`similar_items()`'s
   own cold-cache path, `_similar_items_live()`) for just 20 users --
   also didn't finish in 120s. Each `recommend_for_user` call does an
   O(n_items) cosine sweep per seed item, and each cosine call against
   a 100K+-user item is itself expensive -- the cost compounds instead
   of amortizing.

Bounding both the candidate item pool (top 150 most-interacted-with)
and the evaluated user sample (150, seeded) brought item-CF's own
measured latency to **1,670ms/request** -- itself a real, reportable
number, and a very different one than the ~163ms this project measured
at 300-item scale before. The real dataset didn't just add more rows;
it changed the shape of the problem the existing scaling story was
built around.

This is real data, real production scoring code, just not the full
catalog -- not fabricated data standing in for it. What it isn't yet:
an answer to whether the ranking (item-CF > popularity > content-based)
holds at full scale, or whether it's an artifact of evaluating on the
most-interacted-with (and therefore easiest-to-recommend) items.
That's real follow-up work, not assumed.

## A real bug this surfaced, fixed along the way

The first two evaluation runs gave different popularity/item-CF numbers
for the *same* fixed `SAMPLE_SEED=42` -- nondeterministic despite the
seed. Cause: `eligible_users()` returns a `set`, and Python randomizes
a `set`'s iteration order per process for `str` keys (security feature,
not a bug) -- so `random.Random(seed).sample(list(a_set), n)` sampled
from a differently-ordered input sequence every run, even with the same
seed. Fixed by sorting before sampling
(`sorted(eligible_users(...))`), which makes the input sequence itself
deterministic. The numbers reported above are from the run after that
fix.

## What's still open

- **Scale.** Full-catalog evaluation needs either an algorithmic change
  to the similarity computation (a real engineering task: sparse-matrix
  batch cosine, or an approximate/indexed method -- see
  `scalability_benchmark.py`'s own "next bottleneck" discussion) or
  accepting a bounded sample as the permanent methodology, clearly
  labeled as such either way.
- **Statistical significance.** 150 users is a sample, not the full
  4,354 eligible; no bootstrap CI yet on these specific numbers (the
  machinery exists and is tested -- `bootstrap.py` -- just not wired to
  this comparison).
- **The hybrid** hasn't been built on this real data yet.

## ALS, added later: real, full-scale, but not yet a fair comparison

`train_catalog_als.py` trains Spark MLlib ALS on the same real
interactions, sharing the live catalog's product ids (unlike
`jobs/als-training/`'s RetailRocket job). Unlike the popularity/item-CF/
content-based comparison above, ALS's matrix factorization doesn't have
the pairwise-cosine cost problem, so this ran on the **full** catalog
and the **full** eligible test set, not a bounded sample:

| Model | Precision@10 | Users evaluated | Item pool |
|---|---|---|---|
| ALS (production-scale) | 0.0045 | 225,873 | full ~7,675 |
| Item-CF (bounded sample, above) | 0.0260 | 150 | top 150 |

**These two numbers are not a fair head-to-head yet** -- ALS was
evaluated against every real item in the catalog (including thousands
of long-tail items with little signal), while item-CF was only
evaluated against the 150 *most*-interacted-with items, which is
inherently an easier target. The honest comparison needs both models
run at the same scope; that's real follow-up work, not assumed to
favor either model as things stand.

Three real bugs surfaced and got fixed getting this to run at all,
each a genuine first-encounter (this project's only prior ALS job
always ran inside Docker with a different Python version, never
locally, and never against a real id space this dense):

1. **`pyspark.ml` doesn't import on Python 3.12** -- `distutils` was
   removed from the stdlib entirely, and setuptools' own shim
   (`distutils-precedence.pth`) wasn't actually firing at interpreter
   startup in this environment. Fixed by explicitly invoking
   `_distutils_hack.add_shim()` in the script itself, rather than
   depending on that ambient behavior (which had already silently
   stopped working once between two sessions with no code change).
2. **Kryo buffer overflow building the user-id index** -- ~1.6M
   distinct real reviewer ids is a much bigger string-indexing job than
   this project's only other precedent (RetailRocket's `visitorid` is
   already numeric, never indexed at all). Fixed with
   `spark.kryoserializer.buffer.max=512m`.
3. **Driver OOM inside `ALS.fit()`** -- default local-mode heap (1g)
   wasn't enough for this real, dense interaction graph. Fixed with
   `spark.driver.memory=6g`.
4. **`precision_at_k` grouped by the wrong column** -- a real code bug,
   not an environment one: ALS is trained on a `StringIndexer`-derived
   `user_id_idx` column (string user ids need indexing; RetailRocket's
   job never needed this), but `precision_at_k` still grouped by
   `user_id`, which `recommendForUserSubset` doesn't recognize.
   Parameterized the function on `user_col` and added a regression
   test that exercises exactly this mismatch.
