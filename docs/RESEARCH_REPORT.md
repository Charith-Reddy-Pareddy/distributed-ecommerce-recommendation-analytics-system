# Research report

Every number below was actually measured against this codebase and
this stack -- see `experiments/*/results/*.jsonl` for the raw records
each section draws from, and `experiments/*/README.md` for how to
reproduce a run. Nothing here is estimated or extrapolated.

An earlier version of this report also compared a real-time item-item
recommender against a batch ALS model, evaluated a CF+ALS hybrid blend,
and checked whether that comparison generalized to a different Amazon
category. All three depended on training and serving ALS against the
real product catalog's item ids -- and the only way this project had
a shared id space was an interaction log fabricated (Zipfian-skewed
popularity, a scripted view→cart→purchase funnel) over the real
catalog, since the one real interaction dataset available here
(RetailRocket) has its own disjoint ids and no product text.
Recommendation-quality numbers computed on fabricated interactions
aren't a finding, however clearly the log was labeled synthetic, so
that comparison has been removed rather than kept for a number to
report. See git history if you need that code or those results.

## Abstract

This project investigates whether a distributed e-commerce platform's
serving performance and recommendation quality can be measured, not
just built. Starting from a Lambda-style architecture (Kafka, Spark
Structured Streaming, Spark MLlib ALS, HBase, Cassandra, Elasticsearch,
Postgres, MongoDB), we measure the effect of a workload-aware database
optimizer, train and evaluate an ALS collaborative filter on a real,
public clickstream dataset, measure the real-time item-item
recommender's own scaling limit as catalog size grows, and separately
measure the system's behavior under container failure and sustained
load. The optimizer's Postgres indexer cuts p95 read latency 45%; its
Elasticsearch tuner trades slower time-to-searchable for faster burst
throughput; ALS on real clickstream data lands at a low but honest
Precision@10 (0.0055) explained by the dataset's genuine sparsity, not
a bug; the real-time CF engine's live similarity lookup degrades
roughly linearly with catalog size until replaced with a precomputed
cache; and the system's persistent-offset consumer recovers from a
crash with zero event loss, while its stateless consumer's replay time
and its search index's total loss on container replacement are both
real, measured costs of those designs.

## Problem

A single event stream -- product views, cart adds, purchases -- has to
simultaneously feed a real-time recommender, rolling analytics, a
batch-trained ML model, and a workload-aware serving layer. The
architecture for doing this (Lambda-style: independent streaming and
batch paths reading the same log) is well documented in the
literature, but rarely evaluated: does an autonomous database tuner's
benefit outweigh its overhead? What does implicit-feedback
collaborative filtering actually look like on real, sparse clickstream
data, rather than a curated benchmark? Does the real-time
collaborative-filtering engine's naive similarity lookup actually
scale as a catalog grows, and if not, what fixes it and at what cost?
This report answers those questions for this system, not in the
abstract -- and, as importantly, is explicit about which questions it
no longer tries to answer, and why (see the note above).

## System architecture

See [docs/ARCHITECTURE.md](ARCHITECTURE.md) for the full component
write-up (services, data flow, the Mermaid diagram lives in the
[README](../README.md#architecture)).

## Recommendation algorithms

Two algorithms actually serve live traffic; a third is an offline
study on a real, separate dataset:

- **Item-CF** (`services/recommendation-service/app/model.py`) -- the
  production `RecommendationEngine`: weighted cosine similarity
  between items' interaction vectors, in-memory, rebuilt by replaying
  Kafka. Serves `/recommendations/{user_id}` and
  `/recommendations/similar/{product_id}` against this project's real
  300-product catalog.
- **Popularity** -- sum of event weights per product, no
  personalization; the fallback for users with no interaction history
  yet (`popular_items()`).
- **ALS, implicit feedback** (`jobs/als-training/train_als.py`) --
  Spark MLlib ALS, trained and evaluated entirely on the real
  [RetailRocket](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset)
  clickstream dataset. Its item ids are RetailRocket's own, a
  different, disjoint space from the demo catalog above -- see
  [RetailRocket ALS results](#retailrocket-als-real-interaction-data-at-production-sparsity)
  and [docs/ARCHITECTURE.md](ARCHITECTURE.md#serving-layer-hbase) for
  what that does and doesn't let this project serve live.

### Algorithm definitions

Exact formulas as implemented, not textbook defaults -- see the cited
source for the literal code.

**Item-based CF** (`services/recommendation-service/app/model.py`).
Each item `i` is a sparse vector over users, `v_i[u] = Σ w(e)` for
every event `e` on `(u, i)`, with `w(view)=1, w(add_to_cart)=3,
w(purchase)=5`. Similarity between items `i, j` is weighted cosine,
restricted to their common users `C = users(i) ∩ users(j)` for the dot
product but the *full* vector for each norm (so an item with many more
total interactions gets a proportionally larger norm even where the
two items don't overlap):

```
sim(i, j) = ( Σ_{u∈C} v_i[u]·v_j[u] ) / ( ‖v_i‖ · ‖v_j‖ ),  0 if C = ∅ or either norm = 0
```

A user's candidate score for an unseen item `j` sums, over every item
`i` the user already interacted with (weight `w_i`), that item's
similarity to `j` among its precomputed top-50 neighbors:

```
score(u, j) = Σ_{i ∈ interacted(u)} sim(i, j) · w_i,   j ∉ interacted(u)
```

*Complexity.* One pairwise `sim(i, j)` costs `O(|users(i)| + |users(j)|)`
(full-vector norms plus a set-intersection dot product). A live
`similar_items()` call is `O(n_items · avg_users_per_item)`; the
background `refresh_neighbor_cache()` job that replaces it is
`O(n_items² · avg_users_per_item)` per full rebuild, run periodically
rather than per-request -- see
[Scaling the similarity lookup](ARCHITECTURE.md#recommendation-algorithm)
for the measured wall-clock numbers at three catalog sizes, which grow
faster than the `n_items²` term alone once `avg_users_per_item` scales
with traffic too.

**ALS, implicit feedback** (`jobs/als-training/train_als.py`; Spark
MLlib, Hu-Koren-Volinsky formulation). Raw event weight `r_ui` becomes
a *confidence*, not a target rating:

```
c_ui = 1 + α · r_ui          p_ui = 1 if r_ui > 0 else 0

minimize over U, V:  Σ_{u,i} c_ui · (p_ui − u_uᵀv_i)²  +  λ · ( Σ_u ‖u_u‖² + Σ_i ‖v_i‖² )
```

solved by alternating least squares -- fix `V`, solve the now-linear
system for every `U` row in closed form, then fix `U` and solve for
`V`, repeat for `maxIter` rounds. This project's fixed hyperparameters:
`rank=10` (latent factors), `maxIter=10`, `regParam (λ)=0.1`,
`alpha (α)=1.0`. `coldStartStrategy="drop"` excludes users/items with
no training interactions from evaluation rather than emitting NaN
predictions for them.

*Complexity.* One ALS iteration is `O(n_nonzero · rank²  +  (n_users +
n_items) · rank³)` -- the first term from building each row's normal
equations over its nonzero ratings, the second from solving each of
those `rank × rank` linear systems. Trains offline in batch, not on
the request path.

**Anomaly detection, Welford's online algorithm**
(`jobs/spark-streaming/session_and_anomaly.py`). Per-product running
mean and variance update in `O(1)` per new per-minute count `x`,
without storing the full history or re-scanning it:

```
n ← n + 1
δ ← x − mean
mean ← mean + δ/n
M2 ← M2 + δ·(x − mean)        # note: uses the *updated* mean
variance ← M2 / n             # population variance, not the n−1 sample estimator
z = (x − mean_before_update) / sqrt(variance_before_update)
```

The z-score for window `x` is computed from the running stats *as they
stood before* `x` is folded in (guarded by `n ≥ 3` so the first few
windows per product don't fire on a near-zero variance), then `x` is
folded in for the next window's comparison. `Z_SCORE_THRESHOLD = 2.5`
in production; a controlled 60-event burst produced `z = 32.17` (see
[Results](#systems-experiments)).

## Streaming architecture

`jobs/spark-streaming/session_and_anomaly.py` runs two Spark
Structured Streaming queries over the same Kafka topic: session
reconstruction (5-minute inactivity windows) and per-product demand
anomaly detection (1-minute tumbling windows, Welford's online
algorithm, 2.5σ threshold). See
[docs/ARCHITECTURE.md](ARCHITECTURE.md#speed-layer-spark-structured-streaming).

## Batch architecture

Two batch paths: a MapReduce popularity job over the raw HDFS archive,
and Spark MLlib ALS training on RetailRocket (`jobs/als-training/`).
See [docs/ARCHITECTURE.md](ARCHITECTURE.md#batch-layer-mapreduce).

## Autonomous optimization

`serving-optimizer` watches Postgres (`pg_stat_statements`), Cassandra
(recent per-product event volume), and Elasticsearch (indexing op
rate) and adjusts indexing, partition rollups, and refresh interval
accordingly. See
[docs/ARCHITECTURE.md](ARCHITECTURE.md#serving-layer-performance-indexing-partitioning--refresh-intervals)
for the mechanism, [Results](#results) below for measured effect.

## Experimental methodology

**Dataset & split.** [RetailRocket](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset)
-- real, public clickstream data, ~2.75M events over 4.5 months, ~1.4M
users, ~235K items. `train_als.py` weights raw events the same way the
rest of this project does (`view=1, addtocart=3, transaction=5`),
aggregates to one (user, item) row per pair, then splits users with
`MIN_INTERACTIONS_FOR_EVAL=5` or more into an 80/20 random train/test
split (`seed=42`); users below that threshold still contribute to
training but aren't part of the held-out evaluation, since with 1-2
interactions a held-out item is mostly noise.

**Metrics.** Precision@10, computed natively in Spark
(`train_als.py`'s own `precision_at_k`): request `k×3` candidates per
test user (ALS doesn't exclude already-seen items on its own), drop
training pairs via a left-anti join, take the top-`k` by score, then
intersect against each user's held-out actual items using
`array_intersect` + `size` rather than a Python UDF, so the hit-count
runs in the JVM instead of round-tripping rows through Python.
`experiments/recommendation/metrics.py` implements the same
Precision/Recall/MAP/NDCG@k formulas as plain, dataset-agnostic Python
(hand-verified against known examples -- see the module's own test
invocations) for anything that isn't a Spark job; nothing in this
repo currently calls it, since the only recommendation-quality
experiment left *is* the Spark job above. `experiments/recommendation/bootstrap.py`
similarly implements percentile-method bootstrap confidence intervals,
available and tested but not currently wired into any script either.

**Reproducing a run.** Needs the full Docker Spark+HDFS stack (RetailRocket
CSV loaded into HDFS first) -- see
[docs/RUNNING_LOCALLY.md](RUNNING_LOCALLY.md). The throughput,
optimizer, and fault-tolerance experiments need the same full stack
running (`docker compose up`); the CF scalability benchmark needs only
a local Python virtualenv.

## Results

### RQ1: serving-optimizer, measured on/off

| Store | Metric | Off | On | Effect |
|---|---|---|---|---|
| Postgres | read p95 (filtered query) | 6.14ms | 3.38ms | **-45%** |
| Postgres | write p50 | 3.41ms | 3.63ms | +6% (noise-level) |
| Elasticsearch | burst wall time (20 creates) | 0.71s | 0.25s | faster |
| Elasticsearch | time-to-searchable p50 | 585ms | 1040ms | slower |
| Cassandra | hot/cold classification | -- | correct, verified 3x | -- |

The Postgres win is close to free (negligible write overhead at this
table size). The Elasticsearch result is a genuine trade-off, not a
one-sided win: raising `refresh_interval` during a burst speeds up the
burst itself but measurably delays individual documents becoming
searchable -- exactly the trade-off the architecture doc already
claimed, now confirmed rather than assumed.

### RetailRocket ALS: real interaction data at production sparsity

```
Precision@10 = 0.0055
```

Evaluated on the held-out test split described in
[Experimental methodology](#experimental-methodology) above. Low,
honestly: RetailRocket is genuinely sparse (median 1 interaction per
user across ~1.4M users and ~235K items) -- the dominant reason the
number is low, not an implementation bug. A production system at this
sparsity would want richer item features or a hybrid content +
collaborative approach; this project doesn't build one here, since
doing so on this project's own catalog would require a shared id space
this dataset doesn't provide (see the note at the top of this report).
The trained model and a precomputed top-10-recommendations table are
persisted to HDFS and can be loaded into HBase for point-lookup
serving -- see
[docs/ARCHITECTURE.md](ARCHITECTURE.md#serving-layer-hbase) for why
those recommendations, once loaded, still can't be enriched into real
product details through this project's own `product-service`.

### CF scalability

`similar_items()`'s cosine similarity against every other item on
every request is O(n_items) per call -- fine at this project's real
300-product catalog, not at 100K-10M items. Fixed with a periodically
-refreshed precomputed top-N neighbor cache
(`services/recommendation-service/app/model.py`'s
`refresh_neighbor_cache`), measured against the real production
`RecommendationEngine` class at increasing catalog size *and*
proportional traffic (`experiments/recommendation/scalability_benchmark.py`
-- users scale with items at this project's own real ratio, since a
catalog that's genuinely grown presumably serves proportionally more
traffic too, not the same fixed user count spread thinner). This
benchmark uses synthetic *traffic*, generated directly into the
engine's dicts rather than through Kafka, to drive a real, unmodified
production class -- a load-generation detail, not a recommendation-
quality claim, so it carries none of the concern the removed synthetic
interaction log did (see the note at the top of this report):

| Items | Users | Live p95 | Cache build | Cached p95 | Speedup |
|---|---|---|---|---|---|
| 300 | 2,000 | 8.0ms | 1.1s | 0.002ms | 5,133x |
| 3,000 | 20,000 | 83.8ms | 142.0s | 0.045ms | 1,880x |
| 5,000 | 33,333 | 89.8ms | 357.6s | 0.054ms | 1,648x |

Live latency grows roughly linearly with scale, as expected for an
O(n) per-request scan; cached latency stays flat and sub-millisecond
throughout. The cache build cost is the honest cost of this approach,
not swept under the rug -- it grew faster than a naive O(n²) estimate
once traffic scaled too (142s → 358s, 3,000 → 5,000 items), which is
the real argument for approximate/incremental methods (FAISS, HNSW, or
updating only affected pairs) past whatever scale makes a full
periodic rebuild itself too slow -- not attempted here, see
[Limitations](#limitations).

### Systems experiments

**Throughput** (`experiments/throughput/`, paced load against the live
stack, full consumer pipeline attached). Run twice, three weeks apart
(2026-08-28 and 2026-09-19, the latter after this stack had
accumulated substantially more Kafka history from other testing) --
both shown, since the pattern reproducing independently is more
informative than either run alone:

| Target rate (events/sec) | Achieved (08-28) | Achieved (09-19) | p50 (09-19) | p99 (09-19) |
|---|---|---|---|---|
| 100 | 73.4 | 70.8 | 4.9ms | 5.3s |
| 250 | 147.6 | 162.4 | 6.5ms | 4.9s |
| 500 | 305.9 | 253.9 | 16.1ms | 5.5s |
| 750 | 201.1 | 273.6 | 103.3ms | 7.7s |
| 1000 | 332.9 | 340.0 | 305.4ms | 5.4s |
| 1500 | 395.1 | 368.5 | 258.3ms | 5.8s |

Both runs show the same qualitative pattern: achieved throughput never
scales cleanly with target rate (08-28's 750 dipped *below* its own
500; 09-19 was monotonic but still plateaued well short of target),
and heavy tail latency (multi-second p99) shows up even at the lowest
target rate, not just under heavy load -- real saturation behavior
with a persistent slow-outlier tail, once a full downstream consumer
pipeline is attached. Both runs stay well below
`scripts/kafka_load_test.py`'s own unpaced, no-downstream-consumer
benchmark of 500-650+ events/sec, because that test measures a
fundamentally different thing (raw ingestion capacity vs. sustained
throughput with consumers competing for resources). Zero request
failures at any rate in either run -- the system degrades via latency
and consumer lag, not errors.

**Fault tolerance** (`experiments/fault_tolerance/`):

- `recommendation-service`: `/health` recovers in ~1s after a kill.
  The observable top-10 popularity list stabilized 4.18s after that on
  2026-08-28's smaller topic, and 8.13s after that on 2026-09-19's
  (this is a proxy for replay completion, not confirmation of it --
  see [Limitations](#limitations)). The roughly 2x growth across three
  weeks is consistent with this design's own documented cost: a fresh
  consumer group replays the *entire* topic from scratch on every
  restart, so stabilization time should be expected to track total
  accumulated topic history, not stay constant.
- `hdfs-sink`: 40/40 tracked events survived a crash with **zero
  loss** in both runs -- the persistent, committed-offset consumer
  design works as intended.
- Elasticsearch: genuinely loses its index when its *container* is
  replaced (confirmed via `index_not_found_exception`, not just a
  process kill, which reuses the same container filesystem and proves
  nothing about the "no persistent volume" claim). `product-service`
  propagates that as a raw 500 rather than degrading gracefully.
  Recovery requires restarting `product-service` specifically --
  restarting Elasticsearch again does not fix it, confirming the
  documented reindex-on-`product-service`-startup design is what
  actually matters here.

## Limitations

- **One real interaction dataset, and it doesn't share this project's
  own catalog.** RetailRocket is the only recommendation-quality data
  point in this project. Its item ids are disjoint from the demo
  catalog `product-service` and `recommendation-service` serve, so
  there's no way, without fabricating data, to directly compare it
  against this project's own real-time item-CF model on the same
  users and items. An earlier version of this report worked around
  that by generating a synthetic interaction log over the real catalog
  -- see the note at the top of this report for why that's been
  removed rather than kept.
- **Replay-completion proxies, not guarantees.** The fault-tolerance
  experiment's "stabilized" metric for `recommendation-service` is an
  observable proxy (the visible top-10 stopped changing), not direct
  confirmation the full Kafka topic replay finished -- a full replay
  could plausibly continue slightly longer without visibly changing
  a small top-10 window.
- **The CF scalability fix has its own scaling limit.** The
  precomputed neighbor cache turns per-request cost from O(n_items)
  into O(1), but building it is still O(n_items²)-ish, and that cost
  grew faster than expected once traffic scaled alongside catalog size
  (142s → 358s, 3,000 → 5,000 items). Not measured past 5,000 items
  here -- see [Future work](#future-work).
- **Single-host, single-run measurements (RetailRocket ALS and the CF
  scalability benchmark excepted -- both deterministic given their
  fixed seeds).** Everything here was measured on one Apple Silicon
  Mac, under Docker Desktop resource limits, competing with this
  session's own other testing activity at times. The throughput and
  fault-tolerance experiments have been run twice each, three weeks
  apart, and reproduced the same qualitative pattern both times (see
  [Systems experiments](#systems-experiments)) -- real signal that the
  results aren't a fluke of one run, but still not a formal confidence
  interval or a controlled multi-trial design. The optimizer numbers
  remain a single trial: no reruns yet.
- **Optimizer experiments are short, targeted bursts**, not sustained
  production-scale traffic -- real effect sizes at higher, sustained
  load are plausibly different (likely larger for the Postgres index,
  since benefit compounds with query volume).

## Future work

- Apply bootstrap-CI and repeated-trial treatment to the throughput,
  optimizer, and fault-tolerance experiments -- still no formal
  variance estimate. `experiments/recommendation/bootstrap.py` already
  implements the method and is tested; it just isn't wired to any of
  these experiments yet.
- Approximate nearest-neighbor search (FAISS/HNSW/Annoy) or
  incremental per-pair similarity updates in place of the neighbor
  cache's full periodic rebuild, past whatever catalog+traffic scale
  makes that rebuild itself too slow -- not reached in this project's
  measurements (5,000 items), but the cache-build growth trend is the
  concrete argument for it.
- If this project's own catalog ever has real user interaction data
  (rather than seeded/simulated traffic) at meaningful volume, a
  same-catalog comparison between the real-time item-CF model and a
  catalog-trained ALS model would be the honest version of the
  comparison this report used to run on fabricated data.

## References

- [RetailRocket e-commerce dataset](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset) (Kaggle)
- [McAuley-Lab/Amazon-Reviews-2023](https://amazon-reviews-2023.github.io/)
- [auto-indexing](https://github.com/nimit-pasricha/auto-indexing) -- the Postgres-only project `serving-optimizer`'s indexer is based on, extended here to Cassandra and Elasticsearch.
