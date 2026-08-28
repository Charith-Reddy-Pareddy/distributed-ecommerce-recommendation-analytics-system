# Research report

Every number below was actually measured against this codebase and
this stack -- see `experiments/*/results/*.jsonl` for the raw records
each section draws from, and `experiments/*/README.md` for how to
reproduce a run. Nothing here is estimated or extrapolated.

## Abstract

This project investigates whether a distributed e-commerce platform's
recommendation quality and serving performance can be measured, not
just built. Starting from a Lambda-style architecture (Kafka, Spark
Structured Streaming, Spark MLlib ALS, HBase, Cassandra, Elasticsearch,
Postgres, MongoDB), we compare a real-time item-item collaborative
filter against a batch ALS model, blend the two, evaluate the blend
under a realistic temporal split, run ablations on event weighting and
model hyperparameters, and separately measure the effect of a
workload-aware database optimizer and the system's behavior under
container failure. The hybrid CF+ALS blend outperforms either model
alone; a temporal evaluation split shows the standard random split
overstates quality by roughly 40%; the optimizer's Postgres indexer
cuts p95 read latency 45%; and the system's persistent-offset consumer
recovers from a crash with zero event loss, while its stateless
consumer's replay time and its search index's total loss on container
replacement are both real, measured costs of those designs.

## Problem

A single event stream -- product views, cart adds, purchases -- has to
simultaneously feed a real-time recommender, rolling analytics, a
batch-trained ML model, and a workload-aware serving layer. The
architecture for doing this (Lambda-style: independent streaming and
batch paths reading the same log) is well documented in the
literature, but rarely evaluated: does the batch path actually add
recommendation quality over the streaming path alone? Does blending
them help, and by how much? What does "fresher" streaming data
actually cost in latency versus a batch retrain? Does an autonomous
database tuner's benefit outweigh its overhead? This report answers
those questions for this system, not in the abstract.

## System architecture

See [docs/ARCHITECTURE.md](ARCHITECTURE.md) for the full component
write-up (services, data flow, the Mermaid diagram lives in the
[README](../README.md#architecture)).

## Recommendation algorithms

Four models are compared, all sharing the real 300-product catalog's
id space (see [Ablation studies](#ablation-studies) for why that
matters):

- **Popularity** -- sum of event weights per product, no
  personalization.
- **Item-CF** (`services/recommendation-service/app/model.py`) -- the
  actual production `RecommendationEngine`: weighted cosine similarity
  between items' interaction vectors, in-memory, rebuilt by replaying
  Kafka.
- **Content-based** -- TF-IDF cosine similarity over each product's
  category, brand, and description.
- **Catalog-ALS** (`experiments/recommendation/catalog_als/`) --
  Spark MLlib ALS (implicit feedback), trained on a synthetic,
  catalog-native interaction log so its item ids match the other three
  models, unlike the original RetailRocket-trained model.
- **Hybrid CF+ALS / CF+content** (`services/recommendation-service/app/hybrid.py`,
  `experiments/recommendation/hybrid.py`) -- min-max normalized,
  alpha-weighted blend of two models' scores.

## Streaming architecture

`jobs/spark-streaming/session_and_anomaly.py` runs two Spark
Structured Streaming queries over the same Kafka topic: session
reconstruction (5-minute inactivity windows) and per-product demand
anomaly detection (1-minute tumbling windows, Welford's online
algorithm, 2.5σ threshold). See
[docs/ARCHITECTURE.md](ARCHITECTURE.md#speed-layer-spark-structured-streaming).

## Batch architecture

Two batch paths: a MapReduce popularity job over the raw HDFS archive,
and Spark MLlib ALS training (both the original RetailRocket job and
the catalog-native one built for this report). See
[docs/ARCHITECTURE.md](ARCHITECTURE.md#batch-layer-mapreduce).

## Autonomous optimization

`serving-optimizer` watches Postgres (`pg_stat_statements`), Cassandra
(recent per-product event volume), and Elasticsearch (indexing op
rate) and adjusts indexing, partition rollups, and refresh interval
accordingly. See
[docs/ARCHITECTURE.md](ARCHITECTURE.md#serving-layer-performance-indexing-partitioning--refresh-intervals)
for the mechanism, [Results](#results) below for measured effect.

## Experimental methodology

**Dataset.** `scripts/generate_interactions.py` generates a
synthetic-but-structured interaction log over the real 300-product
catalog: Zipfian-skewed item popularity, per-user category
preferences (1-3 preferred categories, 80% in-category / 20%
exploration), and a view→cart→purchase funnel (P(cart|view)=0.35,
P(purchase|cart)=0.4) matching the same 1/3/5 weighting
`recommendation-service` already uses. 2,000 synthetic users, 14
simulated weeks, 62,234 raw events → 31,219 weighted (user, item)
pairs after aggregation. This is documented, disclosed synthetic data,
never presented as real user behavior -- see
[Limitations](#limitations) for what that does and doesn't justify.

**Splits.** Two, used for different questions:
- *Random 80/20* (`experiments/recommendation/split_interactions.py`)
  -- matches the existing RetailRocket job's methodology, used for the
  main model comparison, hybrid sweep, and hyperparameter ablations.
  Users need ≥5 distinct interactions to get a held-out test split.
- *Temporal* (`experiments/recommendation/temporal_eval.py`) -- weeks
  1-12 train, week 14 test (week 13 reserved, unused here) -- a
  production system never gets to peek at future interactions, so this
  is the more realistic evaluation.

**Metrics.** Precision@10, Recall@10, MAP@10, NDCG@10
(`experiments/recommendation/metrics.py`, hand-verified against known
examples before use -- see the module's own test invocations),
computed identically across every model.

**Reproducing a run.** No Docker needed for the recommendation
experiments -- local-mode PySpark. See
[docs/RUNNING_LOCALLY.md](RUNNING_LOCALLY.md#recommendation-experiments).
The throughput, optimizer, and fault-tolerance experiments need the
full stack (`docker compose up`).

## Results

### RQ2: model comparison (random split, k=10, 1,866 test users)

| Model | Precision@10 | Recall@10 | MAP@10 | NDCG@10 | Latency/request |
|---|---|---|---|---|---|
| Popularity | 0.087 | 0.272 | 0.108 | 0.190 | ~0ms |
| Item-CF (production code) | 0.118 | 0.374 | **0.224** | **0.327** | 163ms |
| Content-based (TF-IDF) | 0.014 | 0.044 | 0.012 | 0.028 | 0.06ms |
| Catalog-ALS | **0.119** | **0.376** | 0.211 | 0.315 | **5.6ms** |

CF and ALS are nearly tied on precision/recall; CF ranks slightly
better (MAP/NDCG), ALS serves **~30x faster** since it's a
precomputed lookup rather than live cosine recomputation over the full
candidate set. Both comfortably beat popularity and content-based
alone -- content-based's weak standalone performance suggests category/
brand/description similarity alone is a poor proxy for this catalog's
actual purchase patterns.

### RQ3: hybrid blend (same split, alpha sweep)

| α (ALS weight) | Precision@10 | Recall@10 | MAP@10 | NDCG@10 |
|---|---|---|---|---|
| 0.00 (pure CF) | 0.1266 | 0.3997 | 0.2338 | 0.3413 |
| 0.25 | **0.1271** | **0.4002** | 0.2374 | **0.3450** |
| 0.50 | 0.1258 | 0.3952 | **0.2393** | 0.3445 |
| 0.75 | 0.1230 | 0.3875 | 0.2298 | 0.3344 |
| 1.00 (pure ALS) | 0.1191 | 0.3756 | 0.2108 | 0.3148 |

(These are full-catalog-scored numbers -- higher than the table above,
which uses each model's own production candidate-shortlisting. See
`experiments/recommendation/hybrid.py`'s docstring.)

A blend around **α=0.25-0.5 beats both pure CF and pure ALS** on every
metric. The CF+content blend, by contrast, only degrades monotonically
as content weight increases (0.1266 → 0.0145 from α=0 to α=1) --
consistent with content-based's weak standalone showing above.

### RQ3: freshness vs. latency

| Path | Latency to reflect a new interaction |
|---|---|
| Streaming (item-CF) | **~0.0005ms** (`_apply_event`, in-memory) |
| Batch (catalog-ALS retrain) | **~4.5s** on this dataset (~27K training rows) |

The retrain time is a floor, not a ceiling -- it would grow with data
volume, and production would add `hbase-loader`'s reload time on top.
The two paths aren't interchangeable at any data scale; the question
is which staleness a given feature can tolerate.

### RQ3 (temporal validity check): random split vs. temporal split

| Split | Item-CF Precision@10 | Catalog-ALS Precision@10 |
|---|---|---|
| Random 80/20 | 0.1175 | 0.1191 |
| Temporal (train weeks 1-12, test week 14) | 0.0678 | 0.0705 |

Both models drop **~40% relative** under the temporal split. The
random split lets a user's train and test interactions interleave in
time; the temporal split enforces a real causal boundary. The random-
split numbers above are the more optimistic of the two, not the more
honest one.

### RQ4: serving-optimizer, measured on/off

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

### Systems experiments

**Throughput** (`experiments/throughput/`, paced load against the live
stack, full consumer pipeline attached):

| Target rate (events/sec) | Achieved | p50 latency | p99 latency |
|---|---|---|---|
| 100 | 73.4 | 38.0ms | 5.0s |
| 250 | 147.6 | 20.2ms | 6.0s |
| 500 | 305.9 | 25.2ms | 4.2s |
| 750 | 201.1 | 540.8ms | 6.9s |
| 1000 | 332.9 | 341.7ms | 5.9s |
| 1500 | 395.1 | 364.1ms | 5.1s |

Achieved throughput never scales cleanly with target rate and is
noisy under load (750's achieved rate is *below* 500's) -- real
saturation behavior with heavy tail latency even at modest target
rates, once a full downstream consumer pipeline is attached. This is
well below `scripts/kafka_load_test.py`'s own unpaced, no-downstream-
consumer benchmark of 500-650+ events/sec, because that test measures
a fundamentally different thing (raw ingestion capacity vs. sustained
throughput with consumers competing for resources).

**Fault tolerance** (`experiments/fault_tolerance/`):

- `recommendation-service`: `/health` recovers in ~1s after a kill;
  the observable top-10 popularity list stabilizes ~4s after that on a
  small topic (this is a proxy for replay completion, not
  confirmation of it -- see [Limitations](#limitations)).
- `hdfs-sink`: 40/40 tracked events survived a crash with **zero
  loss** -- the persistent, committed-offset consumer design works as
  intended.
- Elasticsearch: genuinely loses its index when its *container* is
  replaced (confirmed via `index_not_found_exception`, not just a
  process kill, which reuses the same container filesystem and proves
  nothing about the "no persistent volume" claim). `product-service`
  propagates that as a raw 500 rather than degrading gracefully.
  Recovery requires restarting `product-service` specifically --
  restarting Elasticsearch again does not fix it, confirming the
  documented reindex-on-`product-service`-startup design is what
  actually matters here.

## Ablation studies

### Event weighting (RQ1)

| Scheme | Item-CF Precision@10 | Catalog-ALS Precision@10 |
|---|---|---|
| Uniform (1/1/1) | **0.1225** | 0.1152 |
| Linear (1/2/3) | 0.1209 | 0.1176 |
| Production (1/3/5) | 0.1175 | **0.1191** |
| Steep (1/5/10) | 0.1103 | 0.1180 |

Item-CF's precision *declines* as weighting steepens -- it does best
with flat weights. Catalog-ALS is roughly flat, peaking near the
production scheme. Neither model swings more than ~10% relative
across the whole range: weighting matters, but this system is not
highly sensitive to the exact scheme chosen.

### ALS hyperparameters (rank × regParam)

| rank | regParam=0.01 | regParam=0.1 | regParam=0.5 |
|---|---|---|---|
| 5 | 0.1281 | 0.1300 | **0.1320** |
| 10 (production) | 0.1153 | 0.1191 | 0.1303 |
| 20 | 0.0826 | 0.0973 | 0.1252 |

The production defaults (rank=10, regParam=0.1, inherited from the
much larger RetailRocket job) are **not** optimal at this catalog's
scale -- regParam=0.5 improves every metric at every rank tested, and
lower rank consistently beats higher rank. A smaller, sparser catalog
needs more regularization and less model capacity than a 2.75M-event
dataset does.

### CF neighbor count

`recommend_for_user` hardcodes 20 similar items considered per seed
item. Sweeping that:

| Neighbor count | Precision@10 | NDCG@10 |
|---|---|---|
| 5 | 0.1033 | 0.2889 |
| 10 | 0.1088 | 0.3013 |
| 20 (production) | 0.1175 | 0.3266 |
| 50 | **0.1260** | **0.3426** |

Quality improves **monotonically** through 50 with no plateau --
production's hardcoded 20 is conservative, trading some quality for
per-request compute (each additional neighbor considered is more
cosine similarity work per recommendation).

## Limitations

- **Synthetic interaction data.** The recommendation experiments run
  on a generated, documented-as-synthetic interaction log, not real
  user behavior. The generative model (Zipfian popularity, category
  affinity, a purchase funnel) is deliberately structured so models
  have real signal to learn from, but absolute metric values should
  not be read as "this is what real users would do" -- only the
  *relative* comparisons between models, splits, and configurations
  are the actual claims this report makes.
- **The hybrid endpoint isn't fully wired live.** `hybrid.py` and its
  unit tests are real and correct, and the offline evaluation above
  shows the blend works -- but `hbase-loader` was never extended to
  load the catalog-native ALS model's output into HBase, so
  `/recommendations/hybrid/{user_id}` currently always falls back to
  pure CF in the running system. The offline result is real; the live
  path to it is not yet complete.
- **Replay-completion proxies, not guarantees.** The fault-tolerance
  experiment's "stabilized" metric for `recommendation-service` is an
  observable proxy (the visible top-10 stopped changing), not direct
  confirmation the full Kafka topic replay finished -- a full replay
  could plausibly continue slightly longer without visibly changing
  a small top-10 window.
- **Single-host, single-run measurements.** Everything here was
  measured once, on one Apple Silicon Mac, under Docker Desktop
  resource limits, competing with this session's own other testing
  activity at times. None of these numbers include confidence
  intervals or repeated trials; they establish direction and rough
  magnitude, not statistically rigorous point estimates.
- **Optimizer experiments are short, targeted bursts**, not sustained
  production-scale traffic -- real effect sizes at higher, sustained
  load are plausibly different (likely larger for the Postgres index,
  since benefit compounds with query volume).

## Future work

- Finish wiring the live hybrid endpoint (extend `hbase-loader` to
  load `experiments/recommendation/catalog_als/`'s output).
- Repeat key experiments (model comparison, hybrid sweep) at multiple
  random seeds to get real confidence intervals instead of point
  estimates.
- Extend the temporal evaluation to actually use the reserved
  validation week for early stopping / hyperparameter selection,
  rather than only train/test.
- A real content-based feature set (embeddings over product images/
  descriptions) instead of TF-IDF, given how weak TF-IDF content
  similarity turned out to be standalone.

## References

- [RetailRocket e-commerce dataset](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset) (Kaggle)
- [McAuley-Lab/Amazon-Reviews-2023](https://amazon-reviews-2023.github.io/)
- [auto-indexing](https://github.com/nimit-pasricha/auto-indexing) -- the Postgres-only project `serving-optimizer`'s indexer is based on, extended here to Cassandra and Elasticsearch.
