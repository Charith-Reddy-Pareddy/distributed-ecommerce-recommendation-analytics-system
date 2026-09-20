# Distributed E-Commerce Recommendation Analytics System

**[Live project site](https://charith-reddy-pareddy.github.io/distributed-ecommerce-recommendation-analytics-system/)** -- research questions, architecture, and results charted from this repo's own `experiments/*/results/*.jsonl` data. Also includes a **[dashboard demo](https://charith-reddy-pareddy.github.io/distributed-ecommerce-recommendation-analytics-system/demo.html)** -- a frozen snapshot of the real operational dashboard's search, analytics, and recommendation views, since GitHub Pages can't run the live Kafka/Spark/HBase backend behind it.

A demo e-commerce backend that turns ordinary shopping activity --
browsing, adding to cart, buying -- into real-time product
recommendations, live analytics, and a backend that tunes its own
databases as traffic changes.

It's built as a Lambda-style architecture: every event flows through
one Kafka stream, and independent services -- a real-time recommender,
an analytics aggregator, a batch ML pipeline, and a stream processor --
each read that same stream and build their own view of the data, with
no shared database between them.

## Research question

Underneath the systems work, this project investigates one question:

- **RQ1** — Can workload-aware database optimization
  (`serving-optimizer`) reduce serving latency without excessive
  write/indexing overhead?

Two earlier research questions asked whether a real-time item-item
recommender compares favorably to batch ALS, and whether a CF+ALS
hybrid blend beats either alone. Answering those required training
and serving CF, ALS, and a catalog of products against a *shared* item
id space -- and the only way this project had one was a synthetic
interaction log fabricated over the real product catalog, since the
one real interaction dataset here (RetailRocket) has its own disjoint
item ids and no product text. Recommendation-quality numbers computed
on fabricated interactions aren't a finding, however clearly the log
was labeled synthetic, so that comparison (and the hybrid blend, and a
cross-category generalization check built on the same log) has been
removed rather than kept around for a number to report. What's left as
a recommendation-quality result is the real one: **RetailRocket ALS**
(`jobs/als-training/`), trained and evaluated entirely on real
clickstream data, Precision@10 = 0.0055 -- low, honestly, because
RetailRocket is genuinely sparse (median 1 interaction per user across
~1.4M users), not because of a bug. See
[Data sources](#data-sources) for why its ids can't be served through
this project's own catalog.

**Main contributions**: an event-driven distributed recommendation
pipeline; workload-aware database optimization as a measured
experiment rather than a fixed config; a real-data ALS study at
production-realistic sparsity; and a measured scaling limit (and fix)
for the real-time collaborative-filtering engine as catalog size
grows.

Full methodology and results are in
[docs/RESEARCH_REPORT.md](docs/RESEARCH_REPORT.md); raw data is in
`experiments/*/results/*.jsonl`.

## Key findings

All numbers below are measured, not estimated -- see
[docs/RESEARCH_REPORT.md](docs/RESEARCH_REPORT.md) for full tables and
methodology.

- **RQ1 — the optimizer earns its keep, at a real cost.**
  `serving-optimizer`'s Postgres indexer cut p95 read latency
  **45%** (6.1ms → 3.4ms) for negligible write overhead. Its
  Elasticsearch tuner sped up a write burst (0.71s → 0.25s) but made
  individual documents slower to become searchable (585ms → 1040ms)
  -- confirmed, not just documented. Cassandra hot/cold classification
  was verified correct against real, controlled traffic.
- **RetailRocket ALS: real interaction data, honestly sparse.**
  Precision@10 = **0.0055** on ~2.75M real clickstream events across
  ~1.4M users -- low mainly because the dataset itself is sparse
  (median 1 interaction per user), not because of a bug. A production
  system at this sparsity would want richer item features or a
  hybrid content + collaborative approach. See
  [jobs/als-training/](jobs/als-training/) and
  [docs/RESEARCH_REPORT.md](docs/RESEARCH_REPORT.md).
- **Two more real experiments, run against the live stack:**
  measured Kafka throughput saturates around 200-400 events/sec with
  the full downstream consumer pipeline attached (well under an
  earlier, consumer-free 500-650/sec benchmark); a killed `hdfs-sink`
  recovers with **zero event loss**, while Elasticsearch genuinely
  loses its index on container replacement and only recovers once
  `product-service` -- not ES itself -- restarts.
- **The naive CF lookup doesn't scale, and now it doesn't have to.**
  `similar_items()` used to compute cosine similarity against every
  other item on every request. Measured with catalog size *and*
  traffic scaling together (matching this project's own real
  users-per-product ratio, not an artificially sparse case): live p95
  latency grew **8ms → 84ms → 90ms** at 300 → 3,000 → 5,000 items,
  while a periodically-refreshed precomputed neighbor cache stayed at
  **0.002-0.054ms** throughout -- a 1,600-5,100x speedup. The
  precompute itself isn't free (1s → 142s → 358s at those same
  scales), an honest limit on this approach, not hidden -- see
  [Key challenges](#key-challenges).
- **114 unit tests + 13 integration tests**, all passing, and several
  real bugs found and fixed while building this: a corrupted
  multiprocessing state after repeated force-kills, a Kafka consumer
  that goes silently idle with nothing in the logs to say so, and
  `docker compose start` being a silent no-op on an already-running
  container. See [Key challenges](#key-challenges) below.

## Architecture

`event-service` has no database of its own — Kafka *is* its durable
log. Every event flows through the `events` topic, and each
downstream consumer builds its own independent read model from it.

**Services** (each its own FastAPI app or worker, own Docker image):

| Service | Port | Responsibility | Datastore |
|---|---|---|---|
| `product-service` | 8001 | Product catalog CRUD, enriched documents (tags, specs, images, geo location, rating) | MongoDB |
| `user-service` | 8002 | User CRUD | `user_db` (Postgres) |
| `event-service` | 8003 | Ingests view/cart/purchase events, publishes them to Kafka | Kafka only (no DB) |
| `recommendation-service` | 8004 | Consumes the `events` topic, builds an item-item similarity model in memory, serves recommendations | Kafka (event source only, stateless otherwise) |
| `analytics-service` | 8005 | Consumes the `events` topic independently, maintains rolling product/day counters | `analytics_db` (Postgres) |
| `hdfs-sink` | — | Consumes the `events` topic with a *persistent* consumer group, batches events into HDFS as the raw archive for the batch layer | HDFS (`/events/dt=YYYY-MM-DD/*.jsonl`) |

```mermaid
flowchart LR
    client([client]) -->|POST /events| eventsvc[event-service]
    eventsvc -->|produce| kafka[(Kafka<br/>events, 3 partitions)]

    kafka -->|fresh group| recsvc[recommendation-service]
    kafka -->|fresh group| analyticssvc[analytics-service]
    kafka -->|persistent group| hdfssink[hdfs-sink]
    kafka -->|consume| sparkstream[Spark Structured Streaming]

    analyticssvc --> analyticsdb[(Postgres analytics_db)]
    sparkstream -->|per-minute demand| cassandra[(Cassandra)]
    sparkstream -->|finalized sessions| hdfs[(HDFS)]
    hdfssink -->|append| hdfs

    hdfs --> mapreduce[MapReduce popularity job]
    hdfs --> als[Spark MLlib ALS training]
    als -->|model + top-10 recs| hdfs
    hdfs --> hbaseloader[hbase-loader]
    hbaseloader --> hbase[(HBase)]

    recsvc -->|precomputed lookups| hbase
    recsvc -->|product details| productsvc[product-service]
    productsvc --> mongo[(MongoDB)]
    productsvc --> es[(Elasticsearch)]
    usersvc[user-service] --> userdb[(Postgres user_db)]

    servopt[serving-optimizer] -.polls.-> userdb
    servopt -.polls.-> analyticsdb
    servopt -.polls.-> cassandra
    servopt -.polls.-> es

    browser([browser]) --> dashboard[dashboard]
    dashboard --> productsvc
    dashboard --> recsvc
    dashboard --> analyticssvc
    dashboard --> servopt
```

Kafka runs in **KRaft mode** (no Zookeeper). HDFS, HBase, and Spark
each run as their own small clusters. Every remaining service owns
its own Postgres database (database-per-service, one shared container
for local dev). The `events` topic has 3 partitions and no producer
partition key, so Kafka round-robins events instead of routing by
`user_id` -- fine here, since every consumer aggregates with
commutative counters or event-time windowing, not arrival order.

The full write-up of every component -- the collaborative-filtering
algorithm, the MapReduce popularity job, Spark Structured Streaming,
ALS training in Spark MLlib, HBase, Cassandra, Elasticsearch, and the
autonomous `serving-optimizer` -- is in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Key challenges

- **Four consumers, one topic, no shared state.**
  `recommendation-service`, `analytics-service`, `hdfs-sink`, and
  Spark all read the same `events` topic independently, unkeyed and
  round-robined across partitions. Every consumer had to aggregate
  with commutative counters or event-time windowing instead of
  relying on arrival order.
- **Spark's `update` output mode double-counts if you're not
  careful.** The Cassandra writer for per-minute demand counts had to
  upsert each window's running total rather than increment it --
  Spark re-emits a window every time it updates, so a naive increment
  double-counts. This was the trickiest bug in the project.
- **An autonomous tuner needs a real problem to prove itself
  against.** `serving-optimizer`'s Postgres indexer had nothing to
  act on until `user-service` had a genuinely unindexed, filtered
  query in traffic -- the `country` filter exists specifically to
  give it one.
- **~20 containers on one machine is its own ops problem.**
  Elasticsearch, Cassandra, and Spark each needed individual
  memory-limit tuning to fit inside 12GB without one OOM-killing
  another -- separate from anything about the architecture itself.
- **A silently idle background thread is worse than a crash.**
  `recommendation-service`'s Kafka consumer runs in a daemon thread
  with zero logging -- when it stalled during testing, the HTTP server
  stayed "healthy" throughout, giving no signal anything was wrong.
  Fixed by logging thread startup, every message error, and periodic
  replay progress -- a crash you can see beats a hang you can't.
- **Experiments can lie to themselves in subtle ways.** Building the
  fault-tolerance suite surfaced real bugs in the experiments
  *measuring* the system, not just the system itself: a decision-log
  endpoint's page-size limit producing false negatives under heavy
  background traffic, and `docker compose start` silently no-op'ing on
  an already-running container. Each one looked like a system failure
  until traced back to the test.
- **The fix for one bottleneck can become the next one.** Precomputing
  item-item neighbors instead of computing them live fixed the O(n)
  per-request cost -- but the precompute itself is O(n²)-ish, and its
  own cost grew faster than expected once traffic scaled alongside
  catalog size (142s → 358s from 3,000 → 5,000 items, worse than
  linear in items). That's not a hidden flaw, it's the honest reason
  production systems move to approximate/incremental methods (FAISS,
  HNSW, or updating only affected pairs) past some scale rather than
  exact all-pairs precomputation -- a real *next* bottleneck, not a
  solved problem.

## Technical considerations & takeaways

- **Database-per-service, Kafka as the only shared log.** No service
  reads another's database directly; each downstream service builds
  its own read model from `events`. That's what let four very
  different consumers -- an in-memory model, SQL rollups, a raw
  archive, and a streaming job -- coexist without stepping on each
  other.
- **The id-mismatch problem is real and stays unfixed, on purpose.**
  RetailRocket's item ids are a different, disjoint space from this
  project's own demo catalog -- real clickstream events on someone
  else's real catalog, not this one. `hbase-loader` is source-agnostic
  (`RECS_PATH`/`USER_ID_FIELD`/`ITEM_ID_FIELD` env vars) and will
  faithfully load whatever ALS produces into HBase, but
  `/recommendations/precomputed/{visitor_id}` can never enrich those
  ids through `product-service` -- it always falls back to raw
  RetailRocket ids. A second ALS model used to exist, trained on a
  synthetic interaction log generated *over* the real catalog
  specifically to make the ids line up and enable a hybrid CF+ALS
  blend -- but simulating a model's inputs to get its outputs to
  resolve to real product names isn't a trade worth making just for
  that. See
  [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#serving-layer-hbase).

More trade-offs and lessons learned are in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#trade-offs-and-lessons-learned).

## Benchmarks

Measured locally on Docker Desktop, not a production cluster. The
first block is `scripts/kafka_load_test.py`'s unpaced, no-downstream-
consumer max-throughput test; `experiments/throughput/` measures the
same ingestion path paced at fixed target rates *with* the full
consumer pipeline attached, which is why the achieved numbers differ --
see [docs/RESEARCH_REPORT.md](docs/RESEARCH_REPORT.md) for why.

```
Kafka ingestion (no consumers):  500-650+ events/sec sustained, 8 cores,
                                  all requests returned 202
Kafka ingestion (full pipeline): 200-400 events/sec sustained before
                                  backpressure sets in (experiments/throughput/)
Kafka topic:                     3 partitions, 1 broker, unkeyed (round-robin)
Spark trigger interval:          30s for both streaming queries
Anomaly detection:                one controlled 60-event/10s burst, flagged at z_score=32.17
RetailRocket ALS Precision@10:   0.0055 (~2.75M events, ~1.4M users, sparse)
HBase point lookup:              ~10ms average over the REST layer
Product catalog:                 300 Amazon products
Optimizer Postgres p95:          6.14ms -> 3.38ms after auto-indexing (-45%)
hdfs-sink crash recovery:        40/40 tracked events recovered, zero loss
Test suite:                       114 unit tests + 13 integration tests, all passing
CF similar-items (300 items):     live p95 8ms -> cached p95 0.002ms (5,133x)
CF similar-items (5,000 items):   live p95 90ms -> cached p95 0.054ms (1,648x)
```

## Data sources

Two real, public datasets. No interaction data in this project is
synthetic.

- **[McAuley-Lab/Amazon-Reviews-2023](https://amazon-reviews-2023.github.io/)**
  — 300 real Amazon products (titles, brands, prices, images, ratings,
  ASINs) pulled via `scripts/fetch_amazon_products.py` and stored in
  MongoDB as the demo product catalog. All live traffic (the real-time
  item-item CF model, `recommendation-service`'s `/recommendations/*`
  endpoints, the dashboard demo) is driven by real interactions with
  this catalog, via `scripts/seed_data.py` or the API directly -- not
  fabricated.
- **[RetailRocket e-commerce dataset](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset)**
  (Kaggle) — ~2.75M real clickstream events over 4.5 months, ~1.4M
  users, ~235K items. Trains a separate, offline ALS model
  (`jobs/als-training/`) as this project's recommendation-quality
  study. Its item ids are their own space, disjoint from the demo
  catalog above -- a real dataset about someone else's real catalog,
  not this project's -- so its precomputed recommendations can be
  loaded into HBase and served, but never enriched into product
  details through `product-service`. See
  [Technical considerations](#technical-considerations--takeaways) for
  why that's left as-is rather than papered over.

## Running locally

Requires Docker and Docker Compose, with **at least 12GB** allocated
to Docker Desktop (Settings → Resources → Memory).

```bash
docker compose up --build
```

Starts every store and service, including the dashboard at
[http://localhost:8006](http://localhost:8006). Once the logs settle,
seed some sample data:

```bash
pip install requests
python scripts/seed_data.py
```

Full setup -- throughput testing, the MapReduce job, ALS training,
loading HBase, the test suite, and project layout -- is in
[docs/RUNNING_LOCALLY.md](docs/RUNNING_LOCALLY.md).

## Setup notes

- **Memory**: per-container `mem_limit`s in `docker-compose.yml` sum
  to ~17GB, but that's a ceiling, not concurrent usage -- 12GB works
  in practice. Below that, Elasticsearch, Cassandra, or a Spark
  container is usually first to get OOM-killed (`Exited (137)`); give
  Docker Desktop more RAM or lower that service's `mem_limit`.
- **Apple Silicon**: `hdfs-namenode`/`hdfs-datanode` and the HBase
  services build from source instead of pulling an image, since the
  official Hadoop/HBase images are amd64-only -- the first
  `docker compose up --build` takes longer as a result.
- **Restarting a single service can leave others down**:
  `docker compose up -d <service>` only starts its dependency
  subgraph. After any interruption, prefer a plain `docker compose
  up -d` with no service name.
- **Kafka's default 7-day retention** applies to the `events` topic.
  After a long idle stretch, `recommendation-service`/
  `analytics-service` may restart with less history than expected —
  re-run `scripts/seed_data.py` to refill it.

## Why this project

I wanted to see what actually happens when one event stream has to
feed real-time recommendations, rolling analytics, a batch pipeline,
and a serving layer at the same time, instead of reading about Lambda
Architecture in the abstract. Building all four consumers off the
same Kafka topic surfaced problems a single-service tutorial never
would -- keeping the Cassandra writer correct under Spark's `update`
mode, or noticing partway through that the ALS model and the
in-memory CF model had no real relationship to each other. That
noticing turned into the research question above: once the
architecture existed, the more interesting question became whether
any of it measurably helped -- and, later, into recognizing that a
model comparison propped up by fabricated interaction data wasn't
actually answering that question, however it was labeled. More in
[Key challenges](#key-challenges) and
[docs/RESEARCH_REPORT.md](docs/RESEARCH_REPORT.md).

## AI assistance

I used Claude selectively during development for a limited amount of
documentation, test scaffolding, repetitive boilerplate, and code-review
feedback. I reviewed and adapted those suggestions before incorporating
them into the project. I remain responsible for the final code, tests,
documentation, design decisions, and reported results in this repository.

See [docs/AI_ASSISTANCE.md](docs/AI_ASSISTANCE.md) for details.
