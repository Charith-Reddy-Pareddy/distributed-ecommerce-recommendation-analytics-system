# Distributed E-Commerce Recommendation Analytics System

A demo e-commerce backend that turns ordinary shopping activity --
browsing, adding to cart, buying -- into real-time product
recommendations, live analytics, and a backend that tunes its own
databases as traffic changes.

It's built as a Lambda-style architecture: every event flows through
one Kafka stream, and independent services -- a real-time recommender,
an analytics aggregator, a batch ML pipeline, and a stream processor --
each read that same stream and build their own view of the data, with
no shared database between them.

## Research questions

Underneath the systems work, this project is really investigating four
questions:

- **RQ1** — How does event-weighted implicit feedback (view/cart/
  purchase weights) affect recommendation quality, compared to
  alternative weighting schemes?
- **RQ2** — How does a real-time item-item recommender compare against
  batch ALS on Precision@K, Recall@K, MAP@K, NDCG@K, latency, and
  update freshness?
- **RQ3** — What's the trade-off between recommendation freshness and
  serving latency, given that this architecture supports both a
  streaming and a batch path to the same problem?
- **RQ4** — Can workload-aware database optimization
  (`serving-optimizer`) reduce serving latency without excessive
  write/indexing overhead?

**Main contributions**: an event-driven distributed recommendation
pipeline; a streaming + batch recommendation architecture serving the
same catalog; workload-aware database optimization as a measured
experiment rather than a fixed config; and an experimental evaluation
of recommendation quality and serving performance under this
architecture.

Answers, methodology, and results live in `experiments/` and
[docs/RESEARCH_REPORT.md](docs/RESEARCH_REPORT.md) as they're
completed -- this is ongoing work, not a finished study, and numbers
only get reported once they've actually been measured.

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
- **Two item-id spaces that don't line up.** The ALS model trains on
  the real RetailRocket dataset (its own item ids); the demo catalog
  has separate ids. Precomputed ALS recommendations can't be enriched
  through `product-service` as a result.
- **~20 containers on one machine is its own ops problem.**
  Elasticsearch, Cassandra, and Spark each needed individual
  memory-limit tuning to fit inside 12GB without one OOM-killing
  another -- separate from anything about the architecture itself.

## Technical considerations & takeaways

- **Database-per-service, Kafka as the only shared log.** No service
  reads another's database directly; each downstream service builds
  its own read model from `events`. That's what let four very
  different consumers -- an in-memory model, SQL rollups, a raw
  archive, and a streaming job -- coexist without stepping on each
  other.
- **A low benchmark number can be the dataset, not a bug.** ALS's
  Precision@10 (0.0055) is low mainly because RetailRocket is
  genuinely sparse -- median 1 interaction per user across ~1.4M
  users. A production system would want richer item features or a
  hybrid content + collaborative approach.
- **What I'd cut in a v2.** The in-memory collaborative-filtering
  model and the batch ALS model both rank items with no blending
  strategy between them. I'd pick one, or define how they combine,
  instead of exposing both as separate endpoints.

More trade-offs and lessons learned are in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#trade-offs-and-lessons-learned).

## Benchmarks

Measured locally on Docker Desktop, not a production cluster:

```
Kafka ingestion:         500-650+ events/sec sustained, 8 cores, all
                          requests returned 202 (scripts/kafka_load_test.py)
Kafka topic:              3 partitions, 1 broker, unkeyed (round-robin)
Spark trigger interval:   30s for both streaming queries
Anomaly detection:        one controlled 60-event/10s burst, flagged at z_score=32.17
ALS Precision@10:         0.0055 (RetailRocket, ~2.75M events, ~1.4M users)
HBase point lookup:       ~10ms average over the REST layer
Product catalog:          300 Amazon products
```

## Data sources

Two real, public datasets, used for different parts of the project:

- **[McAuley-Lab/Amazon-Reviews-2023](https://amazon-reviews-2023.github.io/)**
  — 300 real Amazon products (titles, brands, prices, images, ratings,
  ASINs) pulled via `scripts/fetch_amazon_products.py` and stored in
  MongoDB as the demo product catalog.
- **[RetailRocket e-commerce dataset](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset)**
  (Kaggle) — ~2.75M real clickstream events over 4.5 months, used to
  train the offline ALS recommendation model in `jobs/als-training/`.

These are two separate datasets with two separate item-id spaces --
see [Serving layer: HBase](docs/ARCHITECTURE.md#serving-layer-hbase)
for what that means in practice.

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
in-memory CF model had no real relationship to each other. More on
that above in [Key challenges](#key-challenges) and in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#trade-offs-and-lessons-learned).

## AI assistance

I used Claude selectively during development for a limited amount of
documentation, test scaffolding, repetitive boilerplate, and code-review
feedback. I reviewed and adapted those suggestions before incorporating
them into the project. I remain responsible for the final code, tests,
documentation, design decisions, and reported results in this repository.

See [docs/AI_ASSISTANCE.md](docs/AI_ASSISTANCE.md) for details.
