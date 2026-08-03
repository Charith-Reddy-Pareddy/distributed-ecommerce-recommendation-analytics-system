# Distributed E-Commerce Recommendation Analytics System

An event-driven microservices system that simulates an e-commerce
platform, being built out toward a full **Lambda Architecture**: Kafka
ingestion feeding both a real-time (speed) layer and a batch layer,
with independent consumers turning the same event stream into
personalized recommendations and business analytics. See
[Roadmap](#roadmap) below for what's built vs. planned.

## Architecture (current)

```
                 ┌────────────────┐        ┌─────────────────┐
   client ─────► │  event-service  │───────►│  Kafka topic     │
                 │  (POST /events) │        │  "events"        │
                 └─────────────────┘        └──┬────┬─────┬────┘
                                                │    │     │
                                         consumes│  consumes│  consumes
                                                │    │     │
                                                ▼    ▼     ▼
                       ┌───────────────┐ ┌──────────────────┐ ┌───────────┐
                       │ recommendation-│ │ analytics-service │ │ hdfs-sink │
                       │ service        │ │ (rolling product/ │ │ (batches  │
                       │ (item-based CF)│ │  daily stats)     │ │  events   │
                       └───────┬────────┘ └─────────┬─────────┘ │  to HDFS) │
                               │                     │           └─────┬─────┘
                         reads product            writes to            │
                         details from             analytics_db         ▼
                         product-service                        ┌─────────────┐
                                                                  │ HDFS         │
                                                                  │ /events/     │
                                                                  │  dt=YYYY-MM- │
                                                                  │  DD/*.jsonl  │
                                                                  └─────────────┘
                 ┌─────────────────┐        ┌─────────────────┐
                 │ product-service │◄───────│  user-service    │
                 │ (catalog CRUD)  │        │  (user CRUD)     │
                 └─────────────────┘        └─────────────────┘
```

`event-service` has no database of its own — Kafka *is* its durable
log. Every event flows through the `events` topic; downstream
consumers each build their own independent read model from it.

**Services** (each is its own FastAPI app or worker, own Docker image):

| Service | Port | Responsibility | Datastore |
|---|---|---|---|
| `product-service` | 8001 | Product catalog CRUD, enriched documents (tags, specs, images, geo location, rating) | MongoDB |
| `user-service` | 8002 | User CRUD | `user_db` (Postgres) |
| `event-service` | 8003 | Ingests view/cart/purchase events, publishes them to Kafka | Kafka only (no DB) |
| `recommendation-service` | 8004 | Consumes the `events` topic, builds an item-item similarity model in memory, serves recommendations | Kafka (event source only, stateless otherwise) |
| `analytics-service` | 8005 | Consumes the `events` topic independently, maintains rolling product/day counters | `analytics_db` (Postgres) |
| `hdfs-sink` | — | Consumes the `events` topic with a *persistent* consumer group, batches events into HDFS as the raw archive for the batch layer | HDFS (`/events/dt=YYYY-MM-DD/*.jsonl`) |

`hdfs-sink` differs from the other two consumers on purpose: it uses
one committed-offset consumer group instead of a fresh one per
restart, because HDFS is an append-only archive — replaying from the
beginning on every restart would duplicate every batch already
written. It commits Kafka offsets only after a batch is durably
written to HDFS (at-least-once delivery), and flushes on a 500-event
or 30-second window, whichever comes first.

**Why this shape:** `event-service` doesn't know or care who reads
its events — `recommendation-service` and `analytics-service` are
independent consumers of the same Kafka topic, each building its own
read model and its own consumer group (a fresh, unique group ID on
every process start, so each service replays the full topic from the
beginning on boot and reconstructs its state — restart-safe by
construction). This is the standard event-driven / CQRS pattern behind
Lambda Architecture: one durable log, multiple independent readers.

Kafka runs in modern **KRaft mode** (no separate Zookeeper container —
Zookeeper has been phased out of Kafka itself since the 3.3 release)
using the official `apache/kafka` image.

HDFS runs as a single namenode + single datanode, built from a custom
image (`infra/hadoop`) rather than a stock Hadoop image: both the
official `apache/hadoop` image and the popular `bde2020/hadoop-*`
images are amd64-only. Hadoop is plain Java, so its JARs are
architecture-neutral — building on `eclipse-temurin` (which does
publish a native arm64 JRE) and dropping in the stock Hadoop binary
tarball gives native performance on Apple Silicon instead of running
under Rosetta emulation.

Each remaining service also owns its own Postgres database
(`product_db`, `user_db`, `analytics_db` — all in one Postgres
container for local dev, but logically separate schemas —
database-per-service).

## Recommendation algorithm

`recommendation-service` implements **item-based collaborative
filtering**:

1. Every event (`view`, `add_to_cart`, `purchase`) is weighted (1, 3,
   5 respectively) and folded into an in-memory user→item and
   item→user interaction matrix.
2. To recommend for a user, it finds items similar to what the user
   already interacted with, using **cosine similarity** between each
   item's interaction vectors.
3. Candidate items are scored by `similarity × the user's weight on
   the seed item`, summed across all the user's interactions, and
   ranked.
4. New users with no history fall back to a popularity ranking.

State is rebuilt on startup by replaying the entire Kafka topic, so
the service is stateless from a deployment standpoint — kill it,
restart it, and it reconstructs itself from the event log.

## Batch layer: MapReduce

`jobs/product-popularity/` is a real Hadoop **MapReduce** job (via
Hadoop Streaming, so the mapper/reducer are plain Python read from
stdin/write to stdout rather than Java classes) that computes the same
weighted popularity ranking as `recommendation-service`'s fallback,
but as a batch computation over the full HDFS event archive instead of
an in-memory streaming aggregate:

1. **Mapper** (`mapper.py`) reads each archived event line and emits
   `product_id -> weight` (1/3/5 for view/add_to_cart/purchase).
2. Hadoop's shuffle phase groups and sorts these by key.
3. **Reducer** (`reducer.py`) sums the weights per product, relying on
   the standard streaming guarantee that all records for a key arrive
   contiguously — the classic MapReduce accumulator pattern, not an
   in-memory dict keyed by every product.
4. A driver script (`run_job.sh`) submits the job, then sorts the
   (typically small) aggregated result set and writes a final ranked
   file back to HDFS at `/output/product-popularity-ranked.tsv`.

The job runs via Hadoop's `LocalJobRunner` (`mapreduce.framework.name`
defaults to `local`) rather than YARN — with a single datanode in this
local cluster, a full YARN ResourceManager/NodeManager would add
another two JVM services for no real distribution benefit, so this
keeps the resource budget for the rest of the stack (Spark, HBase,
Cassandra, MongoDB, Elasticsearch) intact while still running genuine
Hadoop MapReduce semantics and APIs.

**Verified, not just run**: the job's ranked output was checked against
`recommendation-service`'s popularity fallback and `analytics-service`'s
SQL-based aggregation — three independently implemented computations
over the same event data, and all three produced identical rankings
and identical scores for every product.

It's a one-shot batch job, not a long-running service, so it's kept
out of `docker compose up` behind a Compose profile — see
[Running locally](#running-locally) for the invocation.

## Speed layer: Spark Structured Streaming

`jobs/spark-streaming/session_and_anomaly.py` runs on a Spark standalone
cluster (`spark-master` + `spark-worker`) and reads the Kafka `events`
topic live, running two independent streaming queries:

1. **Session reconstruction** groups each user's events using Spark's
   `session_window` (a gap-based window — by default a new session
   starts after 5 minutes of inactivity), and writes finalized sessions
   to HDFS at `/spark-output/sessions/` once the watermark confirms a
   session won't receive any more events.
2. **Per-product demand anomaly detection** counts events per product
   in 1-minute tumbling windows, maintains a running mean/variance per
   product using **Welford's online algorithm** (so it doesn't need to
   hold full history in memory), and flags a window as anomalous when
   it's more than 2.5 standard deviations from that product's running
   mean — Z-score outlier detection computed incrementally over an
   unbounded stream, via `foreachBatch`.

**Verified with a real spike, not just run**: sent steady baseline
traffic (~8 events/min) for one product for 3 minutes, then a 60-event
burst in 10 seconds, then more baseline. The job correctly built a
baseline (mean≈5.3, stddev≈1.9) from the quiet windows and flagged the
spike window (count=66) with `z_score=32.17` — far past the threshold.
Session reconstruction was verified the same way: a distinct burst of
8 events for one test user produced an HDFS session record with the
exact right `event_count` and `products` list, correctly bounded to
one session window.

Session gap and watermark are configurable via `SESSION_GAP` /
`SESSION_WATERMARK` env vars (defaults: 5 minutes / 10 minutes,
realistic session semantics) — useful for shortening them during local
testing so you don't have to wait ~15 minutes to see output.

Uses the official `apache/spark` image, which — unlike the Hadoop
images — does publish native arm64 builds, so no custom image was
needed here.

## ALS recommendation model (Spark MLlib)

`jobs/als-training/train_als.py` trains a real collaborative-filtering
model — **ALS (Alternating Least Squares)** in Spark MLlib, with
`implicitPrefs=True` since clickstream events (view/add-to-cart/
purchase) are implicit feedback, not explicit ratings — on the
[RetailRocket e-commerce dataset](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset)
(real, public, ~2.75M events collected over 4.5 months; not a synthetic
or self-generated dataset). Event weights match the scheme used
throughout this project (view=1, add_to_cart=3, transaction=5).

**The honest result, not a curated one:** on a held-out 20% test split,
evaluated against 31,285 users with enough interaction history for a
meaningful train/test signal (≥5 interactions — RetailRocket is
extremely sparse, median interactions-per-user is 1 across ~1.4M users
and ~235K items, verified directly from the raw data), the model
achieves:

```
Precision@10 = 0.0055
```

That is a real, reproducible number (same result across two full
training runs with `seed=42`), and it is **far below the >0.15 often
cited in portfolio write-ups of similar stacks** — including an
earlier draft of this project's own resume bullet, before it was
corrected to match what the model actually does. For context: random
recommendation against a 235K-item catalog would score roughly 0.0000425,
so the model is meaningfully better than chance (~68x), but "meaningfully
better than random" and "good" are different claims, and only the
former is true here. Extreme data sparsity is the dominant cause, not
an implementation bug — verified by directly computing the dataset's
interaction distribution rather than assuming it. A production system
would likely need richer features (item metadata, session context),
more training data per user, or a hybrid approach (e.g. blending in the
content-based signals available in `item_properties.csv`, not currently
used) to do meaningfully better on a catalog this large and this sparse.

The trained model and a precomputed top-10-recommendations table for
the ~39,600 users with enough history to recommend for (not the full
~1.4M-user base — see the note in the script) are both persisted to
HDFS, at `/models/als-recommender` and `/output/als-recommendations`
respectively.

## Serving layer: HBase

HDFS is fine for batch reads but isn't a point-lookup store — turning
"the ALS output sits in HDFS" into "a live service can fetch a user's
precomputed recommendations in milliseconds" needs something else.
That's what HBase is for here: a real HBase cluster (master +
regionserver + ZooKeeper, all custom-built the same way as the Hadoop
image — there's no official Apache HBase Docker image at all, let
alone an arm64 one), backed by **real HDFS storage**
(`hbase.rootdir=hdfs://...`), not HBase's local-disk standalone mode.

`services/hbase-loader` (a one-shot job, like the MapReduce job) reads
`/output/als-recommendations` from HDFS and loads it into an
`als_recommendations` HBase table — one row per user, ranked
item/score pairs as columns. `recommendation-service` then queries
HBase's built-in REST server for a new endpoint:

```bash
curl http://localhost:8004/recommendations/precomputed/54
```

**Verified, not assumed:**
- Loaded all 39,632 rows, then queried a known user via HBase's REST
  API directly and confirmed the returned item ids and scores matched
  the HDFS source data exactly.
- Measured real point-lookup latency over 20 requests: **~10ms average
  (5ms min, 48ms max)** through the HTTP REST layer — a native HBase
  client would be faster still, since REST adds its own overhead.
- Killed the `hbase-master` container outright (by accident, while
  writing a cleanup command — caught immediately) and confirmed the
  data survived and the cluster reconnected cleanly once it restarted,
  since HBase's actual state lives in HDFS/ZooKeeper, not the
  container itself.

One honest caveat: the ALS model's ids are RetailRocket's real item
ids (large arbitrary numbers like `9877`), not this project's own demo
product catalog (ids 1-20, seeded via `scripts/seed_data.py`). They're
intentionally separate id spaces — RetailRocket exists to train and
evaluate a real model at real scale, the demo catalog exists to
exercise the live microservices end-to-end without needing a full
external catalog wired into every service — so `/recommendations/precomputed/{id}`
does not (and cannot) enrich its results via `product-service` the way
the other recommendation endpoints do.

## Time-series analytics: Cassandra

The Spark Structured Streaming job already computes per-product,
per-minute event-count windows for Z-score anomaly detection (see
[Speed layer](#speed-layer-spark-structured-streaming) above) — that
same computation is reused, not duplicated, to also populate a
Cassandra time-series table: one aggregate, two consumers.

`product_demand_by_minute` partitions by `(product_id, bucket_date)` —
a day's worth of per-minute counts for one product lives in a single
partition, the standard Cassandra time-series pattern that bounds
partition size by bucketing on time, rather than letting a partition
grow unboundedly the way a naive `PRIMARY KEY (product_id)` table
would. Each window write is a plain upsert of that window's *current*
total (not a counter increment) — Spark's `update` output mode re-emits
a window's running total on every micro-batch until it closes, so
incrementing on each write would double-count; overwriting with the
current total is both simpler and correct.

`analytics-service` exposes it live, distinct from its existing
Postgres-backed endpoints:

```bash
curl http://localhost:8005/analytics/demand-timeseries/4269
```

**Verified, not assumed:** pushed live traffic through Kafka, confirmed
the exact row count Spark's own batch log reported (`window_rows=5292`)
matched Cassandra's row count exactly, then queried a specific product
through both `cqlsh` directly and the live HTTP endpoint and got
identical data both times.

## Product catalog: MongoDB

`product-service` was migrated off Postgres onto MongoDB — a genuine
migration, not MongoDB bolted on alongside the old store. Product
documents are actually enriched (the "enriched product information"
this is meant to demonstrate), with nested, variable-shape data that's
awkward in a flat SQL row but natural in a document store: `tags`,
`specifications` (brand/color/weight as a nested object), `images`,
a GeoJSON `location` (real warehouse-city coordinates — the same field
shape Elasticsearch's geo-filtered search will read later, added once,
not re-seeded), and `rating`.

MongoDB has no native auto-increment, so product ids (still plain
integers, not ObjectIds — every other service in this project keys on
integer product ids: Kafka events, HBase rows, Cassandra partitions,
so switching `product-service` alone to ObjectId strings would ripple
pointlessly across the rest of the system for no real benefit here)
come from an atomic `$inc` against a dedicated counters document, the
standard MongoDB pattern for this.

**Verified, not assumed:** created a product through the API, queried
it back three ways — the API response, a direct `mongosh` query
against the container, and through `recommendation-service`'s existing
product-enrichment call — and got byte-identical enriched data all
three times.

## Product search: Elasticsearch

`product-service` indexes every product into Elasticsearch — full-text
across name/description/tags, plus a `geo_point` built from the same
warehouse `location` MongoDB already stores (see
[Product catalog](#product-catalog-mongodb) above), so nothing extra
had to be seeded for this to work.

```bash
# Full-text
curl "http://localhost:8001/products/search?q=noise+cancelling"

# Category filter
curl "http://localhost:8001/products/search?category=electronics"

# Geo-filtered: within 10km of a point
curl "http://localhost:8001/products/search?lat=30.2672&lon=-97.7431&radius_km=10"
```

Elasticsearch has no persistent volume here — it's a derived index, not
a source of truth, so on every restart it starts empty and
self-heals: `product-service`'s startup reindexes everything from
MongoDB (the actual source of truth), and every new product is also
indexed immediately on creation.

**Verified, not assumed — including that the geo-filter actually
filters, not just accepts the parameters:** looked up which 6 of the
20 seeded products share the Austin warehouse coordinates, ran a
10km-radius geo search centered on Austin, and got back exactly those
6 product ids and no others. Also created a new product through the
API and confirmed it was full-text searchable within about a second
(Elasticsearch's near-real-time refresh), without waiting for the
startup backfill.

## Running locally

Requires Docker and Docker Compose, with **at least 12GB** allocated
to Docker Desktop (Settings → Resources → Memory) — the growing set of
big-data infrastructure needs real headroom.

```bash
docker compose up --build
```

This starts Postgres, MongoDB, Elasticsearch, Kafka, HDFS (namenode +
datanode), ZooKeeper, HBase (master + regionserver + REST server),
Cassandra, a Spark standalone cluster (master + worker + the streaming
job), and all six FastAPI/worker services. Wait for the logs to settle, then seed some
sample data:

```bash
pip install requests
python scripts/seed_data.py
```

To verify real ingestion throughput (not an assumed number):

```bash
pip install httpx
python scripts/kafka_load_test.py --duration 15 --processes 8 --concurrency 32
```

On an 8-core machine this measures 500-650+ events/sec sustained
through the actual HTTP → Kafka path, with zero message loss verified
against Kafka's own offset counts.

To inspect the raw event archive `hdfs-sink` has written to HDFS:

```bash
docker compose exec hdfs-namenode hdfs dfs -ls /events/dt=$(date +%Y-%m-%d)/
docker compose exec hdfs-namenode hdfs dfs -cat /events/dt=$(date +%Y-%m-%d)/*.jsonl
```

The namenode's web UI is also available at
[http://localhost:9870](http://localhost:9870).

To run the product-popularity **MapReduce** batch job over the HDFS
archive (Hadoop Streaming, Python mapper/reducer — see
[Batch layer: MapReduce](#batch-layer-mapreduce) below):

```bash
docker compose --profile jobs run --rm mapreduce-product-popularity
```

To train the **ALS recommendation model**: download the RetailRocket
dataset from [Kaggle](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset)
(requires a free account — there's no unauthenticated mirror), unzip
it into `data/retailrocket/` (gitignored — the dataset isn't checked
into this repo), then load it into HDFS and run the job:

```bash
docker cp data/retailrocket/events.csv <namenode-container>:/tmp/events.csv
docker compose exec hdfs-namenode sh -c \
  "hdfs dfs -mkdir -p /datasets/retailrocket && hdfs dfs -put -f /tmp/events.csv /datasets/retailrocket/events.csv"
docker compose --profile jobs up -d als-training
docker compose logs -f als-training
```

See [ALS recommendation model](#als-recommendation-model-spark-mllib)
below for the real (not curated) Precision@10 result and why it looks
the way it does.

Then load those recommendations into **HBase** for live, low-latency
lookups (see [Serving layer: HBase](#serving-layer-hbase)):

```bash
docker compose --profile jobs up hbase-load-recommendations
curl http://localhost:8004/recommendations/precomputed/54
```

Then try the API:

```bash
# Personalized recommendations for user 1
curl http://localhost:8004/recommendations/1

# Items similar to product 3
curl http://localhost:8004/recommendations/similar/3

# Most popular products (fallback / cold-start)
curl http://localhost:8004/recommendations/popular

# Analytics
curl http://localhost:8005/analytics/top-products
curl http://localhost:8005/analytics/summary
```

Every service also exposes interactive API docs at
`http://localhost:<port>/docs` (FastAPI/Swagger UI), e.g.
`http://localhost:8004/docs`.

To stop everything:

```bash
docker compose down        # stop containers, keep data
docker compose down -v     # stop containers and wipe the Postgres volume
```

## Project layout

```
.
├── docker-compose.yml
├── infra/
│   ├── postgres/init-db.sql     # creates one database per service
│   ├── hadoop/                  # custom native-arm64 HDFS image
│   └── hbase/                   # custom native-arm64 HBase image
├── scripts/
│   ├── seed_data.py             # sample data + simulated traffic
│   └── kafka_load_test.py       # measures real ingestion throughput
├── jobs/
│   ├── product-popularity/      # MapReduce mapper/reducer + driver script
│   ├── spark-streaming/         # Structured Streaming: sessions + anomalies + Cassandra writer
│   └── als-training/            # Spark MLlib ALS training + evaluation
├── data/retailrocket/           # gitignored -- download separately, see below
└── services/
    ├── product-service/
    ├── user-service/
    ├── event-service/
    ├── recommendation-service/
    ├── analytics-service/
    ├── hdfs-sink/                # Kafka -> HDFS batching worker
    └── hbase-loader/             # HDFS -> HBase recommendations loader
```

Each service directory follows the same shape:

```
<service>/
├── Dockerfile
├── requirements.txt
└── app/
    ├── main.py        # FastAPI routes
    ├── database.py    # SQLAlchemy engine/session (services with a DB)
    ├── models.py       # SQLAlchemy models
    ├── schemas.py      # Pydantic request/response models
    └── crud.py         # DB access helpers
```

## Roadmap

This project is being built out toward a full Lambda Architecture.
Current status:

- [x] Five FastAPI microservices, database-per-service
- [x] Kafka (KRaft mode) as the event bus, replacing an earlier
      Redis Streams prototype — verified 500+ events/sec sustained
      ingestion with zero message loss
- [x] HDFS for raw event storage (batch layer input), fed by a
      Kafka-to-HDFS sink — verified zero data loss and restart-safe
      (committed offsets, no duplicate writes) on a native-arm64 build
- [x] MapReduce batch job for product popularity rankings (Hadoop
      Streaming, LocalJobRunner) — verified against two independent
      computations of the same ranking, identical results
- [x] Spark Structured Streaming for session reconstruction and
      Z-score demand anomaly detection (speed layer) — verified with a
      real baseline-then-spike traffic test (z_score=32.17 correctly
      flagged) and a real session that produced the exact expected
      HDFS output
- [x] Spark MLlib ALS collaborative filtering, trained on the real
      RetailRocket e-commerce dataset (2.75M events, downloaded from
      Kaggle, verified against published dataset stats) — measured
      Precision@10 = 0.0055, reported honestly rather than a curated
      or aspirational number (see
      [ALS recommendation model](#als-recommendation-model-spark-mllib)
      for why it's this low and what it would take to improve it)
- [x] HBase for low-latency user-item lookups (real HDFS-backed
      storage, not standalone mode) — verified with an exact-match
      lookup against the HDFS source data, ~10ms measured real-world
      latency, and unplanned proof of durability (survived an
      accidentally-destroyed master container without data loss)
- [x] Cassandra for time-series demand analytics, partitioned by
      (product_id, day) — the same window counts already computed for
      Z-score anomaly detection, reused rather than recomputed;
      verified row-count-exact against Spark's own batch log and
      cross-checked via `cqlsh` and the live HTTP endpoint
- [x] MongoDB for enriched product data — a genuine migration off
      Postgres (not bolted on alongside it), with real nested/
      variable-shape enrichment (tags, specs, images, geo location,
      rating); verified byte-identical data across the API, a direct
      `mongosh` query, and `recommendation-service`'s existing
      enrichment call
- [x] Elasticsearch for full-text and geo-filtered product search,
      self-healing (reindexes from MongoDB on every restart, since ES
      holds no persistent volume) — geo-filter verified against exactly
      the known set of products at one warehouse's coordinates, not
      just that the parameter was accepted
- [ ] A Flask dashboard tying search, live demand, and recommendations
      into one view

## Possible further extensions

- Add an API gateway / BFF in front of the microservices.
- Add authentication (JWT) between services and at the edge.
- Multiple recommendation-service replicas behind a load balancer.
