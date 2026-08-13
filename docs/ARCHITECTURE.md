# Architecture

Full component-by-component write-up. See the [README](../README.md) for
the high-level diagram and service table.

## Recommendation algorithm

`recommendation-service` implements **item-based collaborative
filtering**:

1. Every event (`view`, `add_to_cart`, `purchase`) is weighted (1, 3,
   5 respectively) and folded into an in-memory user→item and
   item→user interaction matrix.
2. Finds items similar to what the user already interacted with,
   using **cosine similarity** between interaction vectors.
3. Candidate items are scored by `similarity × the user's weight on
   the seed item`, summed across all the user's interactions, and
   ranked.
4. New users with no history fall back to a popularity ranking.

State is rebuilt on startup by replaying the Kafka topic, so the
service is stateless from a deployment standpoint.

## Batch layer: MapReduce

`jobs/product-popularity/` is a real Hadoop **MapReduce** job (via
Hadoop Streaming — plain Python mapper/reducer reading stdin/writing
stdout) that computes the same weighted popularity ranking as
`recommendation-service`'s fallback, but as a batch computation over
the full HDFS event archive:

1. **Mapper** emits `product_id -> weight` per archived event.
2. Hadoop's shuffle groups and sorts these by key.
3. **Reducer** sums weights per product using that sort order
   (compare-to-previous-key), not an in-memory dict.
4. A driver script sorts the result and writes the final ranked file
   to HDFS.

Runs via Hadoop's `LocalJobRunner` rather than YARN, to save a couple
of JVM services worth of memory for the rest of the stack.

It's a one-shot job, kept out of `docker compose up` behind a Compose
profile — see [Running locally](RUNNING_LOCALLY.md).

## Speed layer: Spark Structured Streaming

`jobs/spark-streaming/session_and_anomaly.py` runs on a Spark standalone
cluster and reads the Kafka `events` topic live, running two
independent streaming queries:

1. **Session reconstruction** groups each user's events using Spark's
   `session_window` (5-minute inactivity gap by default), writing
   finalized sessions to HDFS once the watermark confirms they're closed.
2. **Per-product demand anomaly detection** counts events per product
   in 1-minute tumbling windows and flags a window as anomalous when
   it's more than 2.5 standard deviations from that product's running
   mean, computed incrementally with **Welford's online algorithm**.

A controlled 60-event burst produced a z_score of 32.17 and was
flagged by the detector (see Benchmarks in the README) -- one test,
not a claim of general accuracy. Session and watermark durations are
configurable via env vars for faster local testing.

## ALS recommendation model (Spark MLlib)

`jobs/als-training/train_als.py` trains an **ALS (Alternating Least
Squares)** implicit-feedback model in Spark MLlib on the real, public
[RetailRocket e-commerce dataset](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset)
(~2.75M events over 4.5 months), using the same event-weighting scheme
as the rest of the project. Trained with `implicitPrefs=True`, since
view/cart/purchase counts are engagement signals, not ratings --
weight becomes ALS's confidence input, not a value to predict.

Evaluated on a held-out test split:

```
Precision@10 = 0.0055
```

RetailRocket is extremely sparse (median interactions per user is 1,
across ~1.4M users and ~235K items) -- the dominant reason the number
is low, not an implementation bug. Evaluation only counts users with
at least 5 interactions (`MIN_INTERACTIONS_FOR_EVAL`), since with 1-2
a held-out item is mostly noise; every interaction still trains the
model regardless. A production system would want richer item
features or a hybrid content + collaborative approach here.

The trained model and a precomputed top-10-recommendations table are
both persisted to HDFS.

## Serving layer: HBase

HDFS is fine for batch reads but isn't a point-lookup store, so a real
HBase cluster (master + regionserver + ZooKeeper, backed by actual
HDFS storage rather than standalone mode) serves the ALS output for
low-latency lookups:

```bash
curl http://localhost:8004/recommendations/precomputed/54
```

Measured point-lookup latency over the REST layer: **~10ms average**.

One caveat: the ALS model uses RetailRocket's own item ids, not this
project's demo catalog ids, so `/recommendations/precomputed/{id}`
can't be enriched through `product-service` like the other endpoints.

## Time-series analytics: Cassandra

Per-product, per-minute window counts (already computed for anomaly
detection) populate a Cassandra table, `product_demand_by_minute`,
partitioned by `(product_id, bucket_date)` so each partition holds
one product's counts for one day. Each write upserts the window's
current total rather than incrementing a counter, since Spark's
`update` mode would otherwise double-count.

`analytics-service` exposes it live:

```bash
curl http://localhost:8005/analytics/demand-timeseries/4269
```

## Product catalog: MongoDB

`product-service` runs on MongoDB rather than Postgres, storing
enriched, variable-shape product documents — `tags`,
`specifications`, `images`, a GeoJSON `location`, and `rating` — that
would be awkward as flat SQL rows. Product ids stay plain integers
via an atomic counter document (MongoDB has no built-in
auto-increment), since every other service keys on integer ids.

The catalog contains **300 Amazon products** — titles, brands,
prices, images, ratings, and ASINs — pulled from the public
[McAuley-Lab/Amazon-Reviews-2023](https://amazon-reviews-2023.github.io/)
dataset via `scripts/fetch_amazon_products.py`.

## Product search: Elasticsearch

`product-service` indexes every product for full-text search,
structured filters (category, brand, price range, minimum rating),
sort, and geo-distance filtering over the same warehouse location
MongoDB already stores:

```bash
curl "http://localhost:8001/products/search?q=noise+cancelling"
curl "http://localhost:8001/products/search?category=electronics&brand=Anker&price_max=50&rating_min=4&sort=rating_desc"
curl "http://localhost:8001/products/search?lat=30.2672&lon=-97.7431&radius_km=10"
```

Elasticsearch has no persistent volume here, so on restart
`product-service` reindexes everything from MongoDB (the actual
source of truth), and every new product is indexed immediately on
creation.

Runs single-node with `number_of_replicas=0` and security disabled --
fine for a local demo, but a real deployment would need a proper
cluster and auth in front of it.

## Serving layer performance: indexing, partitioning & refresh intervals

I added a small optimizer (`serving-optimizer`, port 8007) that
watches recent usage and adjusts Postgres, Cassandra, and
Elasticsearch based on what it's seeing. I based it on
[auto-indexing](https://github.com/nimit-pasricha/auto-indexing),
which does the same for Postgres alone -- I extended it to Cassandra
and Elasticsearch, reading telemetry straight from each store instead
of building a separate pipeline.

**Postgres: index creation and cleanup**

- *Problem:* nothing was watching `pg_stat_statements` for repeatedly
  filtered columns with no index to back them.
- *Solution:* every 15s poll, a column that crosses 20 calls since the
  last cycle gets `CREATE INDEX CONCURRENTLY`'d, gated by table-size
  (≥20 rows), cardinality (≥5% distinct values), and write/read ratio
  (≤3.0) guards so it doesn't index tiny tables, low-cardinality
  columns, or write-heavy ones. An index with no matching query for 3
  consecutive cycles gets dropped.
- *Trade-off:* it won't index a column for query patterns it hasn't
  seen yet, even if a human would.

**Cassandra: hot/cold partition rollups**

- *Problem:* `product_demand_by_minute` has one row per product per
  minute, so a wide-time-range dashboard query for a busy product
  reads a lot of rows for one number.
- *Solution:* each cycle sums every product's volume over the last 60
  minutes; products at or above 50 events get marked hot and get an
  hourly rollup in `product_demand_by_hour`, so wide-range queries
  read pre-aggregated hours instead of raw minutes. Recent partitions
  come from a separate `active_partitions` table (partitioned by date
  alone), not a scan of every partition `product_demand_by_minute`
  has ever had.
- *Trade-off:* at this project's data volume, partitions are nowhere
  near Cassandra's real large-partition threshold, so the rollup
  isn't solving a problem this demo's traffic actually has.

**Elasticsearch: refresh-interval tuning**

- *Problem:* the default 1s `refresh_interval` means every index
  write pays a near-real-time refresh cost, which is fine for one
  product at a time but wasteful during a bulk load.
- *Solution:* indexing ops are tracked per 15s window; crossing 15 ops
  in a window (above normal single-product-create traffic) raises
  `refresh_interval` to 5s until the burst passes, then resets it to 1s.
- *Trade-off:* products indexed during a burst take up to 5s instead
  of ~1s to become searchable; normal traffic is unaffected.

Decisions are logged to their own audit table and surfaced live in the
dashboard's Autonomous Tuning panel.

## Dashboard: Flask

A single web page (`http://localhost:8006`) ties everything above
together as a thin server-side proxy — the browser only ever talks to
Flask, never directly to the backend services.

- **Product Search** — full-text, filters, sort, and geo-search
  against Elasticsearch, with real product photos and a **View on
  Amazon** link built from each product's ASIN.
- **Top Products** and **Event Volume by Day** — Chart.js panels fed
  by `analytics-service`'s SQL aggregates.
- **Personalized Recommendations** — precomputed ALS recommendations
  served from HBase.
- **Autonomous Tuning** — a live feed of `serving-optimizer`'s
  decisions and status.

## Trade-offs and lessons learned

- **Two disconnected id spaces.** ALS trains on RetailRocket's own
  item ids; the demo catalog has its own (see Serving layer: HBase
  above) -- `/recommendations/precomputed/{id}` can't be enriched
  through `product-service` as a result. In a v2 I'd train ALS on the
  demo catalog directly, or add an explicit id mapping.

- **Spark's `update` output mode was the trickiest bug to get
  right.** The Cassandra writer upserts each window's running total
  instead of incrementing a counter, since a naive increment
  double-counts every time Spark re-emits a window.

- **Autonomous systems need a real failure to prove themselves.**
  `serving-optimizer`'s Postgres tuner had nothing to act on until
  `user-service` had a real unindexed, filtered query in traffic --
  the `country` filter exists specifically to give it one.

- **Running ~20 containers on one machine is its own kind of ops
  work.** Every JVM-heavy store needed its own memory-limit tuning
  pass to stop them competing for the same 12GB -- a cost of the
  single-host demo, not of the architecture itself.

- **What I'd cut in a v2:** the in-memory CF model and the batch ALS
  model both rank products for a user with no blending strategy
  between them. A v2 would pick one, or define how they combine,
  instead of exposing both as separate endpoints.
