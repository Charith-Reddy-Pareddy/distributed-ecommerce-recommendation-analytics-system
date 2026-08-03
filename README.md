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
| `product-service` | 8001 | Product catalog CRUD | `product_db` (Postgres) |
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

## Running locally

Requires Docker and Docker Compose, with **at least 12GB** allocated
to Docker Desktop (Settings → Resources → Memory) — the growing set of
big-data infrastructure needs real headroom.

```bash
docker compose up --build
```

This starts Postgres, Kafka, HDFS (namenode + datanode), and all six
services. Wait for the logs to settle, then seed some sample data:

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
│   └── hadoop/                  # custom native-arm64 HDFS image
├── scripts/
│   ├── seed_data.py             # sample data + simulated traffic
│   └── kafka_load_test.py       # measures real ingestion throughput
└── services/
    ├── product-service/
    ├── user-service/
    ├── event-service/
    ├── recommendation-service/
    ├── analytics-service/
    └── hdfs-sink/                # Kafka -> HDFS batching worker
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
- [ ] MapReduce batch job for product popularity rankings
- [ ] Spark Structured Streaming for session reconstruction and
      Z-score demand anomaly detection (speed layer)
- [ ] Spark MLlib ALS collaborative filtering, trained on the
      RetailRocket e-commerce dataset, with a measured Precision@10
- [ ] HBase for low-latency user-item lookups
- [ ] Cassandra for time-series demand analytics
- [ ] MongoDB for enriched product data
- [ ] Elasticsearch for full-text and geo-filtered product search
- [ ] A Flask dashboard tying search, live demand, and recommendations
      into one view

## Possible further extensions

- Add an API gateway / BFF in front of the microservices.
- Add authentication (JWT) between services and at the edge.
- Multiple recommendation-service replicas behind a load balancer.
