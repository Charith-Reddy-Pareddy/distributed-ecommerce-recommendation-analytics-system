# Distributed E-Commerce Recommendation Analytics System

An event-driven microservices system that simulates an e-commerce
platform: users browse and buy products, their interactions stream
through a shared event bus, and two independent downstream services
turn that stream into personalized recommendations and business
analytics.

## Architecture

```
                 ┌────────────────┐        ┌─────────────────┐
   client ─────► │  event-service  │───────►│  Redis Stream    │
                 │  (POST /events) │        │  "events_stream" │
                 └────────┬────────┘        └───────┬─────┬────┘
                          │                          │     │
                    writes to                  consumes│   │consumes
                     event_db                          │   │
                          │                            ▼   ▼
                          │                 ┌───────────────┐ ┌──────────────────┐
                          │                 │ recommendation-│ │ analytics-service │
                          │                 │ service        │ │                   │
                          │                 │ (item-based    │ │ (rolling product/ │
                          │                 │  collaborative │ │  daily stats)     │
                          │                 │  filtering)    │ │                   │
                          │                 └───────┬────────┘ └─────────┬─────────┘
                          │                          │                    │
                          │                    reads product           writes to
                          │                    details from            analytics_db
                          │                    product-service
                          ▼                          ▼
                 ┌─────────────────┐        ┌─────────────────┐
                 │ product-service │◄───────│  user-service    │
                 │ (catalog CRUD)  │        │  (user CRUD)     │
                 └─────────────────┘        └─────────────────┘
```

**Services** (each is its own FastAPI app, own database, own Docker image):

| Service | Port | Responsibility | Datastore |
|---|---|---|---|
| `product-service` | 8001 | Product catalog CRUD | `product_db` (Postgres) |
| `user-service` | 8002 | User CRUD | `user_db` (Postgres) |
| `event-service` | 8003 | Ingests view/cart/purchase events, publishes them to Redis | `event_db` (Postgres) + Redis Stream |
| `recommendation-service` | 8004 | Consumes the event stream, builds an item-item similarity model in memory, serves recommendations | Redis (event source only, stateless otherwise) |
| `analytics-service` | 8005 | Consumes the event stream independently, maintains rolling product/day counters | `analytics_db` (Postgres) |

**Why this shape:** `event-service` doesn't know or care who reads
its events — `recommendation-service` and `analytics-service` are
independent consumers of the same Redis Stream, each building its own
read model. This is the same event-driven / CQRS-ish pattern used in
real distributed analytics pipelines (Kafka + independent consumer
groups), simplified here to Redis Streams so the whole thing runs
locally with `docker compose up`.

Each service also owns its own database (`product_db`, `user_db`,
`event_db`, `analytics_db` all live in one Postgres container for
local dev, but are logically and physically separate schemas —
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

State is rebuilt on startup by replaying the entire Redis Stream, so
the service is stateless from a deployment standpoint — kill it,
restart it, and it reconstructs itself from the event log.

## Running locally

Requires Docker and Docker Compose.

```bash
docker compose up --build
```

This starts Postgres, Redis, and all five services. Wait for the logs
to settle, then seed some sample data:

```bash
pip install requests
python scripts/seed_data.py
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
├── infra/postgres/init-db.sql   # creates one database per service
├── scripts/seed_data.py         # sample data + simulated traffic
└── services/
    ├── product-service/
    ├── user-service/
    ├── event-service/
    ├── recommendation-service/
    └── analytics-service/
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

## Possible extensions

- Swap the in-memory recommendation model for a persisted one (e.g.
  matrix factorization with `implicit`, written to Redis) to support
  multiple recommendation-service replicas.
- Add an API gateway / BFF in front of the five services.
- Replace Redis Streams with Kafka for higher-throughput event
  ingestion.
- Add authentication (JWT) between services and at the edge.
