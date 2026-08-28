# Running locally

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

To measure ingestion throughput (see Benchmarks in the README):

```bash
pip install httpx
python scripts/kafka_load_test.py --duration 15 --processes 8 --concurrency 32
```

To inspect the raw event archive `hdfs-sink` has written:

```bash
docker compose exec hdfs-namenode hdfs dfs -ls /events/dt=$(date +%Y-%m-%d)/
docker compose exec hdfs-namenode hdfs dfs -cat /events/dt=$(date +%Y-%m-%d)/*.jsonl
```

The namenode's web UI is also available at
[http://localhost:9870](http://localhost:9870).

To run the **MapReduce** popularity job over the HDFS archive:

```bash
docker compose --profile jobs run --rm mapreduce-product-popularity
```

To train the **ALS model**: download the RetailRocket dataset from
[Kaggle](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset)
(free account required), unzip into `data/retailrocket/` (gitignored),
then load it into HDFS and run the job:

```bash
docker cp data/retailrocket/events.csv <namenode-container>:/tmp/events.csv
docker compose exec hdfs-namenode sh -c \
  "hdfs dfs -mkdir -p /datasets/retailrocket && hdfs dfs -put -f /tmp/events.csv /datasets/retailrocket/events.csv"
docker compose --profile jobs up -d als-training
docker compose logs -f als-training
```

Then load those recommendations into **HBase**:

```bash
docker compose --profile jobs up hbase-load-recommendations
curl http://localhost:8004/recommendations/precomputed/54
```

Then try the API:

```bash
curl http://localhost:8004/recommendations/1
curl http://localhost:8004/recommendations/similar/3
curl http://localhost:8004/recommendations/popular

curl http://localhost:8005/analytics/top-products
curl http://localhost:8005/analytics/summary

curl http://localhost:8007/tuning/decisions
curl http://localhost:8007/tuning/status
```

Every service also exposes interactive API docs at
`http://localhost:<port>/docs` (FastAPI/Swagger UI).

To stop everything:

```bash
docker compose down        # stop containers, keep data
docker compose down -v     # stop containers and wipe volumes
```

## Recommendation experiments

The offline evaluation harness in `experiments/recommendation/` (popularity,
item-CF, content-based, and catalog-ALS) doesn't need Docker at all -- ALS
trains in local-mode PySpark instead of the Docker Spark+HDFS cluster.
Set up a project-local virtualenv once:

```bash
python3 -m venv .venv
./.venv/bin/pip install -r tests/requirements.txt -r experiments/requirements.txt
```

Then generate the synthetic interaction log, split it, and evaluate:

```bash
./.venv/bin/python scripts/generate_interactions.py
./.venv/bin/python experiments/recommendation/split_interactions.py
./.venv/bin/python experiments/recommendation/offline_models.py       # popularity, item-CF, content-based
./.venv/bin/python experiments/recommendation/catalog_als/train.py    # catalog-native ALS
```

Results land in `experiments/recommendation/results/offline_models.jsonl`,
one JSON record per model per run. The existing RetailRocket ALS job
(`jobs/als-training/`) is unrelated to this and still needs the Docker
Spark+HDFS stack -- see the RetailRocket steps above.

## Monitoring: Prometheus + Grafana

Every FastAPI service exposes request-count and latency metrics at
`/metrics` (`services/*/app/metrics.py`). Prometheus and Grafana are
opt-in, behind a Compose profile, so the default `docker compose up`
memory footprint is unchanged:

```bash
docker compose --profile monitoring up -d prometheus grafana
```

Prometheus scrapes all six services every 15s
([http://localhost:9090](http://localhost:9090), check
Status → Targets). Grafana ([http://localhost:3000](http://localhost:3000),
anonymous access enabled for local use) auto-provisions the Prometheus
datasource and a "Distributed E-Commerce: Service Metrics" dashboard
(`infra/grafana/dashboards/services.json`) with request rate, p95
latency, status codes, and per-endpoint breakdown, all by service.

## Tests

`tests/` covers pure logic that doesn't need the stack running: the
recommendation engine's cosine similarity, `sql_analyzer`'s query
classification, `pg_optimizer`'s call-delta math, the MapReduce
mapper/reducer, and event-service's Pydantic validation. Imports the
real service code directly, no Docker required:

```bash
pip install -r tests/requirements.txt
pytest
```

CI (`.github/workflows/ci.yml`) runs `compileall` and this suite on
every push and pull request.

## Project layout

```
.
├── docker-compose.yml
├── pytest.ini
├── .github/workflows/ci.yml     # compileall + pytest on push/PR
├── tests/                        # unit tests, see Tests above
├── infra/
│   ├── postgres/init-db.sql     # creates one database per service
│   ├── hadoop/                  # custom native-arm64 HDFS image
│   └── hbase/                   # custom native-arm64 HBase image
├── scripts/
│   ├── fetch_amazon_products.py # pulls 300 product records from Amazon-Reviews-2023
│   ├── seed_data.py             # loads data/amazon_products.json + simulated traffic
│   └── kafka_load_test.py       # measures real ingestion throughput
├── jobs/
│   ├── product-popularity/      # MapReduce mapper/reducer + driver script
│   ├── spark-streaming/         # Structured Streaming: sessions + anomalies + Cassandra writer
│   └── als-training/            # Spark MLlib ALS training + evaluation
├── data/
│   ├── amazon_products.json     # 300 Amazon products (committed -- small enough to track)
│   └── retailrocket/            # gitignored -- download separately, see above
└── services/
    ├── product-service/
    ├── user-service/
    ├── event-service/
    ├── recommendation-service/
    ├── analytics-service/
    ├── hdfs-sink/                # Kafka -> HDFS batching worker
    ├── hbase-loader/             # HDFS -> HBase recommendations loader
    ├── serving-optimizer/        # autonomous Postgres/Cassandra/ES tuner
    └── dashboard/                # Flask UI: search, analytics, recommendations
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
