# Catalog indexing scalability

The archived run is in [catalog_indexing_benchmark.json](../results/catalog_indexing_benchmark.json). It measured the Elasticsearch bulk catalog index path using real records from the local Amazon Reviews 2023 product catalog.

## Results

Three sequential repetitions were measured at each batch size on one Apple Silicon Mac using Docker Desktop 4.94.0, Docker Engine 29.8.2, and Elasticsearch 8.19.3. Elasticsearch ran as a single Compose node with a 512 MB heap. The input catalog contained 7,675 records, with a matching ASIN-to-product-ID map; the source files are not in Git.

| Products | Median elapsed time | Median measured throughput | Observed elapsed range |
| ---: | ---: | ---: | ---: |
| 100 | 0.136 s | 736 products/s | 0.127–0.228 s |
| 300 | 0.151 s | 1,984 products/s | 0.140–0.227 s |
| 1,000 | 0.270 s | 3,699 products/s | 0.250–0.356 s |

## How to read these measurements

The timer starts after selecting the sample and stops after indexing and deleting that run’s temporary index. Throughput is therefore the sample count divided by the full timed region, including cleanup; it is not an isolated sustained indexing rate. The JSON file retains the individual repetitions and their calculated rates.

These are small, single-node local measurements, not a full-catalog or concurrent-user benchmark. They do not measure search latency, ingestion throughput, Kafka or Cassandra load, recommendation serving, or behavior under failures. Do not extrapolate them directly to a production cluster.

## Reproduction status

The recorded run used local Amazon source files and a local working checkout. Those data files are excluded from Git. Also, the benchmark runner calls `ElasticsearchProductIndex.index_catalog`, while that batch method is not present in the current `main` version of `services/product_service/app/search.py`. The archived result is therefore a record of the completed local run, but this repository revision does not yet reproduce it end to end from a clean checkout.
