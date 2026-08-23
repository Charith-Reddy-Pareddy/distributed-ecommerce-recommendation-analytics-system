# Throughput experiments

Not yet run. Will sweep Kafka ingestion at 100/250/500/750/1000/1500
events/sec against the live stack, measuring ingestion throughput,
p50/p95/p99 latency, Kafka consumer lag, CPU, and memory. Requires
`docker compose up --build` running with 12GB+ allocated to Docker
Desktop. Results land in `results/` via `experiments/common.py`.
