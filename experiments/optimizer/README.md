# Optimizer experiments (RQ4)

Not yet run. Will compare `serving-optimizer` enabled vs. disabled,
measuring query latency, index count, write overhead, Elasticsearch
indexing latency, and Cassandra query latency under the same
synthetic traffic. Requires `docker compose up --build` running.
Results land in `results/` via `experiments/common.py`.
