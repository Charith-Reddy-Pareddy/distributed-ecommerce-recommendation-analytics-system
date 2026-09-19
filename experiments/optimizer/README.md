# Optimizer experiments (RQ4)

Compares `serving-optimizer` enabled vs. disabled, measuring query
latency, index count, write overhead, Elasticsearch indexing latency,
and Cassandra query latency under the same synthetic traffic. Requires
`docker compose up --build` running. Results land in `results/` via
`experiments/common.py`.

Already run: see `results/optimizer_postgres.jsonl`,
`results/optimizer_cassandra.jsonl`, and
`results/optimizer_elasticsearch.jsonl`. Re-run `run.py` any time to
append fresh records.
