# Fault-tolerance experiments

Kills and restarts individual consumers/stores (recommendation-service,
analytics-service, hdfs-sink, Elasticsearch, Cassandra, HBase) under
live load via scripted `docker compose stop`/`start`, measuring
recovery time, event loss/duplication (produced vs. consumed counts),
and recommendation freshness during the outage. Requires
`docker compose up --build` running. Results land in `results/` via
`experiments/common.py`.

Already run: see `results/fault_hdfs_sink.jsonl`,
`results/fault_elasticsearch.jsonl`, and
`results/fault_recommendation_service.jsonl`. Re-run `run.py` any time
to append fresh records.
