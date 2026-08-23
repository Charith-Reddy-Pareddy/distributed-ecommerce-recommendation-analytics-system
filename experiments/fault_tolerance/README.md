# Fault-tolerance experiments

Not yet run. Will kill and restart individual consumers/stores
(recommendation-service, analytics-service, hdfs-sink, Elasticsearch,
Cassandra, HBase) under live load via scripted `docker compose stop`/
`start`, measuring recovery time, event loss/duplication (produced vs.
consumed counts), and recommendation freshness during the outage.
Requires `docker compose up --build` running. Results land in
`results/` via `experiments/common.py`.
