# Experiments

Reproducible runs backing the research questions in the main
[README](../README.md#research-questions) and
[docs/RESEARCH_REPORT.md](../docs/RESEARCH_REPORT.md). Each subfolder
holds one experiment's config, run script, and results:

```
experiments/
├── common.py            # shared result-recording helper
├── recommendation/      # RQ1-RQ3: weighting, model comparison, hybrid ablations
├── throughput/          # Kafka ingestion sweep: throughput, p50/p95/p99, lag
├── optimizer/           # RQ4: serving-optimizer on vs. off
└── fault_tolerance/      # consumer/store restart recovery, event loss/dup
```

Every run appends one JSON record per result via `common.record_result()`
-- config, dataset, model, metric, result, hardware, timestamp -- to that
experiment's `results/*.jsonl`. Nothing in this directory is reported in
the README or research report until it's actually been run.

Status: scaffolding only. Results land as each experiment is run.
