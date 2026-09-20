# Recommendation experiments (RQ1-RQ3)

No Docker needed -- everything here runs locally against
`data/interactions_train.parquet` / `data/interactions_test.parquet`
(built by `split_interactions.py` from `generate_interactions.py`'s
synthetic, catalog-native interaction log, sharing the real 300-product
catalog's id space). See [docs/RESEARCH_REPORT.md](../../docs/RESEARCH_REPORT.md)
for full methodology, results, and formal algorithm definitions.

Run in this order for a from-scratch reproduction:

```bash
python scripts/generate_interactions.py     # writes data/interactions.parquet
python experiments/recommendation/split_interactions.py

python experiments/recommendation/offline_models.py       # popularity, item-CF, content-based
python experiments/recommendation/catalog_als/train.py    # Spark MLlib ALS (local-mode PySpark)
python experiments/recommendation/neural_cf/train.py      # NeuMF (PyTorch, CPU, ~1 min)
python experiments/recommendation/hybrid.py                # CF+ALS / CF+content blends

python experiments/recommendation/temporal_eval.py         # RQ3: temporal split validity check
python experiments/recommendation/scalability_benchmark.py # CF neighbor-cache scaling
python experiments/recommendation/ablation_weights.py
python experiments/recommendation/ablation_als_hyperparams.py
python experiments/recommendation/ablation_cf_neighbors.py
python experiments/recommendation/bootstrap_ci.py           # significance CIs on the model comparison
```

Every script appends its result(s) to `results/*.jsonl` via
`experiments/common.py`'s `record_result()` -- nothing is overwritten,
so re-running a script adds another timestamped record rather than
destroying the last run's.

- `catalog_als/` -- ALS trained on the catalog-native log, sharing the
  product catalog's id space (unlike the separate RetailRocket model
  in `jobs/als-training/`, which stays in its own id space as a larger
  sparsity/weighting study).
- `cross_category/` -- RQ5: does the RQ2 model comparison generalize to
  a different Amazon category, or is it specific to this project's own
  4-category catalog mix? Own catalog, own synthetic interaction log,
  kept separate for the same reason `catalog_als/` is kept separate
  from `jobs/als-training/`.
- `neural_cf/` -- NeuMF (He et al., WWW 2017), a PyTorch GMF+MLP fusion
  model trained with implicit-feedback negative sampling -- the only
  actual neural network in this project.
- `offline_models.py` -- popularity, item-CF (the real production
  `RecommendationEngine`), and content-based (TF-IDF).
- `hybrid.py` -- min-max normalized, alpha-weighted blends (CF+ALS,
  CF+content), swept over alpha.
- `results/` -- one `.jsonl` per experiment, append-only.
