# Recommendation experiments (RQ1-RQ3)

Not yet run. Will hold:

- `generate_interactions.py` output -- the synthetic, catalog-native
  interaction log used for training and evaluation (see
  `scripts/generate_interactions.py`).
- `catalog_als/` -- ALS trained on that catalog-native log, sharing
  the product catalog's id space (unlike the existing RetailRocket
  model in `jobs/als-training/`).
- An offline evaluation harness scoring popularity, item-CF, catalog
  ALS, content-based, RetailRocket ALS, and hybrid blends on
  Precision@K/Recall@K/MAP@K/NDCG@K/latency.
- `results/` -- one `.jsonl` per experiment run, via `experiments/common.py`.
