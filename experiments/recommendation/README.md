# Recommendation experiments

This directory used to hold a full model-comparison suite (popularity,
item-CF, content-based, catalog-native ALS, NeuMF, a CF+ALS hybrid
blend, and a cross-category generalization check) trained and
evaluated on a synthetic interaction log generated over this project's
real product catalog. That log was fabricated -- Zipfian-skewed
popularity and a scripted view→cart→purchase funnel, not real user
behavior -- so however clearly it was labeled synthetic, treating
recommendation-quality numbers computed on it as research findings
wasn't an honest trade. That pipeline has been removed; see git
history if you need the code.

The only recommendation-quality study left in this project is the
real one: `jobs/als-training/` trains Spark MLlib ALS on the
[RetailRocket](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset)
clickstream dataset (~2.75M real events, ~1.4M users). Its item ids
are a disjoint space from this project's own demo catalog, so its
results are a standalone study, not something the live services can
serve enriched. Full methodology and results are in
[docs/RESEARCH_REPORT.md](../../docs/RESEARCH_REPORT.md).

What's left here is the dataset-agnostic tooling that study depends on
(or that is dataset-agnostic and worth keeping regardless):

- `metrics.py` -- precision/recall/MAP/NDCG@K. Pure functions over a
  ranked recommendation list and a held-out actual set; used to score
  the RetailRocket ALS model.
- `bootstrap.py` -- percentile-method bootstrap confidence intervals
  (`bootstrap_ci`, `paired_bootstrap_diff`) over per-user metric
  values. Not currently called by any script in this repo, kept
  because it's pure/reusable and has its own test coverage
  (`tests/test_bootstrap.py`).
- `scalability_benchmark.py` -- measures the *real* production
  `RecommendationEngine`'s
  (`services/recommendation-service/app/model.py`) similar-items
  latency as catalog size grows. Uses synthetic *traffic*, generated
  directly into the engine's dicts for speed rather than through
  Kafka/event-service, but that's a load-generation detail, not a
  recommendation-quality claim -- see the module's own docstring.
  Run with `python experiments/recommendation/scalability_benchmark.py
  [n_items ...]`.
- `results/` -- one `.jsonl` per experiment, append-only, via
  `experiments/common.py`'s `record_result()`.
