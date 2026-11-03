# Recommendation quality evaluation

Computed from the real Amazon Reviews 2023 extract and the project's real ASIN-to-product-ID map. This pilot uses a deterministic user sample of real source rows; it does not generate interaction data.

- Source file: `catalog_reviews.csv`
- Mapped review rows: 44,807
- Eligible users with a relevant held-out review: 13,424
- Held-out rule: latest mapped review per user; rating >= threshold is relevant
- Relevance threshold: rating >= 4.0
- User sample: 1% selected deterministically by user ID hash; all source rows for selected users are retained
- ALS: rank 10, 2 iterations, regParam 0.1, alpha 1.0, seed 42
- Spark version: 4.2.1
- Catalog size used for coverage: 7,675 mapped products
- Candidate pool per method before seen-item filtering: up to 60
- Seen items returned after filtering (ALS / popularity): 0 / 0

## Results at K=10

| Method | Recall@K | NDCG@K | Catalog coverage@K |
| --- | ---: | ---: | ---: |
| ALS | 0.0129 | 0.0066 | 2.49% |
| Popularity | 0.0227 | 0.0121 | 0.18% |

## Method and limits

The latest mapped review for each user is held out. Earlier rows form training data; repeated rows for the held-out user-item pair are removed from training. Only held-out ratings meeting the threshold count as relevant. Both methods draw from training products, remove each user's training products, then take the first K remaining products. ALS uses the project's implicit-feedback settings and fixed seed; popularity ranks the training products by interaction count. The candidate pool is enlarged by the largest per-user seen set so filtering can still yield K recommendations when enough products are available.

This is an offline evaluation on a deterministic user sample, not an online experiment or evidence of sales impact. The per-user latest-review split and relevance threshold affect results; compare model changes using the same source and settings. Results do not generalize to RetailRocket or application events.

## Reproduce

Set `AMAZON_REVIEWS_PATH` and `AMAZON_CATALOG_MAP_PATH` to the real local files, then run:

```sh
PYTHONPATH=src .venv/bin/python -m ecommerce_analytics.recommendation_evaluation \
  --reviews "$AMAZON_REVIEWS_PATH" \
  --catalog-map "$AMAZON_CATALOG_MAP_PATH" \
  --output docs/recommendation_evaluation.md \
  --top-k 10 --relevant-rating 4.0 \
  --user-sample-percent 1 --max-iter 2
```
