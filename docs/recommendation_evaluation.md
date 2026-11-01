# Recommendation quality evaluation

Computed from the real Amazon Reviews 2023 extract and the project's real ASIN-to-product-ID map. This pilot uses a deterministic user sample of real source rows; it does not generate interaction data.

- Source file: `catalog_reviews.csv`
- Mapped review rows: 44,807
- Eligible users with a relevant held-out review: 13,424
- Held-out rule: latest mapped review per user; rating >= threshold is relevant
- Relevance threshold: rating >= 4.0
- User sample: 1% selected deterministically by user ID hash; all source rows for selected users are retained
- ALS iterations: 2
- Catalog size used for coverage: 7,675 mapped products

## Results at K=10

| Method | Recall@K | NDCG@K | Catalog coverage@K |
| --- | ---: | ---: | ---: |
| ALS | 0.0124 | 0.0061 | 2.57% |
| Popularity | 0.0223 | 0.0119 | 0.13% |

## Method and limits

The latest mapped review for each user is held out. Earlier rows form training data; repeated rows for the held-out user-item pair are removed from training. Only held-out ratings meeting the threshold count as relevant. ALS uses the project's implicit-feedback settings and fixed seed. Both methods now remove each user's training products before taking their top-K recommendations. The candidate pool includes extra products so filtering does not reduce the requested K when enough trained products are available.

The table above records the earlier pilot, whose popularity baseline did not filter each user's seen products. It is retained as a historical result and should not be compared directly with runs from the updated evaluator. This remains an offline user-sample evaluation, not an online experiment or evidence of sales impact. The per-user latest-review split and relevance threshold still affect results; a global time cutoff and full-data evaluation remain future work. Results do not generalize to RetailRocket or application events.
