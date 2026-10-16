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

The latest mapped review for each user is held out. Earlier rows form training data; repeated rows for the held-out user-item pair are removed from training. Only held-out ratings meeting the threshold count as relevant. ALS uses the project's implicit-feedback settings and fixed seed. The popularity baseline ranks products by training-row count and uses the same global top-K list for each user without per-user seen-item filtering, so treat it as a simple exploratory baseline.

This is an offline pilot on the disclosed user sample, not a full-catalog evaluation, online experiment, or evidence of sales impact. The ALS and popularity candidate filtering differ. The split and relevance threshold affect the result; compare future model changes using the same source and settings. Results do not generalize to RetailRocket or application events.
