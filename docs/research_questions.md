# Research questions

This first research brief uses only the real Amazon Reviews 2023 extract and the real RetailRocket files when present. It records questions, not findings; no sample or generated records are used.

## Questions supported by the Amazon review extract

1. **How are submitted ratings distributed?** Count review rows at each rating from 1 to 5, and report the total number of source rows. The source fields are `rating` and `asin`; the available dashboard endpoint currently reports rating counts.
2. **How do rating volume and average rating vary by product?** Group real reviews by `asin`, report the review count alongside the average rating, and avoid treating a small number of reviews as equally reliable as a large number. The source fields are `asin`, `rating`, and `timestamp`.
3. **Do ALS recommendations improve on a popularity baseline for held-out reviews?** Compare Recall@K and NDCG@K against recommending the most-reviewed products, and report catalog coverage. A deterministic 1% pilot using per-user latest-review holdout and shared seen-item filtering is recorded in [the evaluation report](recommendation_evaluation.md). It is an initial result, not a full-data or statistical-significance claim.
4. **Can review history help predict a review's rating?** Treat a real Amazon review as one row and its numeric rating (1–5) as the regression target. Compare a training-set mean baseline with a regression model using reviewer history, product history, and calendar features derived only from earlier reviews.

Amazon's available review fields are `user_id`, `asin`, `rating`, and `timestamp`. The project's ASIN-to-product-ID map may be used for model inputs, but an ASIN is not itself a mapped `product_id`.

For this tabular task, eligible rows need nonempty user and ASIN values, a valid timestamp, and a finite rating from 1 to 5. Candidate features are the reviewer's earlier review count and mean rating, the product's earlier review count and mean rating, and the target review's calendar month. Each history feature must use only rows with a strictly earlier timestamp; rows at the same timestamp as the target are excluded. Do not use the target rating, later reviews, or unverified catalog metadata as features. Keep the full real dataset and compare a model trained before a global time cutoff with later reviews; keep equal-timestamp rows on the same side of the split. Report MAE and RMSE against the mean baseline. These are evaluation rules, not measured results.

## Event questions currently blocked

RetailRocket can support questions about view-to-cart and cart-to-transaction rates by item or time using its real `visitorid`, `event`, `itemid`, `transactionid`, and `timestamp` fields. Those source files are not present in the inspected checkout or Desktop backup, so those analyses remain blocked until the real files are supplied.

Application event questions such as interaction volume by event type also remain blocked for dashboard-wide analysis: the current Cassandra endpoint reads recent events for one user and does not expose global aggregates. Do not substitute fabricated events or combine RetailRocket identifiers with Amazon identifiers without a verified crosswalk.

## Data and reporting rules

- Record the exact source file, row count, time range, filters, and metric definition for every reported result.
- Keep exploratory results separate from recommendation-quality claims; only report the latter after a held-out evaluation against a baseline.
- Show an unavailable state when a required real source or aggregate endpoint is missing.
