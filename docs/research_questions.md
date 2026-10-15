# Research questions

This first research brief uses only the real Amazon Reviews 2023 extract and the real RetailRocket files when present. It records questions, not findings; no sample or generated records are used.

## Questions supported by the Amazon review extract

1. **How are submitted ratings distributed?** Count review rows at each rating from 1 to 5, and report the total number of source rows. The source fields are `rating` and `asin`; the available dashboard endpoint currently reports rating counts.
2. **How do rating volume and average rating vary by product?** Group real reviews by `asin`, report the review count alongside the average rating, and avoid treating a small number of reviews as equally reliable as a large number. The source fields are `asin`, `rating`, and `timestamp`.
3. **Do ALS recommendations improve on a popularity baseline for held-out reviews?** Use a time-ordered train/test split based on `timestamp`; compare Recall@K and NDCG@K against recommending the most-reviewed products, and report catalog coverage. This is a proposed evaluation, not a result: the current ALS export does not yet calculate these metrics or compare a baseline.

Amazon's available review fields are `user_id`, `asin`, `rating`, and `timestamp`. The project's ASIN-to-product-ID map may be used for model inputs, but an ASIN is not itself a mapped `product_id`.

## Event questions currently blocked

RetailRocket can support questions about view-to-cart and cart-to-transaction rates by item or time using its real `visitorid`, `event`, `itemid`, `transactionid`, and `timestamp` fields. Those source files are not present in the inspected checkout or Desktop backup, so those analyses remain blocked until the real files are supplied.

Application event questions such as interaction volume by event type also remain blocked for dashboard-wide analysis: the current Cassandra endpoint reads recent events for one user and does not expose global aggregates. Do not substitute fabricated events or combine RetailRocket identifiers with Amazon identifiers without a verified crosswalk.

## Data and reporting rules

- Record the exact source file, row count, time range, filters, and metric definition for every reported result.
- Keep exploratory results separate from recommendation-quality claims; only report the latter after a held-out evaluation against a baseline.
- Show an unavailable state when a required real source or aggregate endpoint is missing.
