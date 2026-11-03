# Research report: review analytics and recommendations

This report studies product ratings and offline recommendation quality using the real Amazon Reviews 2023 data. It asks whether ALS ranks relevant held-out products more effectively than a popularity baseline. It also describes the observed ratings and review volume. These analyses do not estimate sales impact or causal effects.

## Research question

For users with a relevant latest review, does Spark ALS rank the held-out product more effectively at K=10 than recommending the most-reviewed training products? Recall@10 and NDCG@10 measure ranking quality; catalog coverage describes how broadly each method recommends products. Descriptive rating and review-volume summaries provide context for the dataset.

## Data

The primary source is the Amazon Reviews 2023 review extract, `catalog_reviews.csv`, kept outside this repository. It contains `user_id`, `asin`, `rating`, and `timestamp`. The recommendation job joins ASINs to the project's real ASIN-to-integer product map; the inspected local source included 7,675 mapped products. An ASIN and the mapped `product_id` are distinct identifiers.

The report's descriptive statistics use the available 4,500,228 review rows. The recommendation pilot uses the mapped rows and a deterministic 1% sample of users, retaining all source rows for sampled users. The source files and ID map are not committed, so reproducing the analysis requires obtaining the same source data and setting the local paths described in [the evaluation report](recommendation_evaluation.md).

RetailRocket event files were not present in the inspected checkout or backup. Its visitor and item IDs also have no verified mapping to Amazon users or products. RetailRocket and application-event analyses are therefore outside this report's measured results.

## Methods and evaluation setup

Descriptive statistics count reviews by rating and summarize review counts and mean ratings by product. Recommendation quality is evaluated offline with each sampled user's latest mapped review held out. Earlier reviews train the model, duplicate training rows for the held-out user-item pair are removed, and held-out ratings of at least 4.0 count as relevant. ALS and popularity use the same per-user seen-item exclusion and top-10 cutoff. The pilot uses Spark ALS with rank 10, two iterations, regParam 0.1, alpha 1.0, and seed 42. The full protocol and reproducible command are in [Recommendation quality evaluation](recommendation_evaluation.md).

This is a deterministic pilot, not a full-data evaluation. Its results should be treated as descriptive until they are checked across broader samples and training settings. No synthetic interaction records are used.

## Descriptive results

- Source file: `catalog_reviews.csv`
- Review rows: 4,500,228
- Distinct reviewed ASINs: 7,675
- Review timestamp range (UTC): 2000-07-12T20:01:47+00:00 to 2023-09-09T13:36:39.830000+00:00
- Weighted mean rating: 4.3815
- Unweighted mean of ASIN rating averages: 4.4034

## Rating distribution

| Rating | Reviews | Share |
| ---: | ---: | ---: |
| 1.0 | 294,820 | 6.55% |
| 2.0 | 170,631 | 3.79% |
| 3.0 | 268,134 | 5.96% |
| 4.0 | 556,063 | 12.36% |
| 5.0 | 3,210,580 | 71.34% |

## Reviews per product

The median is 253.0 reviews per ASIN; the 90th percentile is 1,304. Counts range from 38 to 39,313; 0 ASINs have one review.

## Interpretation and limits

These are descriptive summaries of observed reviews, not estimates of sales or causal effects. Product averages have unequal review counts, so they should be read alongside the volume distribution above.

ALS recommendation quality has not been measured here. A separate time-ordered held-out evaluation against a popularity baseline is required before making a quality claim. RetailRocket and global application-event questions remain blocked as described in [the source notes](dashboard-data-sources.md).
