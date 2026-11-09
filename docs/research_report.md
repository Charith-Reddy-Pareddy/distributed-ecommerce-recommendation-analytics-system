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

## Recommendation pilot results

The deterministic 1% pilot included 44,807 mapped review rows and 13,424 users with a relevant held-out review. It compares Spark ALS with a popularity ranking at K=10, using the same per-user seen-item filtering for both methods.

| Method | Recall@10 | NDCG@10 | Catalog coverage@10 |
| --- | ---: | ---: | ---: |
| ALS | 1.29% | 0.66% | 2.49% |
| Popularity | 2.27% | 1.21% | 0.18% |

Popularity leads ALS by 0.98 percentage points in Recall@10 and 0.55 points in NDCG@10. ALS has broader catalog coverage in this run, at 2.49% versus 0.18%. Thus ALS exposed more distinct products, while popularity ranked more held-out relevant products successfully near the top. The values are rounded; the full settings and reproducible command are in [the evaluation report](recommendation_evaluation.md).

## Interpretation and limits

The ratings are concentrated at five stars (71.34%), so the high weighted mean should be read alongside the rating distribution and review counts. Product averages have unequal numbers of reviews and are descriptive, not estimates of sales or causal effects.

The recommendation comparison is also descriptive, not a significance test. It uses a deterministic 1% user sample, one latest-review holdout per eligible user, a relevance threshold of 4.0, and two ALS iterations. The current results do not establish how the difference changes with a larger sample, different training settings, or full-data evaluation. Popularity is the stronger quality baseline under this pilot protocol; ALS's higher coverage is a separate exposure tradeoff, not evidence of better ranking quality.

RetailRocket-backed and global application-event analyses remain unavailable: the real RetailRocket files are absent from the inspected checkout and the Cassandra API does not expose global event aggregates. Those questions need their real inputs or an aggregate endpoint before they can be evaluated.

## Tabular rating sensitivity

The sensitivity analysis used 16,216 real Subscription Boxes reviews at the 70th, 80th, and 90th percentile timestamp cutoffs. At each cutoff, the training-mean baseline and a 30-tree random forest with maximum depth 6 were compared using seeds 42, 7, and 17. Rows before the cutoff formed training data; rows at or after it formed the test set. The input file hash and run settings are in [the sensitivity results](../results/tabular_rating_sensitivity.json).

| Cutoff | Train / test rows | Baseline MAE | Forest MAE mean (range) | MAE reduction | Baseline RMSE | Forest RMSE mean (range) | RMSE reduction |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 70th percentile | 11,208 / 5,008 | 1.3929 | 1.3648 (1.3634–1.3663) | 2.02% | 1.5878 | 1.5667 (1.5649–1.5697) | 1.33% |
| 80th percentile | 12,849 / 3,367 | 1.3944 | 1.3776 (1.3770–1.3781) | 1.21% | 1.5789 | 1.5582 (1.5569–1.5593) | 1.31% |
| 90th percentile | 14,432 / 1,784 | 1.3790 | 1.3366 (1.3345–1.3387) | 3.08% | 1.5586 | 1.5228 (1.5206–1.5240) | 2.30% |

The forest had lower MAE and RMSE than the baseline in all nine runs. The seed-to-seed ranges were small in this experiment, while the error levels varied across cutoffs. These cutoffs reuse the same review collection and create overlapping training sets, so they are sensitivity checks rather than independent replications. The three seeds and one product category also limit what can be inferred. The results do not establish statistical significance or predict performance on other categories.

### Paired error intervals

For each run, the model-minus-baseline error difference was bootstrapped over paired test reviews using 1,000 resamples and seed 42. Negative values favor the random forest. Each interval is conditional on one cutoff and model seed.

| Cutoff | Model seed | MAE difference (95% interval) | RMSE difference (95% interval) |
| ---: | ---: | ---: | ---: |
| 70th percentile | 42 | -0.0266 [-0.0333, -0.0195] | -0.0182 [-0.0254, -0.0109] |
| 70th percentile | 7 | -0.0284 [-0.0355, -0.0211] | -0.0223 [-0.0296, -0.0149] |
| 70th percentile | 17 | -0.0295 [-0.0359, -0.0224] | -0.0229 [-0.0297, -0.0158] |
| 80th percentile | 42 | -0.0163 [-0.0247, -0.0074] | -0.0220 [-0.0306, -0.0131] |
| 80th percentile | 7 | -0.0167 [-0.0254, -0.0080] | -0.0196 [-0.0287, -0.0103] |
| 80th percentile | 17 | -0.0174 [-0.0265, -0.0086] | -0.0203 [-0.0295, -0.0105] |
| 90th percentile | 42 | -0.0423 [-0.0552, -0.0292] | -0.0349 [-0.0482, -0.0206] |
| 90th percentile | 7 | -0.0446 [-0.0570, -0.0317] | -0.0379 [-0.0505, -0.0245] |
| 90th percentile | 17 | -0.0403 [-0.0527, -0.0277] | -0.0346 [-0.0466, -0.0216] |

All intervals are below zero, consistent with lower errors for the forest in these runs. The bootstrap resamples review rows, not users or products; repeated observations may be related, so the intervals do not capture that dependence. The cutoffs also share reviews and are not independent replications. Treat this as a sensitivity result for one category, not evidence that the model will perform similarly on other data.
