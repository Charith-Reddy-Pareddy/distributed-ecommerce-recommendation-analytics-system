# Amazon review research report

Descriptive results computed from the real Amazon Reviews 2023 extract. The source CSV is local and is not included in this repository.

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
