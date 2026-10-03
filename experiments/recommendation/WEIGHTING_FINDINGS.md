# Recommendation weighting and uncertainty findings

## Evaluation design

The comparison reuses the existing random train/test split and holds the evaluation scope fixed at 150 users, 150 candidate items, and 818,345 training interactions. Held-out relevance and the candidate set do not change between weighting schemes. Four confidence transforms are applied to training review ratings: binary (`1`), linear (`rating`), squared (`rating²`), and exponential (`2^(rating−1)`). Both item-based collaborative filtering and implicit ALS are evaluated at Precision@10, Recall@10, MAP@10, and NDCG@10.

Intervals use 5,000 user-level bootstrap resamples at 95% confidence. Paired intervals compare each scheme with linear weighting on the same sampled users. The hybrid comparison uses the same bootstrap settings and pairs each model against item-CF. Raw user-level values and identifiers are not written to the result files; the committed JSONL contains aggregate metrics, intervals, settings, data fingerprints, and runtimes.

## Results

| Model / weighting | Precision@10 | Recall@10 | MAP@10 | NDCG@10 |
| --- | ---: | ---: | ---: | ---: |
| Item-CF, binary | 0.0353 | 0.2306 | 0.1043 | 0.1473 |
| Item-CF, linear | 0.0333 | 0.2150 | 0.0946 | 0.1361 |
| Item-CF, squared | 0.0347 | 0.2283 | 0.0963 | 0.1403 |
| Item-CF, exponential | 0.0353 | 0.2317 | 0.0907 | 0.1366 |
| ALS, binary | 0.0240 | 0.1689 | 0.0670 | 0.0978 |
| ALS, linear | 0.0240 | 0.1856 | 0.0699 | 0.1024 |
| ALS, squared | 0.0220 | 0.1711 | 0.0580 | 0.0898 |
| ALS, exponential | 0.0227 | 0.1706 | 0.0617 | 0.0930 |

For item-CF, binary weighting's paired MAP difference from linear is +0.0096 (95% CI +0.0007 to +0.0206) and its NDCG difference is +0.0112 (+0.0021 to +0.0219). The precision difference is +0.0020 (0.0000 to +0.0047); its interval touches zero. Recall improves by +0.0156 (0.0000 to +0.0378), also touching zero. The exponential scheme has the highest recall point estimate, while its MAP interval includes zero. These small-sample intervals are exploratory, especially across multiple schemes and metrics.

For ALS, all paired intervals against linear weighting include zero. Linear has the highest Recall, MAP, and NDCG point estimates; binary and linear tie in Precision. This evaluation does not show a clear advantage from reweighting ALS confidence.

The matched hybrid comparison found a Precision@10 gain of +0.0053 for CF+ALS at blend weight 0.50 over item-CF (95% paired CI +0.0007 to +0.0100). CF+content at 0.25 gained +0.0033, but its interval (−0.0020 to +0.0087) includes zero. Other metrics and blend weights remain available in `results/matched_hybrids.jsonl`.

## Limits

The fixed 150-user evaluation is a reproducible, matched-scope experiment rather than a population-wide claim. It uses the existing random split, not a chronological split. Bootstrap intervals quantify user sampling uncertainty within this held-out scope; they do not account for split choice, hyperparameter selection, or multiple comparisons. The recorded data hashes and seed identify the inputs and settings used for this run.
