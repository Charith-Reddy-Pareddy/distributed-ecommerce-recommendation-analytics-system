# Recommendation weighting and uncertainty findings

The evaluation uses the existing random split and fixes one scope for every model: 150 users, 150 candidate items, and 818,345 training interactions. Held-out relevance and candidates stay constant. The four training transforms are raw rating, binary positive feedback (`1` for ratings of 4–5 and `0` for 1–3), squared rating, and uniform confidence (`1` for every review). Item-CF and implicit ALS are measured at Precision@10, Recall@10, MAP@10, and NDCG@10.

Intervals use 5,000 paired user-level bootstrap resamples at 95% confidence, with raw ratings as the reference. The committed JSONL stores aggregate metrics, intervals, settings, data fingerprints, and runtimes; it does not store reviewer identifiers.

| Model / weighting | Precision@10 | Recall@10 | MAP@10 | NDCG@10 |
| --- | ---: | ---: | ---: | ---: |
| Item-CF, raw | 0.0333 | 0.2150 | 0.0946 | 0.1361 |
| Item-CF, binary | 0.0327 | 0.2178 | 0.0977 | 0.1380 |
| Item-CF, squared | 0.0347 | 0.2283 | 0.0963 | 0.1403 |
| Item-CF, uniform | 0.0353 | 0.2306 | 0.1043 | 0.1473 |
| ALS, raw | 0.0240 | 0.1856 | 0.0699 | 0.1024 |
| ALS, binary | 0.0213 | 0.1628 | 0.0624 | 0.0920 |
| ALS, squared | 0.0220 | 0.1711 | 0.0580 | 0.0898 |
| ALS, uniform | 0.0240 | 0.1689 | 0.0670 | 0.0978 |

Uniform weighting has the strongest item-CF point estimates. Relative to raw ratings, its paired MAP difference is +0.0096 (95% CI +0.0007 to +0.0206) and NDCG is +0.0112 (+0.0021 to +0.0219). Its Precision interval starts at zero (+0.0020; 0.0000 to +0.0047), as does Recall (+0.0156; 0.0000 to +0.0378). For ALS, raw ratings have the strongest Recall, MAP, and NDCG point estimates; raw and uniform tie on Precision. All ALS paired intervals include zero. The sample does not establish a clear ALS weighting advantage.

The matched hybrid result remains available in `results/matched_hybrids.jsonl`: CF+ALS at blend weight 0.50 improved Precision@10 by +0.0053 over item-CF (95% paired CI +0.0007 to +0.0100). CF+content at 0.25 gained +0.0033, with an interval crossing zero (−0.0020 to +0.0087).

These 150-user results are exploratory, use a random rather than chronological split, and do not account for split choice, hyperparameter selection, or multiple comparisons. Input hashes and seeds are recorded in the result files.
