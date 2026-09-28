# Matched-scope hybrid comparison

The comparison in `compare_hybrids.py` holds the training interactions,
candidate catalog, sampled users, held-out relevance sets, and seen-item
exclusions fixed across popularity, production item-CF, TF-IDF content,
Spark implicit ALS, and the two score-level hybrids. Results are recorded
in `results/matched_hybrids.jsonl` with input SHA-256 fingerprints.

## Population and scoring

- Real Amazon review interactions from the existing random train/test split.
- Top 150 items by **training** frequency, with product-id tie breaking.
- All 818,345 training rows on those items; duplicate user/item weights
  aggregate identically for CF and ALS. No training-user subsampling.
- 150 users, sampled with seed 42 from sorted eligible ids. Each has at
  least five distinct training items and an unseen held-out item within
  the candidate catalog. Held-out items outside this catalog are excluded
  for every model. This conditions the result on a popular-item population.
- CF uses production scoring with the top-50 neighbor cache. Content uses
  category, brand, and description from a product-service snapshot with
  the same item ids as the interaction ETL.
- ALS uses rank 10, 10 iterations, regularization 0.1, confidence alpha 1,
  and seed 42. Every unseen candidate is scored directly; there is no
  top-30 truncation before seen-item filtering.
- Hybrids min-max normalize each component over each user's unseen scored
  candidates, then blend `(1-alpha)*CF + alpha*second_model`. Missing
  candidates contribute zero; a constant score vector gives equal mass
  to its observed candidates. Ties resolve by ascending product id.
- Alpha 0 and 1 reproduce the corresponding standalone model's ranked
  support. Users with empty predictions remain in all metric denominators.

## Recorded results

All values below use k=10 and the same 150 users. All models returned at
least one recommendation for every evaluated user in this run.

| Model | Precision | Recall | MAP | NDCG |
|---|---:|---:|---:|---:|
| Popularity | 0.02333 | 0.14722 | 0.06006 | 0.09093 |
| Item-CF | 0.03333 | 0.21500 | 0.09465 | 0.13614 |
| Content | 0.02067 | 0.15611 | 0.06455 | 0.09257 |
| ALS | 0.02400 | 0.18556 | 0.06985 | 0.10235 |
| CF+ALS, alpha=0.25 | 0.03400 | 0.22500 | 0.10325 | 0.14492 |
| CF+ALS, alpha=0.50 | 0.03867 | 0.26944 | 0.11025 | 0.15925 |
| CF+ALS, alpha=0.75 | 0.03067 | 0.22389 | 0.10015 | 0.13768 |
| CF+content, alpha=0.25 | 0.03667 | 0.24833 | 0.10074 | 0.14749 |
| CF+content, alpha=0.50 | 0.02867 | 0.20278 | 0.08584 | 0.12302 |
| CF+content, alpha=0.75 | 0.02200 | 0.16500 | 0.07514 | 0.10455 |

The fixed descriptive grid also records both endpoints. The largest
observed Precision@10 values occur at alpha 0.50 for CF+ALS and 0.25
for CF+content. This is not a validation-selected operating point or a
statistical significance claim. Bootstrap intervals, validation-based
weight selection, and temporal evaluation remain separate work.

The CF cache build took 33.60 seconds; ALS fitting plus candidate scoring
took 40.37 seconds. These are setup wall times, **not API serving latency**.
The earlier full-catalog ALS result (0.00449 on 225,873 users) and earlier
bounded CF result (0.0260) use different evaluation populations and must
not be compared directly with this table.

## Defects found during verification

1. CF's no-neighbor fallback called popularity without excluding the
   user's training items. A disjoint two-item regression reproduced the
   seen item in its recommendations. The fallback now excludes seen ids.
2. Catalog ALS evaluation inner-joined predictions to actuals. A user
   whose predictions were all removed as seen items disappeared from the
   denominator; if every user disappeared, precision became `None`.
   Actual users now drive a left join and empty predictions score zero.
   These fixes do not retroactively recompute older experiment records.

## Reproduction

Use the existing experiment environment (`experiments/requirements.txt`
plus the project's test/service dependencies), a compatible Java runtime,
and the real `interactions_train.parquet` / `interactions_test.parquet`
files. Supply a JSON array of product-service records captured from the
same catalog used by the interaction ETL. Do not infer ids from array
positions or use a newly reseeded catalog's mapping.

```bash
python -m experiments.recommendation.compare_hybrids \
  --data-dir /path/to/real/interactions \
  --catalog-snapshot /path/to/catalog-snapshot.json
```

The snapshot used for this run is retained locally at
`/tmp/ecommerce-catalog-snapshot.json`; it is not an immutable archive.
The result stores fingerprints of the exact train, test, and snapshot
files, along with the protocol and hyperparameters. Results contain
aggregate metrics only, with no reviewer identifiers. Input archival
and a durable mapping artifact are needed for long-term reproduction.
The script fails if candidate product metadata is missing instead of
silently dropping candidates from the content comparison.

## Verification

The baseline default suite passed 153 tests. Three added regression cases
failed before the fixes (seen-item fallback, omitted zero-hit users, and
an all-empty ALS result). After implementation, the complete default suite
passed 172 tests and the existing live integration suite passed all 13.
The local integration run used the rebuilt recommendation service and
existing datastore volumes; it did not reset or erase the catalog.
