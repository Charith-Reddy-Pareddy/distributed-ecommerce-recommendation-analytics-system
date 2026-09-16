"""End-to-end proof of the completed live hybrid pipeline:

    catalog-native interactions -> Spark ALS -> HDFS -> hbase-loader
        -> HBase -> recommendation-service (live CF + precomputed ALS)
        -> hybrid response

This is the one gap docs/RESEARCH_REPORT.md's Limitations section used
to flag: the live hybrid endpoint could not previously obtain
catalog-native ALS recommendations (hbase-loader only ever loaded
RetailRocket's output), so it always fell back to pure CF.

Rather than just checking the hybrid endpoint responds (which it always
did, even via the fallback), this proves the *mechanism*: a live
interaction updates the CF component within seconds, while the
precomputed ALS component -- loaded once, offline, via
experiments/recommendation/catalog_als/ -> upload_to_hdfs.py ->
hbase-loader -- stays exactly static, unaffected by live traffic. Two
brand-new products (created fresh in this test, guaranteed absent from
the ALS training data) prove the CF side is really live, not cached.

Requires the ALS table to already be loaded via:
    docker compose --profile jobs run --rm \\
      -e RECS_PATH=/output/catalog-als-recommendations \\
      -e USER_ID_FIELD=user_id -e ITEM_ID_FIELD=product_id \\
      -e RECREATE_TABLE=true hbase-load-recommendations
"""
import time
import uuid

import httpx
import pytest

from conftest import EVENT_SERVICE, PRODUCT_SERVICE, RECOMMENDATION_SERVICE

# A user known to have a precomputed catalog-ALS row (any user in the
# ~1,866 eligible synthetic users the offline evaluation split produced).
ALS_COVERED_USER = 1500


def _create_real_product(marker):
    resp = httpx.post(
        f"{PRODUCT_SERVICE}/products",
        json={"name": f"HybridPipelineTest-{marker}", "category": "electronics", "price": 9.99, "description": "hybrid pipeline e2e test"},
        timeout=5.0,
    )
    assert resp.status_code in (200, 201)
    return resp.json()["id"]


def _fire_purchase(user_id, product_id):
    resp = httpx.post(
        f"{EVENT_SERVICE}/events",
        json={"user_id": user_id, "product_id": product_id, "event_type": "purchase"},
        timeout=5.0,
    )
    assert resp.status_code == 202


def test_hybrid_endpoint_genuinely_blends_live_cf_and_precomputed_als():
    precomputed = httpx.get(f"{RECOMMENDATION_SERVICE}/recommendations/precomputed/{ALS_COVERED_USER}", timeout=5.0)
    if precomputed.status_code == 404:
        pytest.skip(f"user {ALS_COVERED_USER} has no precomputed ALS row -- load the catalog-native ALS table first")
    als_before = [r["id"] for r in precomputed.json()["recommendations"]]
    assert als_before, "precomputed ALS row exists but has no enrichable items -- was RetailRocket-id data loaded instead of catalog-native?"

    hybrid = httpx.get(f"{RECOMMENDATION_SERVICE}/recommendations/hybrid/{ALS_COVERED_USER}", timeout=5.0)
    assert hybrid.status_code == 200
    assert hybrid.json()["source"] == "hybrid-cf-als", (
        "hybrid endpoint fell back to pure CF -- it has no precomputed ALS row for this user"
    )

    # Two brand-new products, guaranteed absent from the ALS training
    # snapshot -- if they show up anywhere live, it can only be via CF.
    marker = uuid.uuid4().hex[:8]
    product_a = _create_real_product(f"A-{marker}")
    product_b = _create_real_product(f"B-{marker}")

    # Fresh, random co-purchasing users each run -- reusing a fixed pair
    # across repeated runs accumulates a growing cluster of test products
    # all mutually tied at cosine similarity 1.0 (every prior run's pair
    # shares the same two users), which can knock this run's own pair out
    # of a small top-N window on a tie, even though CF genuinely did pick
    # it up. Real finding, not a pipeline bug -- see the analogous
    # popularity-dilution issue in test_kafka_event_flow.py.
    user_1, user_2 = uuid.uuid4().int % 900000 + 100000, uuid.uuid4().int % 900000 + 100000
    for user_id in (user_1, user_2):
        for product_id in (product_a, product_b):
            _fire_purchase(user_id, product_id)

    def cf_picked_up_new_products():
        resp = httpx.get(f"{RECOMMENDATION_SERVICE}/recommendations/similar/{product_a}", params={"n": 50}, timeout=5.0)
        if resp.status_code != 200:
            return False
        ids = [p["id"] for p in resp.json()["similar_products"]]
        return product_b in ids

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if cf_picked_up_new_products():
            break
        time.sleep(1)
    else:
        raise AssertionError("live CF never picked up the new co-purchase -- interaction -> Kafka -> CF update is broken")

    # The precomputed ALS component must be completely unaffected by
    # this live traffic -- it's a batch artifact, reloaded only by
    # rerunning the offline pipeline, not by anything happening now.
    precomputed_after = httpx.get(f"{RECOMMENDATION_SERVICE}/recommendations/precomputed/{ALS_COVERED_USER}", timeout=5.0)
    als_after = [r["id"] for r in precomputed_after.json()["recommendations"]]
    assert als_after == als_before, (
        "precomputed ALS recommendations changed after live traffic -- "
        "the batch/streaming freshness distinction (RQ3) no longer holds"
    )
