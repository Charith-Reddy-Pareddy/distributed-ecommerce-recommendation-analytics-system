"""Full pipeline: create products, generate interactions for them, and
confirm recommendation-service returns them enriched with real product
data (not just a bare id+score) -- exercises event-service, Kafka,
recommendation-service, and its product-service enrichment call
together, not any one of them in isolation.

Uses real, freshly-created catalog products throughout, not bare
synthetic ids -- recommendation-service enriches from product-service
*after* ranking, so a synthetic id (as most earlier load-test traffic
used) gets silently dropped and would make these tests flaky against
this stack's accumulated test history, not because anything is broken.

Two distinct pollution sources were found live-debugging this test
against a long-lived, never-reset stack, both from the same underlying
cause -- nothing here cleans up after itself, and Kafka/product-service
retain everything forever (no expiry, full-topic replay on restart):

1. product-service's own ids come from a real, permanently-incrementing
   counter (services/product-service/app/database.py) -- but before
   experiments/throughput/run.py and scripts/kafka_load_test.py were
   fixed to use a reserved high id range, they posted purely-synthetic
   events with product_id drawn from 1-10,000. A freshly
   product-service-minted id landing in that already-polluted low range
   inherits unrelated phantom interaction history from the moment it's
   created -- observed directly: a new product landed on id 722, and its
   cosine similarity to a genuinely co-purchased sibling was diluted
   enough by that unrelated history to fall out of the cached top-50
   neighbors entirely.

2. test_purchased_product_surfaces_as_similar_item used to hardcode its
   two "shopper" user ids (950301, 950302) on every run. Since cosine
   similarity is scale-invariant, every past run's product pair -- having
   been purchased by these exact same two users with the exact same
   weights -- ties at a *perfect* 1.0 similarity with every other past
   run's pair, not just with its own partner. With ten such old pairs
   already sitting in the top-10 window (stable-sort insertion order
   keeps the oldest ones first), a new run's pair could tie at 1.0 too
   and still never crack the top 10 -- no amount of extra purchase
   weight fixes a tie, since cosine doesn't care about magnitude, only
   direction. Fixed by giving each run its own random shopper ids, so a
   run's pair shares users with nothing but itself.

Both fixes compound: random user ids stop this test's own history from
tying with itself, and driving real per-pair purchase weight up guards
against the (separate, already-observed) case where a product id
happens to inherit unrelated dilution from other tests' history.
"""
import random
import time
import uuid

import httpx

from conftest import EVENT_SERVICE, PRODUCT_SERVICE, RECOMMENDATION_SERVICE


def _create_real_product(marker):
    resp = httpx.post(
        f"{PRODUCT_SERVICE}/products",
        json={"name": f"E2ETestProduct-{marker}", "category": "electronics", "price": 49.99, "description": "e2e test"},
        timeout=5.0,
    )
    assert resp.status_code in (200, 201)
    return resp.json()["id"]


def test_new_user_with_no_history_gets_enriched_popular_fallback():
    # Give a real product enough weight to reliably outrank this stack's
    # accumulated synthetic-id test traffic in a small top-N request.
    product_id = _create_real_product(uuid.uuid4().hex[:10])
    for _ in range(200):
        httpx.post(
            f"{EVENT_SERVICE}/events",
            json={"user_id": 1, "product_id": product_id, "event_type": "purchase"},
            timeout=5.0,
        )

    def has_enriched_fallback():
        resp = httpx.get(f"{RECOMMENDATION_SERVICE}/recommendations/999999999", params={"n": 5}, timeout=5.0)
        return resp.status_code == 200 and len(resp.json()["recommendations"]) > 0

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if has_enriched_fallback():
            break
        time.sleep(1)
    else:
        raise AssertionError("new user with no history never got an enriched popular fallback")

    resp = httpx.get(f"{RECOMMENDATION_SERVICE}/recommendations/999999999", params={"n": 5}, timeout=5.0)
    for item in resp.json()["recommendations"]:
        assert "name" in item and "price" in item and "score" in item


def test_purchased_product_surfaces_as_similar_item():
    product_id = _create_real_product(uuid.uuid4().hex[:10])
    second_product_id = _create_real_product(uuid.uuid4().hex[:10])

    # Random per-run shopper ids, not a hardcoded pair -- see module
    # docstring: fixed ids reused across every past run of this test tie
    # at a *perfect*, magnitude-independent cosine similarity with every
    # other past run's pair, since cosine only cares about direction.
    # Reserved high range so these never collide with product-service's
    # own real, permanently-incrementing catalog id counter either.
    shopper_ids = (random.randint(5_000_000, 6_000_000), random.randint(5_000_000, 6_000_000))

    # Two different users both interact with this product AND a second one,
    # so item-CF has co-occurrence signal to compute similarity from.
    # 30 purchases per (user, product) pair, not 1: a freshly-minted
    # product id can still carry unrelated weight from this stack's
    # pre-existing history (see module docstring's product 722 case),
    # which dilutes cosine similarity for any single-event signal --
    # cosine only counts *shared* users in its numerator but each item's
    # *entire* history in its denominator, so a weak, easily-diluted
    # signal needs to be replaced with an overwhelming one.
    with httpx.Client(timeout=5.0) as client:
        for user_id in shopper_ids:
            for pid in (product_id, second_product_id):
                for _ in range(30):
                    client.post(
                        f"{EVENT_SERVICE}/events",
                        json={"user_id": user_id, "product_id": pid, "event_type": "purchase"},
                    )

    def similar_now_includes_second_product():
        resp = httpx.get(f"{RECOMMENDATION_SERVICE}/recommendations/similar/{product_id}", params={"n": 10}, timeout=5.0)
        if resp.status_code != 200:
            return False
        ids = [p["id"] for p in resp.json()["similar_products"]]
        return second_product_id in ids

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if similar_now_includes_second_product():
            return
        time.sleep(1)
    raise AssertionError("co-purchased product never surfaced as a similar item")
