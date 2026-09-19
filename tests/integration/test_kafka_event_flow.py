"""Does an event posted to event-service actually make it through Kafka
and show up in a downstream consumer's live state?

Uses a real, freshly-created catalog product (not a bare synthetic id)
weighted heavily enough to outrank the huge volume of synthetic-id
traffic this same stack has accumulated from earlier throughput and
fault-tolerance testing -- recommendation-service's popularity ranking
enriches from product-service *after* taking the top-N by weight, so a
synthetic id that was never a real product (as most load-test traffic
uses) gets silently dropped and can crowd out genuine low-volume
products from a small top-N response. That's real product-service/
recommendation-service behavior, not a test artifact to work around by
checking unenriched internal state.

This test is itself not idempotent: Kafka and product-service's store
both persist across `docker compose` restarts (named volumes, never
wiped), so every past run of this exact test left its own real,
permanently-weighted product behind. A fixed "200 purchases is
comfortably above everything else" margin quietly erodes every time
the test runs against the same long-lived stack -- past runs tie
each other at exactly the same weight, and popular_items()'s stable
sort breaks ties by insertion order, so a strict top-5 check
eventually starts losing to its own history. random_margin below
makes each run's weight distinct from every other run's (past or
future) with overwhelming probability, and top_n is generous enough
to tolerate the small number of same-weight products this stack has
already accumulated.
"""
import random
import time
import uuid

import httpx

from conftest import EVENT_SERVICE, PRODUCT_SERVICE, RECOMMENDATION_SERVICE


def _post_event(user_id, product_id, event_type):
    resp = httpx.post(
        f"{EVENT_SERVICE}/events",
        json={"user_id": user_id, "product_id": product_id, "event_type": event_type},
        timeout=5.0,
    )
    assert resp.status_code == 202
    return resp


def _create_real_product(marker):
    resp = httpx.post(
        f"{PRODUCT_SERVICE}/products",
        json={"name": f"KafkaFlowTestProduct-{marker}", "category": "electronics", "price": 9.99, "description": "kafka flow test"},
        timeout=5.0,
    )
    assert resp.status_code in (200, 201)
    return resp.json()["id"]


def _poll_until(check, deadline_s=30, interval_s=1.0):
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        if check():
            return True
        time.sleep(interval_s)
    return False


def test_event_reaches_popularity_ranking():
    product_id = _create_real_product(uuid.uuid4().hex[:10])
    # 400 purchases * weight 5 = 2000 as a base -- comfortably above the
    # highest weight observed among this stack's accumulated test
    # traffic (max 1000 as of this writing) -- plus a random 1-40 extra
    # purchases so this run's total weight doesn't exactly tie any past
    # or future run of this same test (see module docstring).
    random_margin = random.randint(1, 40)
    for _ in range(400 + random_margin):
        _post_event(user_id=1, product_id=product_id, event_type="purchase")

    def product_is_popular():
        resp = httpx.get(f"{RECOMMENDATION_SERVICE}/recommendations/popular", params={"n": 20}, timeout=5.0)
        ids = [p["id"] for p in resp.json()["popular_products"]]
        return product_id in ids

    assert _poll_until(product_is_popular), (
        f"product {product_id} never appeared in /recommendations/popular "
        "after being posted -- Kafka -> recommendation-service pipeline broken"
    )


def test_event_accepted_returns_202():
    resp = _post_event(user_id=1, product_id=950102, event_type="view")
    assert resp.status_code == 202
