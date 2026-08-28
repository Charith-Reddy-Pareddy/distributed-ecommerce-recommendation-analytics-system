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
"""
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
    # 200 purchases * weight 5 = 1000 -- comfortably above the highest
    # weight observed among this stack's accumulated test traffic.
    for _ in range(200):
        _post_event(user_id=1, product_id=product_id, event_type="purchase")

    def product_is_popular():
        resp = httpx.get(f"{RECOMMENDATION_SERVICE}/recommendations/popular", params={"n": 5}, timeout=5.0)
        ids = [p["id"] for p in resp.json()["popular_products"]]
        return product_id in ids

    assert _poll_until(product_is_popular), (
        f"product {product_id} never appeared in /recommendations/popular "
        "after being posted -- Kafka -> recommendation-service pipeline broken"
    )


def test_event_accepted_returns_202():
    resp = _post_event(user_id=1, product_id=950102, event_type="view")
    assert resp.status_code == 202
