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
"""
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

    # Two different users both interact with this product AND a second one,
    # so item-CF has co-occurrence signal to compute similarity from.
    for user_id in (950301, 950302):
        for pid in (product_id, second_product_id):
            httpx.post(
                f"{EVENT_SERVICE}/events",
                json={"user_id": user_id, "product_id": pid, "event_type": "purchase"},
                timeout=5.0,
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
