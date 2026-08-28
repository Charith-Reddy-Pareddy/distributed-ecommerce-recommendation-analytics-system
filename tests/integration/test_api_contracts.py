"""Do the services actually honor their documented contracts under real
HTTP -- health checks, validation errors, and not-found handling?
"""
import httpx

from conftest import ANALYTICS_SERVICE, EVENT_SERVICE, PRODUCT_SERVICE, RECOMMENDATION_SERVICE, USER_SERVICE

HEALTHY_SERVICES = [
    (PRODUCT_SERVICE, "product-service"),
    (USER_SERVICE, "user-service"),
    (EVENT_SERVICE, "event-service"),
    (RECOMMENDATION_SERVICE, "recommendation-service"),
]


def test_all_services_report_healthy():
    for base_url, name in HEALTHY_SERVICES:
        resp = httpx.get(f"{base_url}/health", timeout=5.0)
        assert resp.status_code == 200, f"{name} /health returned {resp.status_code}"
        assert resp.json().get("status") == "ok", f"{name} /health body: {resp.json()}"


def test_event_service_rejects_invalid_event_type():
    resp = httpx.post(
        f"{EVENT_SERVICE}/events",
        json={"user_id": 1, "product_id": 1, "event_type": "not_a_real_event_type"},
        timeout=5.0,
    )
    assert resp.status_code == 422


def test_event_service_rejects_missing_fields():
    resp = httpx.post(f"{EVENT_SERVICE}/events", json={"user_id": 1}, timeout=5.0)
    assert resp.status_code == 422


def test_user_service_rejects_missing_required_fields():
    resp = httpx.post(f"{USER_SERVICE}/users", json={"country": "US"}, timeout=5.0)
    assert resp.status_code == 422


def test_product_service_404s_for_missing_product():
    resp = httpx.get(f"{PRODUCT_SERVICE}/products/999999999", timeout=5.0)
    assert resp.status_code == 404


def test_recommendation_service_precomputed_404s_for_unknown_user():
    resp = httpx.get(f"{RECOMMENDATION_SERVICE}/recommendations/precomputed/999999999999", timeout=5.0)
    assert resp.status_code == 404


def test_analytics_summary_returns_expected_shape():
    resp = httpx.get(f"{ANALYTICS_SERVICE}/analytics/summary", timeout=5.0)
    assert resp.status_code == 200
    body = resp.json()
    # One row per (day, event_type), not a single aggregate dict.
    assert isinstance(body, list)
    assert len(body) > 0
    for row in body:
        assert {"day", "event_type", "count"} <= row.keys()
