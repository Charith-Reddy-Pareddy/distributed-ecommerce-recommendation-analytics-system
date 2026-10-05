from fastapi.testclient import TestClient

from ecommerce_analytics.main import app


def test_health_endpoint_reports_service_ready() -> None:
    response = TestClient(app).get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
