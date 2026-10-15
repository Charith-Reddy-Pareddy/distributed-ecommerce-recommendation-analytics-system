from pathlib import Path

from fastapi.testclient import TestClient

from services.product_service.app.main import app


def test_rating_endpoint_reports_missing_real_data(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AMAZON_REVIEWS_PATH", str(tmp_path / "reviews.csv"))

    with TestClient(app) as client:
        response = client.get("/metrics/amazon-ratings")

    assert response.status_code == 503
    assert response.json() == {"detail": "Amazon review data is unavailable"}
