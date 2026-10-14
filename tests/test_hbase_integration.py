"""Live REST/API check; start the local Compose HBase service to run it."""

import csv
import os
from pathlib import Path
import uuid
from urllib.error import URLError
from urllib.request import urlopen

import pytest
from fastapi.testclient import TestClient

from ecommerce_analytics.hbase_recommendations import HBaseRecommendations
from ecommerce_analytics.recommendation_api import app, get_store

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[1]
BACKUP_REVIEWS = (
    ROOT.parent
    / "Distributed E-Commerce Recommendation Analytics System"
    / "data"
    / "catalog_reviews.csv"
)


def _real_user_id() -> str:
    configured = os.getenv("AMAZON_REVIEWS_PATH")
    paths = [Path(configured)] if configured else [
        ROOT / "data" / "catalog_reviews.csv",
        BACKUP_REVIEWS,
    ]
    path = next((item for item in paths if item.is_file()), None)
    if path is None:
        message = "Real Amazon reviews not found; set AMAZON_REVIEWS_PATH"
        if os.getenv("REQUIRE_INTEGRATION", "").lower() in {"1", "true", "yes"}:
            pytest.fail(message)
        pytest.skip(message)

    with path.open(newline="", encoding="utf-8") as stream:
        user_id = next(csv.DictReader(stream))["user_id"]
    if not user_id:
        pytest.fail(f"Real Amazon review has no user_id: {path}")
    return user_id


def test_live_hbase_missing_recommendation_path():
    base_url = os.getenv("HBASE_INTEGRATION_URL", "http://localhost:8080").rstrip("/")
    try:
        with urlopen(f"{base_url}/version/cluster", timeout=2) as response:
            assert response.status == 200
    except URLError as exc:
        message = f"local HBase REST is unavailable: {exc}"
        if os.getenv("REQUIRE_INTEGRATION", "").lower() in {"1", "true", "yes"}:
            pytest.fail(message)
        pytest.skip(message)

    table = f"probe_{uuid.uuid4().hex[:12]}"
    store = HBaseRecommendations(base_url, table=table)
    try:
        store.ensure_table()
        user_id = _real_user_id()
        assert store.get(user_id) is None

        app.dependency_overrides[get_store] = lambda: store
        try:
            response = TestClient(app).get(f"/recommendations/{user_id}")
        finally:
            app.dependency_overrides.clear()
    finally:
        store._request("DELETE", f"{table}/schema")
    assert response.status_code == 404
    assert response.json()["detail"] == "No recommendations for this user"
