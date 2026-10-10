"""Live REST/API check; start the local Compose HBase service to run it."""

import os
import uuid
from urllib.error import URLError
from urllib.request import urlopen

import pytest
from fastapi.testclient import TestClient

from ecommerce_analytics.hbase_recommendations import HBaseRecommendations
from ecommerce_analytics.recommendation_api import app, get_store

pytestmark = pytest.mark.integration


def test_live_hbase_recommendation_round_trip():
    base_url = os.getenv("HBASE_INTEGRATION_URL", "http://localhost:8080").rstrip("/")
    try:
        with urlopen(f"{base_url}/version/cluster", timeout=2) as response:
            assert response.status == 200
    except URLError as exc:
        pytest.skip(f"local HBase REST is unavailable: {exc}")

    table = f"recs_{uuid.uuid4().hex[:12]}"
    store = HBaseRecommendations(base_url, table=table)
    store.ensure_table()
    try:
        expected = [{"product_id": 17, "score": 0.625}]
        store.write_batch([{"user_id": "integration-user", "recommendations": expected}])
        assert store.get("integration-user") == expected

        app.dependency_overrides[get_store] = lambda: store
        try:
            response = TestClient(app).get("/recommendations/integration-user")
        finally:
            app.dependency_overrides.clear()
        assert response.status_code == 200
        assert response.json() == {
            "user_id": "integration-user",
            "recommendations": expected,
        }
    finally:
        store._request("DELETE", f"{table}/schema")
