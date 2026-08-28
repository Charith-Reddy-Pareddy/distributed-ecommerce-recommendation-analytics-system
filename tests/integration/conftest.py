"""Integration tests need the live stack (docker compose up). Excluded
from the default `pytest` run by pytest.ini's `--ignore=tests/integration`
-- run explicitly with `pytest tests/integration`.

Skips (not errors) with a clear message if the stack isn't reachable,
so running this by accident without Docker up fails gracefully.
"""
import httpx
import pytest

PRODUCT_SERVICE = "http://localhost:8001"
USER_SERVICE = "http://localhost:8002"
EVENT_SERVICE = "http://localhost:8003"
RECOMMENDATION_SERVICE = "http://localhost:8004"
ANALYTICS_SERVICE = "http://localhost:8005"


@pytest.fixture(scope="session", autouse=True)
def require_live_stack():
    try:
        resp = httpx.get(f"{EVENT_SERVICE}/health", timeout=3.0)
        resp.raise_for_status()
    except httpx.HTTPError:
        pytest.skip("Live stack not reachable at localhost -- run `docker compose up` first")
