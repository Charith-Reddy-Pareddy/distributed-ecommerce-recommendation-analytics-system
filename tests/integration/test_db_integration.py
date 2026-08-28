"""Do writes through each service's API actually land in, and read back
correctly from, its real datastore -- Postgres for user-service, MongoDB
+ Elasticsearch for product-service?
"""
import time
import uuid

import httpx

from conftest import PRODUCT_SERVICE, USER_SERVICE


def test_user_roundtrips_through_postgres():
    marker = uuid.uuid4().hex[:10]
    payload = {"username": f"itest_{marker}", "email": f"itest_{marker}@example.com", "country": "US"}
    create = httpx.post(f"{USER_SERVICE}/users", json=payload, timeout=5.0)
    assert create.status_code in (200, 201)
    user_id = create.json()["id"]

    listed = httpx.get(f"{USER_SERVICE}/users", params={"country": "US"}, timeout=5.0)
    assert listed.status_code == 200
    ids = [u["id"] for u in listed.json()]
    assert user_id in ids


def test_product_roundtrips_through_mongo_and_es():
    marker = uuid.uuid4().hex[:10]
    payload = {"name": f"IntegrationTestProduct-{marker}", "category": "electronics", "price": 19.99, "description": "db integration test"}
    create = httpx.post(f"{PRODUCT_SERVICE}/products", json=payload, timeout=5.0)
    assert create.status_code in (200, 201)
    product_id = create.json()["id"]

    # Mongo (source of truth): fetched directly by id.
    fetched = httpx.get(f"{PRODUCT_SERVICE}/products/{product_id}", timeout=5.0)
    assert fetched.status_code == 200
    assert fetched.json()["name"] == payload["name"]

    # Elasticsearch: indexed on creation, should be searchable shortly after.
    def found_in_search():
        resp = httpx.get(f"{PRODUCT_SERVICE}/products/search", params={"q": marker}, timeout=5.0)
        return any(p["id"] == product_id for p in resp.json().get("results", []))

    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if found_in_search():
            return
        time.sleep(1)
    raise AssertionError(f"product {product_id} never became searchable in Elasticsearch")
