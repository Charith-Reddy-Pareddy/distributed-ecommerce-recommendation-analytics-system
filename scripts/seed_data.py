"""Seeds the running services with real Amazon product data, sample
users, and simulated interaction events so the recommendation,
search, and analytics endpoints have something to work with.

Product data comes from data/amazon_products.json, 300 real Amazon
products extracted via scripts/fetch_amazon_products.py.

Usage (with all services already up via `docker compose up`):

    pip install requests
    python scripts/seed_data.py
"""
import json
import random
import time
from pathlib import Path

import requests

PRODUCT_SERVICE = "http://localhost:8001"
USER_SERVICE = "http://localhost:8002"
EVENT_SERVICE = "http://localhost:8003"

AMAZON_PRODUCTS_PATH = Path(__file__).resolve().parent.parent / "data" / "amazon_products.json"

# Real city coordinates, [lon, lat] -- the Amazon dataset has no
# warehouse geo data, so each product gets one of these assigned on seed.
WAREHOUSES = {
    "New York": (-74.006, 40.7128),
    "Los Angeles": (-118.2437, 34.0522),
    "Chicago": (-87.6298, 41.8781),
    "Austin": (-97.7431, 30.2672),
    "Seattle": (-122.3321, 47.6062),
}


def load_amazon_products() -> list[dict]:
    raw = json.loads(AMAZON_PRODUCTS_PATH.read_text())
    products = []
    for item in raw:
        products.append(
            {
                "name": item["name"],
                "category": item["category"],
                "price": item["price"],
                "description": item["description"],
                "tags": [item["category"], item["brand"]],
                "specifications": {"brand": item["brand"]},
                "images": [item["image"]],
                "location": {
                    "type": "Point",
                    "coordinates": list(random.choice(list(WAREHOUSES.values()))),
                },
                "rating": {"average": item["average_rating"], "count": item["rating_number"]},
                "asin": item["asin"],
            }
        )
    return products


SAMPLE_USERS = [
    {
        "username": f"user{i}",
        "email": f"user{i}@example.com",
        "country": random.choice(["US", "UK", "IN", "DE"]),
    }
    for i in range(1, 21)
]


def seed_products() -> list[int]:
    ids = []
    for product in load_amazon_products():
        resp = requests.post(f"{PRODUCT_SERVICE}/products", json=product)
        resp.raise_for_status()
        ids.append(resp.json()["id"])
    return ids


def seed_users() -> list[int]:
    ids = []
    for user in SAMPLE_USERS:
        resp = requests.post(f"{USER_SERVICE}/users", json=user)
        resp.raise_for_status()
        ids.append(resp.json()["id"])
    return ids


def simulate_events(user_ids: list[int], product_ids: list[int], count: int = 300) -> None:
    event_types = ["view"] * 6 + ["add_to_cart"] * 3 + ["purchase"] * 1
    for _ in range(count):
        payload = {
            "user_id": random.choice(user_ids),
            "product_id": random.choice(product_ids),
            "event_type": random.choice(event_types),
        }
        resp = requests.post(f"{EVENT_SERVICE}/events", json=payload)
        resp.raise_for_status()
        time.sleep(0.01)


if __name__ == "__main__":
    print("Seeding real Amazon product data...")
    product_ids = seed_products()
    print(f"Created {len(product_ids)} products")

    print("Seeding users...")
    user_ids = seed_users()
    print(f"Created {len(user_ids)} users")

    print("Simulating interaction events...")
    simulate_events(user_ids, product_ids)

    print("\nDone! Try:")
    print(f"  curl http://localhost:8004/recommendations/{user_ids[0]}")
    print("  curl http://localhost:8005/analytics/top-products")
    print(f'  curl "http://localhost:8001/products/search?q=headphones"')
