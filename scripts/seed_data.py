"""Seed the running services with sample products, users, and simulated
interaction events so the recommendation and analytics endpoints have
something to work with.

Usage (with all services already up via `docker compose up`):

    pip install requests
    python scripts/seed_data.py
"""
import random
import time

import requests

PRODUCT_SERVICE = "http://localhost:8001"
USER_SERVICE = "http://localhost:8002"
EVENT_SERVICE = "http://localhost:8003"

CATEGORIES = ["electronics", "books", "home", "sportswear", "toys"]

SAMPLE_PRODUCTS = [
    {
        "name": f"{category.title()} Item {i}",
        "category": category,
        "price": round(random.uniform(9.99, 299.99), 2),
        "description": f"A great {category} product",
    }
    for category in CATEGORIES
    for i in range(1, 5)
]

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
    for product in SAMPLE_PRODUCTS:
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
    print("Seeding products...")
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
