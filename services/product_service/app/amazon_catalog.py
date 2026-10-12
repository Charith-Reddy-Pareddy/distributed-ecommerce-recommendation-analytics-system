"""Map Amazon Reviews 2023 product rows into the product API schema."""

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

from ecommerce_analytics.als_export import load_catalog_map
from services.product_service.app.schemas import ProductCreate

DATA_PATH = Path(__file__).resolve().parents[3] / "data"
CATALOG_PATH = DATA_PATH / "amazon_products.json"
ID_MAP_PATH = DATA_PATH / "asin_product_id_map.json"


def product_from_amazon(row: dict[str, Any]) -> ProductCreate:
    """Convert one source product record without dropping its catalog fields."""
    asin = row.get("asin")
    if not isinstance(asin, str):
        raise ValueError("Amazon product must have a 10-character ASIN")
    asin = asin.strip().upper()
    if len(asin) != 10 or not asin.isalnum():
        raise ValueError("Amazon product must have a 10-character ASIN")

    image = row.get("image")
    payload = {
        "name": row.get("name"),
        "category": row.get("category"),
        "brand": row.get("brand"),
        "price": Decimal(str(row.get("price"))),
        "description": row.get("description") or "",
        "images": [image] if isinstance(image, str) and image else [],
        "rating": {
            "average": row.get("average_rating") or 0,
            "count": row.get("rating_number") or 0,
        },
        "asin": asin,
    }
    return ProductCreate.model_validate(payload)


def load_catalog(path: Path = CATALOG_PATH) -> list[ProductCreate]:
    """Read and validate the local product metadata extract."""
    rows = json.loads(path.read_text())
    if not isinstance(rows, list):
        raise ValueError("Amazon catalog must be a JSON array")

    products = []
    asins = set()
    for row in rows:
        product = product_from_amazon(row)
        if product.asin in asins:
            raise ValueError(f"Duplicate ASIN in Amazon catalog: {product.asin}")
        asins.add(product.asin)
        products.append(product)
    return products


def load_mapped_catalog(
    catalog_path: Path = CATALOG_PATH, id_map_path: Path = ID_MAP_PATH
) -> list[tuple[int, ProductCreate]]:
    """Pair each real catalog row with the stable ID used by ALS output."""
    products = load_catalog(catalog_path)
    id_map = load_catalog_map(id_map_path)
    missing = [product.asin for product in products if product.asin not in id_map]
    if missing:
        raise ValueError(
            f"{len(missing)} Amazon products are missing from the ASIN-to-ID map; "
            f"first missing ASIN: {missing[0]}"
        )
    return [(id_map[product.asin], product) for product in products]
