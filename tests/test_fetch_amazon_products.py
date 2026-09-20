from scripts.fetch_amazon_products import product_from_row


def make_row(**overrides):
    row = {
        "title": "A Perfectly Fine Product Title",
        "price": "19.99",
        "images": {"large": ["https://example.com/img.jpg"]},
        "store": "Acme",
        "details": None,
        "description": ["A longer description of the product."],
        "average_rating": 4.5,
        "rating_number": 120,
        "parent_asin": "B000TESTASIN",
    }
    row.update(overrides)
    return row


def test_product_from_row_extracts_expected_fields():
    product = product_from_row(make_row(), "Musical_Instruments")
    assert product == {
        "name": "A Perfectly Fine Product Title",
        "category": "musical instruments",
        "price": 19.99,
        "description": "A longer description of the product.",
        "brand": "Acme",
        "average_rating": 4.5,
        "rating_number": 120,
        "image": "https://example.com/img.jpg",
        "asin": "B000TESTASIN",
    }


def test_product_from_row_rejects_short_title():
    assert product_from_row(make_row(title="abc"), "Electronics") is None


def test_product_from_row_rejects_missing_price():
    assert product_from_row(make_row(price="None"), "Electronics") is None
    assert product_from_row(make_row(price=None), "Electronics") is None


def test_product_from_row_rejects_price_above_max():
    assert product_from_row(make_row(price="5000.00"), "Electronics") is None


def test_product_from_row_rejects_no_images():
    assert product_from_row(make_row(images=None), "Electronics") is None
    assert product_from_row(make_row(images={"large": []}), "Electronics") is None


def test_product_from_row_falls_back_to_details_brand_when_no_store():
    import json

    row = make_row(store="", details=json.dumps({"Brand": "Fallback Brand"}))
    product = product_from_row(row, "Electronics")
    assert product["brand"] == "Fallback Brand"


def test_product_from_row_defaults_brand_to_unknown():
    row = make_row(store="", details=None)
    product = product_from_row(row, "Electronics")
    assert product["brand"] == "Unknown"


def test_product_from_row_falls_back_description_to_title_when_missing():
    row = make_row(description=[])
    product = product_from_row(row, "Electronics")
    assert product["description"] == row["title"]
