"""Flask dashboard tying together product search, analytics, and ALS
recommendations into one page.

A thin proxy: the browser only talks to Flask, which forwards to
whichever backend service owns the data.
"""
import os

import requests
from flask import Flask, jsonify, render_template, request

PRODUCT_SERVICE_URL = os.getenv("PRODUCT_SERVICE_URL", "http://product-service:8000")
RECOMMENDATION_SERVICE_URL = os.getenv("RECOMMENDATION_SERVICE_URL", "http://recommendation-service:8000")
ANALYTICS_SERVICE_URL = os.getenv("ANALYTICS_SERVICE_URL", "http://analytics-service:8000")
SERVING_OPTIMIZER_URL = os.getenv("SERVING_OPTIMIZER_URL", "http://serving-optimizer:8000")

app = Flask(__name__)


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/health")
def health():
    return jsonify({"status": "ok", "service": "dashboard"})


@app.get("/api/search")
def search():
    params = {k: v for k, v in request.args.items() if v}
    try:
        resp = requests.get(f"{PRODUCT_SERVICE_URL}/products/search", params=params, timeout=5)
        resp.raise_for_status()
        return jsonify(resp.json())
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 502


@app.post("/api/products/batch")
def products_batch():
    product_ids = request.get_json(silent=True) or []
    try:
        resp = requests.post(f"{PRODUCT_SERVICE_URL}/products/batch", json=product_ids, timeout=5)
        resp.raise_for_status()
        return jsonify(resp.json())
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 502


@app.get("/api/top-products")
def top_products():
    limit = request.args.get("limit", "10")
    try:
        resp = requests.get(
            f"{ANALYTICS_SERVICE_URL}/analytics/top-products", params={"limit": limit}, timeout=5
        )
        resp.raise_for_status()
        return jsonify(resp.json())
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 502


@app.get("/api/summary")
def event_summary():
    try:
        resp = requests.get(f"{ANALYTICS_SERVICE_URL}/analytics/summary", timeout=5)
        resp.raise_for_status()
        return jsonify(resp.json())
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 502


@app.get("/api/tuning/decisions")
def tuning_decisions():
    limit = request.args.get("limit", "20")
    try:
        resp = requests.get(f"{SERVING_OPTIMIZER_URL}/tuning/decisions", params={"limit": limit}, timeout=5)
        resp.raise_for_status()
        return jsonify(resp.json())
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 502


@app.get("/api/tuning/status")
def tuning_status():
    try:
        resp = requests.get(f"{SERVING_OPTIMIZER_URL}/tuning/status", timeout=5)
        resp.raise_for_status()
        return jsonify(resp.json())
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 502


@app.get("/api/recommendations/<int:visitor_id>")
def recommendations(visitor_id: int):
    try:
        resp = requests.get(
            f"{RECOMMENDATION_SERVICE_URL}/recommendations/precomputed/{visitor_id}", timeout=5
        )
        if resp.status_code == 404:
            return jsonify({"visitor_id": visitor_id, "recommendations": []}), 404
        resp.raise_for_status()
        return jsonify(resp.json())
    except requests.RequestException as e:
        return jsonify({"error": str(e)}), 502


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
