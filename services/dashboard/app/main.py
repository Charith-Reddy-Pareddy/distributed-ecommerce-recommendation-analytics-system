"""Flask dashboard: a single web interface tying together the three
serving-layer pieces built earlier -- Elasticsearch product search,
Cassandra-backed live demand data (populated by the Spark Structured
Streaming job), and HBase-backed ALS recommendations -- into one page.

This is a thin proxy layer, deliberately: the browser never talks to
product-service/analytics-service/recommendation-service directly (no
CORS setup needed, and it keeps service URLs as server-side config,
not exposed to the client), it talks to Flask, and Flask forwards to
whichever backend service actually owns that data.
"""
import os

import requests
from flask import Flask, jsonify, render_template, request

PRODUCT_SERVICE_URL = os.getenv("PRODUCT_SERVICE_URL", "http://product-service:8000")
RECOMMENDATION_SERVICE_URL = os.getenv("RECOMMENDATION_SERVICE_URL", "http://recommendation-service:8000")
ANALYTICS_SERVICE_URL = os.getenv("ANALYTICS_SERVICE_URL", "http://analytics-service:8000")

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


@app.get("/api/demand/<int:product_id>")
def demand(product_id: int):
    days = request.args.get("days", "1")
    try:
        resp = requests.get(
            f"{ANALYTICS_SERVICE_URL}/analytics/demand-timeseries/{product_id}",
            params={"days": days},
            timeout=5,
        )
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
