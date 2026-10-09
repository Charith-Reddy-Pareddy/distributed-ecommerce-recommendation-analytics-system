"""Train ALS from Amazon reviews and export catalog-ID recommendations."""

import argparse
import json
from pathlib import Path
from typing import Any


def _validate_catalog_map(data: Any) -> dict[str, int]:
    if not isinstance(data, dict) or not data:
        raise ValueError("catalog map must be a non-empty ASIN-to-product-ID object")
    if any(not isinstance(asin, str) or not asin for asin in data):
        raise ValueError("catalog map ASINs must be non-empty strings")
    if any(type(product_id) is not int or product_id <= 0 for product_id in data.values()):
        raise ValueError("catalog map product IDs must be positive integers")
    if len(set(data.values())) != len(data):
        raise ValueError("catalog map product IDs must be unique")
    return data


def load_catalog_map(path: str | Path) -> dict[str, int]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return _validate_catalog_map(data)


def export_recommendations(
    reviews_path: str | Path,
    catalog_map: dict[str, int],
    output_path: str | Path,
    top_k: int = 10,
) -> None:
    if top_k < 1:
        raise ValueError("top_k must be positive")
    catalog_map = _validate_catalog_map(catalog_map)

    from pyspark.ml.feature import IndexToString, StringIndexer
    from pyspark.ml.recommendation import ALS
    from pyspark.sql import SparkSession
    from pyspark.sql.types import (
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    spark = (
        SparkSession.builder.appName("amazon-review-als-export")
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.kryoserializer.buffer.max", "512m")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        review_schema = StructType(
            [
                StructField("user_id", StringType(), nullable=False),
                StructField("asin", StringType(), nullable=False),
                StructField("rating", DoubleType(), nullable=False),
                StructField("timestamp", LongType(), nullable=False),
            ]
        )
        reviews = (
            spark.read.option("header", "true")
            .option("enforceSchema", "false")
            .schema(review_schema)
            .csv(str(reviews_path))
        )
        items = spark.createDataFrame(
            [(asin, product_id) for asin, product_id in catalog_map.items()],
            ["asin", "product_id"],
        )
        interactions = reviews.join(items, on="asin", how="inner").select(
            "user_id", "product_id", "rating"
        )
        if interactions.limit(1).count() == 0:
            raise ValueError("no review rows matched the catalog map")

        user_indexer = StringIndexer(
            inputCol="user_id", outputCol="user_idx", handleInvalid="skip"
        ).fit(interactions.select("user_id").distinct())
        indexed = user_indexer.transform(interactions)
        model = ALS(
            userCol="user_idx",
            itemCol="product_id",
            ratingCol="rating",
            implicitPrefs=True,
            rank=10,
            maxIter=10,
            regParam=0.1,
            alpha=1.0,
            coldStartStrategy="drop",
            seed=42,
        ).fit(indexed)

        raw = model.recommendForAllUsers(top_k)
        named_users = IndexToString(
            inputCol="user_idx",
            outputCol="user_id",
            labels=user_indexer.labels,
        ).transform(raw)
        output = named_users.selectExpr(
            "user_id",
            "transform(recommendations, r -> "
            "named_struct('product_id', r.product_id, 'score', r.rating)) "
            "AS recommendations",
        )
        output.write.mode("overwrite").json(str(output_path))
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reviews", type=Path, required=True)
    parser.add_argument("--catalog-map", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--top-k", type=int, default=10)
    args = parser.parse_args()
    catalog_map = load_catalog_map(args.catalog_map)
    export_recommendations(args.reviews, catalog_map, args.output, args.top_k)


if __name__ == "__main__":
    main()
