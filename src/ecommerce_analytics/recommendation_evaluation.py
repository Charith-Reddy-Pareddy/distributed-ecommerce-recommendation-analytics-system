"""Evaluate ALS recommendations against a popularity baseline on real reviews."""

import argparse
import os
import sys
from pathlib import Path
from typing import Any


def evaluate_reviews(
    reviews_path: str | Path,
    catalog_map: dict[str, int],
    top_k: int = 10,
    relevant_rating: float = 4.0,
    user_sample_percent: int = 100,
    max_iter: int = 10,
) -> dict[str, Any]:
    """Train and compare ALS and popularity using each user's latest review.

    A user's latest mapped review is held out. Ratings at or above
    ``relevant_rating`` count as relevant; users without earlier mapped reviews
    are excluded. Only aggregate metrics are returned.
    """
    if top_k < 1:
        raise ValueError("top_k must be positive")
    if not 1 <= user_sample_percent <= 100:
        raise ValueError("user_sample_percent must be between 1 and 100")
    if max_iter < 1:
        raise ValueError("max_iter must be positive")
    if not catalog_map or any(type(pid) is not int or pid <= 0 for pid in catalog_map.values()):
        raise ValueError("catalog_map must contain positive integer product IDs")
    if len(set(catalog_map.values())) != len(catalog_map):
        raise ValueError("catalog_map product IDs must be unique")

    from pyspark.ml.feature import StringIndexer
    from pyspark.ml.recommendation import ALS
    from pyspark.sql import SparkSession, Window
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.appName("amazon-als-evaluation")
        .master("local[*]")
        .config("spark.driver.memory", "6g")
        .config("spark.kryoserializer.buffer.max", "512m")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        schema = StructType(
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
            .schema(schema)
            .csv(str(reviews_path))
        )
        items = spark.createDataFrame(
            [(asin, pid) for asin, pid in catalog_map.items()],
            ["asin", "product_id"],
        )
        interactions = reviews.join(items, "asin", "inner").select(
            "user_id", "product_id", "rating", "timestamp"
        )
        if user_sample_percent < 100:
            interactions = interactions.filter(
                F.pmod(F.xxhash64("user_id"), F.lit(100)) < user_sample_percent
            )
        order = Window.partitionBy("user_id").orderBy(
            F.col("timestamp").desc(), F.col("product_id").asc(), F.col("rating").desc()
        )
        ranked = interactions.withColumn("row_num", F.row_number().over(order)).persist()
        mapped_rows = ranked.count()
        held = ranked.filter(F.col("row_num") == 1)
        targets = held.filter(F.col("rating") >= relevant_rating).select(
            "user_id", "product_id"
        )
        train = ranked.filter(F.col("row_num") > 1).select(
            "user_id", "product_id", "rating"
        )
        # Remove earlier duplicate user-item rows so a held-out item is unseen.
        train = train.join(targets, ["user_id", "product_id"], "left_anti")
        if mapped_rows == 0 or train.limit(1).count() == 0:
            raise ValueError("not enough mapped review rows for a temporal evaluation")
        target_count = targets.count()
        if target_count == 0:
            raise ValueError("no held-out reviews meet the relevant-rating threshold")

        indexer = StringIndexer(
            inputCol="user_id", outputCol="user_idx", handleInvalid="skip"
        ).fit(train.select("user_id").distinct())
        indexed_train = indexer.transform(train).withColumn(
            "user_idx", F.col("user_idx").cast("int")
        )
        indexed_targets = indexer.transform(targets).withColumn(
            "user_idx", F.col("user_idx").cast("int")
        )
        als = ALS(
            userCol="user_idx",
            itemCol="product_id",
            ratingCol="rating",
            implicitPrefs=True,
            rank=10,
            maxIter=max_iter,
            regParam=0.1,
            alpha=1.0,
            coldStartStrategy="drop",
            seed=42,
        ).fit(indexed_train)

        users = indexed_targets.select("user_idx").distinct()
        recommendations = als.recommendForUserSubset(users, top_k).select(
            "user_idx", F.posexplode("recommendations").alias("pos", "rec")
        ).select(
            "user_idx", "pos", F.col("rec.product_id").alias("product_id")
        )
        als_hits = recommendations.join(
            indexed_targets.select("user_idx", "product_id"),
            ["user_idx", "product_id"],
        )
        als_scored = als_hits.select(
            "user_idx", (1 / F.log2(F.col("pos") + 2)).alias("dcg")
        )

        popular_rows = (
            train.groupBy("product_id")
            .agg(F.count(F.lit(1)).alias("train_count"))
            .orderBy(F.col("train_count").desc(), F.col("product_id"))
            .limit(top_k)
            .collect()
        )
        popular = spark.createDataFrame(
            [(row.product_id, pos) for pos, row in enumerate(popular_rows)],
            ["product_id", "pos"],
        )
        popular_hits = indexed_targets.join(popular, "product_id", "inner")

        def metrics(hits: Any, score_col: str | None = None) -> dict[str, float]:
            hit_count = hits.select("user_id" if "user_id" in hits.columns else "user_idx").distinct().count()
            ndcg = (
                hits.select(F.sum(score_col).alias("value")).first()["value"]
                if score_col
                else hits.select(
                    F.sum(1 / F.log2(F.col("pos") + 2)).alias("value")
                ).first()["value"]
            )
            return {
                f"recall_at_{top_k}": hit_count / target_count,
                f"ndcg_at_{top_k}": float(ndcg or 0.0) / target_count,
            }

        als_result = metrics(als_scored, "dcg")
        popular_result = metrics(popular_hits)
        als_coverage = recommendations.select("product_id").distinct().count() / len(catalog_map)
        popular_coverage = len(popular_rows) / len(catalog_map)

        return {
            "source_file": Path(reviews_path).name,
            "mapped_review_rows": mapped_rows,
            "eligible_users": target_count,
            "held_out_rule": "latest mapped review per user; rating >= threshold is relevant",
            "relevant_rating_threshold": relevant_rating,
            "user_sample_percent": user_sample_percent,
            "als_iterations": max_iter,
            "top_k": top_k,
            "catalog_products": len(catalog_map),
            "als": {**als_result, f"catalog_coverage_at_{top_k}": als_coverage},
            "popularity": {
                **popular_result,
                f"catalog_coverage_at_{top_k}": popular_coverage,
            },
        }
    finally:
        spark.stop()


def render_report(result: dict[str, Any]) -> str:
    k = result["top_k"]
    rows = [
        "# Recommendation quality evaluation",
        "",
        "Computed from the real Amazon Reviews 2023 extract and the project's "
        "real ASIN-to-product-ID map. This pilot uses a deterministic user sample "
        "of real source rows; it does not generate interaction data.",
        "",
        f"- Source file: `{result['source_file']}`",
        f"- Mapped review rows: {result['mapped_review_rows']:,}",
        f"- Eligible users with a relevant held-out review: {result['eligible_users']:,}",
        f"- Held-out rule: {result['held_out_rule']}",
        f"- Relevance threshold: rating >= {result['relevant_rating_threshold']}",
        f"- User sample: {result['user_sample_percent']}% selected deterministically by user ID hash; all source rows for selected users are retained",
        f"- ALS iterations: {result['als_iterations']}",
        f"- Catalog size used for coverage: {result['catalog_products']:,} mapped products",
        "",
        f"## Results at K={k}",
        "",
        "| Method | Recall@K | NDCG@K | Catalog coverage@K |",
        "| --- | ---: | ---: | ---: |",
    ]
    for name in ("als", "popularity"):
        values = result[name]
        rows.append(
            f"| {name.upper() if name == 'als' else 'Popularity'} | "
            f"{values[f'recall_at_{k}']:.4f} | "
            f"{values[f'ndcg_at_{k}']:.4f} | "
            f"{values[f'catalog_coverage_at_{k}']:.2%} |"
        )
    rows.extend(
        [
            "",
            "## Method and limits",
            "",
            "The latest mapped review for each user is held out. Earlier rows form "
            "training data; repeated rows for the held-out user-item pair are removed "
            "from training. Only held-out ratings meeting the threshold count as "
            "relevant. ALS uses the project's implicit-feedback settings and fixed "
            "seed. The popularity baseline ranks products by training-row count and "
            "uses the same global top-K list for each user without per-user seen-item "
            "filtering, so treat it as a simple exploratory baseline.",
            "",
            "This is an offline pilot on the disclosed user sample, not a full-catalog "
            "evaluation, online experiment, or evidence of sales impact. The ALS and "
            "popularity candidate filtering differ. The split and relevance threshold affect "
            "the result; compare future model changes using the same source and "
            "settings. Results do not generalize to RetailRocket or application events.",
            "",
        ]
    )
    return "\n".join(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reviews", type=Path, required=True)
    parser.add_argument("--catalog-map", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("docs/recommendation_evaluation.md"))
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--relevant-rating", type=float, default=4.0)
    parser.add_argument("--user-sample-percent", type=int, default=100)
    parser.add_argument("--max-iter", type=int, default=10)
    args = parser.parse_args()

    from ecommerce_analytics.als_export import load_catalog_map

    result = evaluate_reviews(
        args.reviews,
        load_catalog_map(args.catalog_map),
        top_k=args.top_k,
        relevant_rating=args.relevant_rating,
        user_sample_percent=args.user_sample_percent,
        max_iter=args.max_iter,
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_report(result), encoding="utf-8")


if __name__ == "__main__":
    main()
