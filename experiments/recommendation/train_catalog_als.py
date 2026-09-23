"""Trains Spark MLlib ALS on the real review interactions
(experiments/recommendation/interactions_train.parquet /
interactions_test.parquet, see build_interactions.py and
split_interactions.py), sharing the live catalog's own product ids --
unlike jobs/als-training/train_als.py's RetailRocket job, this ALS
model's item ids are the same ones product-service and
recommendation-service already use.

Local-mode PySpark, no Docker/HDFS cluster needed -- same evaluation
methodology as the RetailRocket job (precision_at_k via a left-anti
join + array_intersect, not a Python UDF, so the hit-count runs in the
JVM), same event-weighting precedent (real values used directly as
ALS confidence, not remapped -- see build_interactions.py's docstring
for why a star rating isn't a view/cart/purchase funnel stage).

Usage:

    python -m experiments.recommendation.train_catalog_als
"""
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import array_intersect, col, collect_set, row_number, size

from experiments.common import record_result

DATA_DIR = Path(__file__).resolve().parent
RESULTS_DIR = DATA_DIR / "results"
TOP_K = 10
RANK = 10
MAX_ITER = 10
REG_PARAM = 0.1
ALPHA = 1.0


def precision_at_k(model, train_df, test_df, k: int = TOP_K):
    test_users = test_df.select("user_id").distinct()

    raw_recs = model.recommendForUserSubset(test_users, k * 3)
    exploded = raw_recs.selectExpr("user_id", "explode(recommendations) as rec").select(
        "user_id", col("rec.product_id").alias("product_id"), col("rec.rating").alias("score")
    )

    train_pairs = train_df.select("user_id", "product_id")
    unseen_recs = exploded.join(train_pairs, on=["user_id", "product_id"], how="left_anti")

    ranked = unseen_recs.withColumn(
        "rank", row_number().over(Window.partitionBy("user_id").orderBy(col("score").desc()))
    )
    top_k_recs = ranked.filter(col("rank") <= k).groupBy("user_id").agg(
        collect_set("product_id").alias("recommended_items")
    )

    actual = test_df.groupBy("user_id").agg(collect_set("product_id").alias("actual_items"))
    joined = top_k_recs.join(actual, on="user_id", how="inner")

    scored = joined.withColumn(
        "hits", size(array_intersect(col("recommended_items"), col("actual_items")))
    )
    scored = scored.withColumn("precision", col("hits") / k)

    result = scored.agg({"precision": "avg"}).collect()[0][0]
    evaluated_users = scored.count()
    return result, evaluated_users


def main() -> None:
    spark = SparkSession.builder.appName("catalog-als-training").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    train_df = spark.read.parquet(str(DATA_DIR / "interactions_train.parquet"))
    test_df = spark.read.parquet(str(DATA_DIR / "interactions_test.parquet"))
    print(f"[ALS] Train rows: {train_df.count()}, Test rows: {test_df.count()}", flush=True)

    from pyspark.ml.recommendation import ALS

    als = ALS(
        userCol="user_id_idx",
        itemCol="product_id",
        ratingCol="weight",
        implicitPrefs=True,
        rank=RANK,
        maxIter=MAX_ITER,
        regParam=REG_PARAM,
        alpha=ALPHA,
        coldStartStrategy="drop",
        seed=42,
    )

    # ALS's userCol needs a numeric type -- user_id here is a real
    # Amazon reviewer id (string), unlike RetailRocket's already-numeric
    # visitorid, so it's indexed to an integer first. product_id is
    # already the live catalog's own integer id, used as-is.
    from pyspark.ml.feature import StringIndexer

    indexer = StringIndexer(inputCol="user_id", outputCol="user_id_idx")
    indexer_model = indexer.fit(train_df.select("user_id").union(test_df.select("user_id")))
    train_indexed = indexer_model.transform(train_df)
    test_indexed = indexer_model.transform(test_df)

    print("[ALS] Training...", flush=True)
    model = als.fit(train_indexed)
    print("[ALS] Training complete.", flush=True)

    precision, evaluated_users = precision_at_k(model, train_indexed, test_indexed, TOP_K)
    print(f"[ALS] Precision@{TOP_K} = {precision:.4f} (evaluated on {evaluated_users} users)", flush=True)

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    record_result(
        RESULTS_DIR,
        name="catalog_als",
        config={"rank": RANK, "maxIter": MAX_ITER, "regParam": REG_PARAM, "alpha": ALPHA, "k": TOP_K},
        dataset="real Amazon reviews (McAuley-Lab/Amazon-Reviews-2023, 5-core), joined to the live catalog, full interaction set",
        model="catalog_als",
        metric="precision",
        result={"precision": precision, "n_users": evaluated_users},
    )

    spark.stop()


if __name__ == "__main__":
    main()
