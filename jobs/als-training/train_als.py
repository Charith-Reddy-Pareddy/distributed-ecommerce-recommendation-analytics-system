"""Trains an implicit-feedback ALS model in Spark MLlib on the real
RetailRocket clickstream dataset, evaluates Precision@10 on a held-out
split, and writes the model plus precomputed top-10-per-user recs to HDFS.

The batch layer's recommender -- trained offline on full history, unlike
recommendation-service's always-current but Kafka-since-restart-only
in-memory engine.

RetailRocket events are implicit feedback, not ratings, so
implicitPrefs=True: event weight becomes ALS's confidence input, not a
predicted score. Same weighting as the rest of the project (view=1,
add_to_cart=3, purchase=5) for consistency.
"""
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, collect_set, count, row_number, size, sum as spark_sum, udf, when
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from pyspark.ml.recommendation import ALS

HDFS_URI = os.getenv("HDFS_URI", "hdfs://hdfs-namenode:9000")
EVENTS_PATH = f"{HDFS_URI}/datasets/retailrocket/events.csv"
MODEL_OUTPUT_PATH = f"{HDFS_URI}/models/als-recommender"
RECS_OUTPUT_PATH = f"{HDFS_URI}/output/als-recommendations"

EVENT_WEIGHTS = {"view": 1.0, "addtocart": 3.0, "transaction": 5.0}
TOP_K = 10
# RetailRocket is extremely sparse (median 1 interaction/user), so
# evaluating on users with too little history is mostly noise. Eval is
# restricted to users with enough interactions (k-core filtering);
# training still uses everyone.
MIN_INTERACTIONS_FOR_EVAL = 5

EVENTS_SCHEMA = StructType(
    [
        StructField("timestamp", LongType()),
        StructField("visitorid", LongType()),
        StructField("event", StringType()),
        StructField("itemid", LongType()),
        StructField("transactionid", StringType()),
    ]
)


def load_interactions(spark: SparkSession):
    raw = spark.read.csv(EVENTS_PATH, header=True, schema=EVENTS_SCHEMA)

    weighted = raw.withColumn(
        "weight",
        when(col("event") == "view", EVENT_WEIGHTS["view"])
        .when(col("event") == "addtocart", EVENT_WEIGHTS["addtocart"])
        .when(col("event") == "transaction", EVENT_WEIGHTS["transaction"])
        .otherwise(0.0),
    )

    # Aggregate to one row per (user, item): ALS wants a single confidence
    # value per pair, not one row per raw event.
    return weighted.groupBy("visitorid", "itemid").agg(spark_sum("weight").alias("weight"))


def precision_at_k(model, train_df, test_df, k: int = TOP_K):
    test_users = test_df.select("visitorid").distinct()

    # Ask for more than k -- ALS doesn't exclude already-seen items, so we
    # need room to drop those before taking the final top-k.
    raw_recs = model.recommendForUserSubset(test_users, k * 3)
    exploded = raw_recs.selectExpr("visitorid", "explode(recommendations) as rec").select(
        "visitorid", col("rec.itemid").alias("itemid"), col("rec.rating").alias("score")
    )

    train_pairs = train_df.select("visitorid", "itemid")
    unseen_recs = exploded.join(train_pairs, on=["visitorid", "itemid"], how="left_anti")

    ranked = unseen_recs.withColumn(
        "rank", row_number().over(Window.partitionBy("visitorid").orderBy(col("score").desc()))
    )
    top_k_recs = ranked.filter(col("rank") <= k).groupBy("visitorid").agg(
        collect_set("itemid").alias("recommended_items")
    )

    actual = test_df.groupBy("visitorid").agg(collect_set("itemid").alias("actual_items"))

    joined = top_k_recs.join(actual, on="visitorid", how="inner")

    def hit_count(recommended, actual_items):
        return len(set(recommended) & set(actual_items))

    hit_count_udf = udf(hit_count, LongType())
    scored = joined.withColumn("hits", hit_count_udf(col("recommended_items"), col("actual_items")))
    scored = scored.withColumn("precision", col("hits") / k)

    result = scored.agg({"precision": "avg"}).collect()[0][0]
    evaluated_users = scored.count()
    return result, evaluated_users


def main() -> None:
    spark = SparkSession.builder.appName("retailrocket-als-training").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    interactions = load_interactions(spark).cache()
    total_interactions = interactions.count()
    print(f"[ALS] Total distinct (user, item) interactions: {total_interactions}", flush=True)

    # Only users with enough history can have both a train and test example.
    user_counts = interactions.groupBy("visitorid").agg(count("*").alias("n"))
    eligible_users = user_counts.filter(col("n") >= MIN_INTERACTIONS_FOR_EVAL).select("visitorid")
    eligible_interactions = interactions.join(eligible_users, on="visitorid", how="inner")
    ineligible_interactions = interactions.join(eligible_users, on="visitorid", how="left_anti")

    train_eligible, test = eligible_interactions.randomSplit([0.8, 0.2], seed=42)
    # Users with too little history to evaluate still contribute to
    # training -- more data for ALS, just not part of the held-out set.
    train = train_eligible.unionByName(ineligible_interactions)

    train.cache()
    test.cache()
    print(f"[ALS] Train rows: {train.count()}, Test rows: {test.count()}", flush=True)

    als = ALS(
        userCol="visitorid",
        itemCol="itemid",
        ratingCol="weight",
        implicitPrefs=True,
        rank=10,
        maxIter=10,
        regParam=0.1,
        alpha=1.0,
        coldStartStrategy="drop",
        seed=42,
    )
    model = als.fit(train)
    print("[ALS] Model training complete.", flush=True)

    precision, evaluated_users = precision_at_k(model, train, test, TOP_K)
    print(
        f"[ALS] Precision@{TOP_K} = {precision:.4f} (evaluated on {evaluated_users} users "
        f"with >= {MIN_INTERACTIONS_FOR_EVAL} interactions)",
        flush=True,
    )

    model.write().overwrite().save(MODEL_OUTPUT_PATH)
    print(f"[ALS] Model saved to {MODEL_OUTPUT_PATH}", flush=True)

    # Precompute for active users only -- most users have 1 interaction and
    # no real signal, and running this for everyone took over an hour.
    active_users = eligible_users
    all_recs = model.recommendForUserSubset(active_users, TOP_K)
    output = all_recs.selectExpr("visitorid", "explode(recommendations) as rec").select(
        col("visitorid"), col("rec.itemid").alias("itemid"), col("rec.rating").alias("score")
    )
    output.write.mode("overwrite").json(RECS_OUTPUT_PATH)
    print(f"[ALS] Top-{TOP_K} recommendations for all users written to {RECS_OUTPUT_PATH}", flush=True)


if __name__ == "__main__":
    main()
