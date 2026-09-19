"""Speed layer: two Spark Structured Streaming queries over the Kafka
`events` topic -- session reconstruction and per-product demand
anomaly detection.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, count, from_json, session_window, to_timestamp, window
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from cassandra_writer import write_demand_count
from welford import WelfordAnomalyTracker

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
HDFS_URI = os.getenv("HDFS_URI", "hdfs://hdfs-namenode:9000")
Z_SCORE_THRESHOLD = float(os.getenv("Z_SCORE_THRESHOLD", "2.5"))
# A 5-minute inactivity gap ends a session; override both for faster
# local testing instead of waiting ~15 minutes for realistic durations.
SESSION_GAP = os.getenv("SESSION_GAP", "5 minutes")
SESSION_WATERMARK = os.getenv("SESSION_WATERMARK", "10 minutes")

EVENT_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("user_id", IntegerType()),
        StructField("product_id", IntegerType()),
        StructField("event_type", StringType()),
        StructField("created_at", StringType()),
    ]
)


def build_events_stream(spark: SparkSession):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", "events")
        .option("startingOffsets", "latest")
        # The topic sometimes gets deleted and recreated during resets,
        # which Spark's Kafka source otherwise treats as fatal data loss.
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_time", to_timestamp(col("created_at")))
    )


def run_session_reconstruction(events):
    sessions = (
        events.withWatermark("event_time", SESSION_WATERMARK)
        .groupBy(session_window(col("event_time"), SESSION_GAP), col("user_id"))
        .agg(
            count("*").alias("event_count"),
            collect_list("product_id").alias("products"),
        )
    )

    return (
        sessions.writeStream.outputMode("append")
        .format("json")
        .option("path", f"{HDFS_URI}/spark-output/sessions")
        .option("checkpointLocation", f"{HDFS_URI}/spark-checkpoints/sessions")
        .trigger(processingTime="30 seconds")
        .start()
    )


def run_anomaly_detection(events):
    counts = (
        events.withWatermark("event_time", "2 minutes")
        .groupBy(window(col("event_time"), "1 minute"), col("product_id"))
        .agg(count("*").alias("event_count"))
    )

    # See welford.py's module docstring for why this needs an explicit
    # idempotency guard that write_demand_count() below doesn't.
    tracker = WelfordAnomalyTracker(Z_SCORE_THRESHOLD)

    def process_batch(batch_df, batch_id):
        rows = batch_df.collect()
        print(
            f"[BATCH] anomaly-detection batch_id={batch_id} "
            f"window_rows={len(rows)} tracked_products={tracker.tracked_product_count()}",
            flush=True,
        )
        for row in rows:
            product_id = row["product_id"]
            event_count = row["event_count"]
            window_start = row["window"]["start"]
            window_end = row["window"]["end"]

            write_demand_count(product_id, window_start, event_count)

            stats = tracker.observe(product_id, window_start, event_count)
            if stats is None:
                continue
            if stats["is_anomaly"]:
                print(
                    f"[ANOMALY] batch={batch_id} product_id={product_id} "
                    f"window_end={window_end} count={event_count} "
                    f"running_mean={stats['mean']:.2f} running_stddev={stats['stddev']:.2f} "
                    f"z_score={stats['z_score']:.2f}",
                    flush=True,
                )

    return (
        counts.writeStream.outputMode("update")
        .foreachBatch(process_batch)
        .trigger(processingTime="30 seconds")
        .start()
    )


def main() -> None:
    spark = SparkSession.builder.appName("ecommerce-session-and-anomaly").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    events = build_events_stream(spark)

    run_session_reconstruction(events)
    run_anomaly_detection(events)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
