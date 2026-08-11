"""Spark Structured Streaming job (the speed layer): two independent
queries over the live Kafka `events` topic.

1. Session reconstruction: groups each user's events via session_window
   (5-minute inactivity gap), writing finalized sessions to HDFS.
2. Per-product demand: 1-minute tumbling window counts, feeding both
   Z-score anomaly detection (Welford's online algorithm -- no need to
   hold full history) and a Cassandra time-series write (same
   aggregate, two consumers, not computed twice -- see cassandra_writer.py).

Both queries share the Kafka source but keep independent offsets and
state, which is normal for Structured Streaming.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, count, from_json, session_window, to_timestamp, window
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from cassandra_writer import write_demand_count

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
HDFS_URI = os.getenv("HDFS_URI", "hdfs://hdfs-namenode:9000")
Z_SCORE_THRESHOLD = float(os.getenv("Z_SCORE_THRESHOLD", "2.5"))
# Real session semantics: a 5-minute gap of inactivity ends a session, and
# HDFS output only appears once the watermark confirms a session is closed
# (won't receive any more late events). Both overridable for faster local
# verification -- e.g. SESSION_GAP=30 seconds, SESSION_WATERMARK=1 minute
# lets you see session output in under two minutes instead of waiting
# ~15 minutes for realistic durations to elapse.
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
        # The `events` topic gets deleted and recreated during this
        # project's own data-reset workflows (see README/serving-optimizer
        # notes on cleaning up stale demo data), which resets offsets to 0.
        # Without this, Spark's Kafka source treats that discontinuity as
        # fatal data loss and kills the whole query -- correct default for
        # a topic that shouldn't normally be recreated, wrong for this one.
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

    # Welford's online algorithm: running (count, mean, sum-of-squared-
    # deviations) per product, updated one micro-batch at a time. This
    # is driver-local state -- fine for a single-instance demo job, but
    # would need an external store (e.g. the Cassandra layer being added
    # next) to survive a driver restart in a real deployment.
    running_stats: dict[int, tuple[int, float, float]] = {}

    def process_batch(batch_df, batch_id):
        rows = batch_df.collect()
        print(
            f"[BATCH] anomaly-detection batch_id={batch_id} "
            f"window_rows={len(rows)} tracked_products={len(running_stats)}",
            flush=True,
        )
        for row in rows:
            product_id = row["product_id"]
            event_count = row["event_count"]
            window_start = row["window"]["start"]
            window_end = row["window"]["end"]

            write_demand_count(product_id, window_start, event_count)

            n, mean, m2 = running_stats.get(product_id, (0, 0.0, 0.0))

            if n >= 3:
                variance = m2 / n
                stddev = variance**0.5
                if stddev > 0:
                    z_score = (event_count - mean) / stddev
                    if abs(z_score) > Z_SCORE_THRESHOLD:
                        print(
                            f"[ANOMALY] batch={batch_id} product_id={product_id} "
                            f"window_end={window_end} count={event_count} "
                            f"running_mean={mean:.2f} running_stddev={stddev:.2f} "
                            f"z_score={z_score:.2f}",
                            flush=True,
                        )

            new_n = n + 1
            delta = event_count - mean
            new_mean = mean + delta / new_n
            new_m2 = m2 + delta * (event_count - new_mean)
            running_stats[product_id] = (new_n, new_mean, new_m2)

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
