"""Publishes ingested events onto the `events` Kafka topic, which is the
durable log that every downstream consumer reads from independently:
recommendation-service and analytics-service tail it in real time, an
HDFS sink batches it into cold storage for the batch layer, and Spark
Structured Streaming consumes it for session reconstruction and
anomaly detection.

event-service itself keeps no database -- Kafka *is* its persistence.
This keeps the ingestion path on the hot path doing only two things:
validate, then produce.
"""
import json
import os

from confluent_kafka import Producer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = "events"

_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def publish_event(event: dict) -> None:
    _producer.produce(TOPIC, value=json.dumps(event).encode("utf-8"))
    _producer.poll(0)


def flush() -> None:
    _producer.flush()
