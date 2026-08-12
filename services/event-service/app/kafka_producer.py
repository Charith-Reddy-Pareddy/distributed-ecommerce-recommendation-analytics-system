"""Publishes ingested events onto the `events` Kafka topic, the
durable log every downstream consumer reads from independently.

event-service keeps no database of its own -- Kafka is its
persistence, so ingestion just validates and produces.
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
