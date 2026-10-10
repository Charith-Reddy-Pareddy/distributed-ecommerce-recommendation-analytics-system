import logging
import os
from dataclasses import dataclass
from typing import Protocol

from ecommerce_analytics.cassandra_store import CassandraEventStore
from ecommerce_analytics.schemas import EventRecord


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ConsumerSettings:
    bootstrap_servers: str
    group_id: str
    topic: str
    cassandra_host: str
    cassandra_port: int
    cassandra_keyspace: str

    @classmethod
    def from_env(cls) -> "ConsumerSettings":
        def setting(name: str, default: str) -> str:
            value = os.environ.get(name, default).strip()
            if not value:
                raise ValueError(f"{name} must not be empty")
            return value

        port_text = setting("CASSANDRA_PORT", "9042")
        try:
            port = int(port_text)
        except ValueError as exc:
            raise ValueError("CASSANDRA_PORT must be an integer") from exc
        if not 1 <= port <= 65535:
            raise ValueError("CASSANDRA_PORT must be between 1 and 65535")

        return cls(
            bootstrap_servers=setting("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"),
            group_id=setting("KAFKA_CONSUMER_GROUP", "event-persistence"),
            topic=setting("KAFKA_EVENTS_TOPIC", "events"),
            cassandra_host=setting("CASSANDRA_HOST", "localhost"),
            cassandra_port=port,
            cassandra_keyspace=setting("CASSANDRA_KEYSPACE", "ecommerce_analytics"),
        )


class KafkaConsumeError(RuntimeError):
    pass


class KafkaMessage(Protocol):
    def error(self) -> object | None: ...

    def value(self) -> bytes | None: ...


class KafkaConsumerClient(Protocol):
    def poll(self, timeout: float) -> KafkaMessage | None: ...

    def commit(self, *, message: KafkaMessage, asynchronous: bool = False) -> object: ...

    def close(self) -> None: ...


class KafkaEventConsumer:
    def __init__(self, consumer: KafkaConsumerClient, store: CassandraEventStore) -> None:
        self._consumer = consumer
        self._store = store

    def consume_one(self, timeout: float = 1.0) -> bool:
        msg = self._consumer.poll(timeout)
        if msg is None:
            return False

        err = msg.error()
        if err is not None:
            raise KafkaConsumeError(f"Kafka returned an error: {err}")

        payload = msg.value()
        if payload is None:
            raise KafkaConsumeError("Kafka message has no value")
        try:
            rec = EventRecord.model_validate_json(payload)
        except (ValueError, TypeError) as exc:
            raise KafkaConsumeError("Kafka message is not a valid event record") from exc

        self._store.store(rec)
        self._consumer.commit(message=msg, asynchronous=False)
        logger.info("Processed event id=%s type=%s", rec.id, rec.event_type.value)
        return True

    def run(self) -> None:
        try:
            while True:
                self.consume_one()
        finally:
            self._consumer.close()


def run_event_consumer() -> None:
    settings = ConsumerSettings.from_env()

    from cassandra.cluster import Cluster
    from confluent_kafka import Consumer

    cluster = Cluster(
        [settings.cassandra_host],
        port=settings.cassandra_port,
    )
    session = cluster.connect()
    try:
        store = CassandraEventStore(session, settings.cassandra_keyspace)
        store.ensure_schema()
        consumer = Consumer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "group.id": settings.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        consumer.subscribe([settings.topic])
        KafkaEventConsumer(consumer, store).run()
    finally:
        session.shutdown()
        cluster.shutdown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_event_consumer()
