import pytest

from ecommerce_analytics.event_consumer import KafkaConsumeError, KafkaEventConsumer


class Message:
    def __init__(self, value: bytes | None, error: object | None = None) -> None:
        self._value = value
        self._error = error

    def value(self) -> bytes | None:
        return self._value

    def error(self) -> object | None:
        return self._error


class Consumer:
    def __init__(self, messages: list[Message], trace: list[str] | None = None) -> None:
        self.messages = iter(messages)
        self.committed: list[Message] = []
        self.trace = trace if trace is not None else []
        self.closed = False

    def poll(self, timeout: float) -> Message | None:
        return next(self.messages, None)

    def commit(self, *, message: Message, asynchronous: bool = False) -> None:
        self.trace.append("commit")
        self.committed.append(message)

    def close(self) -> None:
        self.closed = True


class Store:
    def __init__(self, trace: list[str] | None = None) -> None:
        self.records: list[object] = []
        self.trace = trace if trace is not None else []
        self.error: Exception | None = None

    def store(self, record: object) -> None:
        if self.error is not None:
            raise self.error
        self.trace.append("store")
        self.records.append(record)


def valid_payload() -> bytes:
    return (
        b'{"id":"00000000-0000-0000-0000-000000000001",'
        b'"user_id":1,"product_id":2,"event_type":"purchase",'
        b'"occurred_at":"2026-01-02T03:04:00+00:00"}'
    )


def test_consumer_stores_before_committing() -> None:
    trace: list[str] = []
    msg = Message(valid_payload())
    client = Consumer([msg], trace)
    store = Store(trace)

    assert KafkaEventConsumer(client, store).consume_one()

    assert len(store.records) == 1
    assert client.committed == [msg]
    assert trace == ["store", "commit"]


def test_consumer_returns_false_when_poll_times_out() -> None:
    client = Consumer([])
    store = Store()

    assert not KafkaEventConsumer(client, store).consume_one()
    assert store.records == []
    assert client.committed == []


@pytest.mark.parametrize(
    "msg",
    [
        Message(None, error="broker error"),
        Message(None),
        Message(b"not json"),
        Message(b'{"event_type":"unknown"}'),
    ],
)
def test_consumer_rejects_bad_messages_without_committing(msg: Message) -> None:
    client = Consumer([msg])
    store = Store()

    with pytest.raises(KafkaConsumeError):
        KafkaEventConsumer(client, store).consume_one()

    assert store.records == []
    assert client.committed == []


def test_consumer_does_not_commit_when_store_fails() -> None:
    msg = Message(valid_payload())
    client = Consumer([msg])
    store = Store()
    store.error = RuntimeError("Cassandra unavailable")

    with pytest.raises(RuntimeError, match="Cassandra unavailable"):
        KafkaEventConsumer(client, store).consume_one()

    assert client.committed == []


def test_consumer_closes_client_after_processing_error() -> None:
    client = Consumer([Message(None, error="broker error")])
    store = Store()

    with pytest.raises(KafkaConsumeError):
        KafkaEventConsumer(client, store).run()

    assert client.closed
