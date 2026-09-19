# Claude helped find and fix the bug these tests cover.
from conftest import load_app_module

main = load_app_module("hdfs-sink", "main", "hdfssink_app")


def test_parse_message_returns_dict_for_valid_json():
    assert main._parse_message(b'{"user_id": 1, "product_id": 2}') == {"user_id": 1, "product_id": 2}


def test_parse_message_returns_none_for_malformed_json():
    # Regression test: this used to be inline `json.loads(msg.value())`
    # with no try/except, so a single malformed message would crash the
    # process before its batch's offsets were committed -- and because
    # this consumer group commits offsets explicitly rather than
    # replaying from scratch on restart (unlike recommendation-service),
    # a restart would just re-fetch and re-crash on the exact same
    # message forever. Must return None, not raise.
    assert main._parse_message(b"{not valid json") is None


def test_parse_message_returns_none_for_none_input():
    assert main._parse_message(None) is None


def test_event_day_uses_created_at_when_valid():
    assert main._event_day({"created_at": "2026-01-15T10:00:00+00:00"}) == "2026-01-15"


def test_event_day_falls_back_to_today_when_created_at_missing():
    import datetime

    today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
    assert main._event_day({}) == today


def test_event_day_falls_back_to_today_for_malformed_created_at():
    # Regression test: datetime.fromisoformat() used to be called
    # unguarded -- a malformed created_at would raise ValueError and
    # crash the whole batch flush (poisoning every event in that batch,
    # not just the one with the bad field), for the same unrecoverable-
    # crash-loop reason as the malformed-JSON case above.
    import datetime

    today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
    assert main._event_day({"created_at": "not-a-real-timestamp"}) == today
    assert main._event_day({"created_at": 12345}) == today
