"""Autonomous Elasticsearch refresh_interval tuner.

Polls ES's own /_stats API for indexing rate -- same reasoning as the
Postgres tuner polling pg_stat_statements directly, no separate pipeline
needed.

Load-bearing caveat: create_product is the only thing that writes into
the products index, and README.md already claims a new product is
searchable within ~1s. Raising refresh_interval on any indexing activity
would break that. So the burst threshold sits well above normal
single-product creation -- only a real bulk load (e.g. seed_data.py)
crosses it, trading a few seconds of latency for less refresh overhead.
"""
import time

from elasticsearch import Elasticsearch

from . import config, decision_log

_client = None
_state = {"last_total": None, "last_delta": 0, "mode": "normal"}


def _get_client() -> Elasticsearch:
    global _client
    if _client is None:
        _client = Elasticsearch(config.ELASTICSEARCH_URL)
    return _client


def _index_total_ops(client: Elasticsearch) -> int | None:
    try:
        stats = client.indices.stats(index=config.ES_INDEX, metric="indexing")
    except Exception as e:
        print(f"[elasticsearch] stats fetch failed: {e}", flush=True)
        return None
    return stats["indices"][config.ES_INDEX]["total"]["indexing"]["index_total"]


def run_cycle() -> None:
    client = _get_client()
    total = _index_total_ops(client)
    if total is None:
        return

    delta = 0 if _state["last_total"] is None else max(0, total - _state["last_total"])
    _state["last_total"] = total
    _state["last_delta"] = delta

    is_burst = delta >= config.ES_BURST_OPS_THRESHOLD

    if is_burst and _state["mode"] != "burst":
        client.indices.put_settings(
            index=config.ES_INDEX, settings={"refresh_interval": config.ES_BURST_REFRESH_INTERVAL}
        )
        _state["mode"] = "burst"
        decision_log.record(
            "elasticsearch", "raise_refresh_interval", f"{config.ES_INDEX} index",
            f"{delta} index ops in last {config.POLL_INTERVAL_SECONDS}s (>= {config.ES_BURST_OPS_THRESHOLD}) -- bulk load detected",
            {"refresh_interval": config.ES_BURST_REFRESH_INTERVAL, "recent_ops": delta},
        )
    elif not is_burst and _state["mode"] != "normal":
        client.indices.put_settings(
            index=config.ES_INDEX, settings={"refresh_interval": config.ES_NORMAL_REFRESH_INTERVAL}
        )
        _state["mode"] = "normal"
        decision_log.record(
            "elasticsearch", "reset_refresh_interval", f"{config.ES_INDEX} index",
            f"indexing activity dropped to {delta} ops in last {config.POLL_INTERVAL_SECONDS}s -- reverting to near-real-time",
            {"refresh_interval": config.ES_NORMAL_REFRESH_INTERVAL, "recent_ops": delta},
        )


def get_status() -> dict:
    return {
        "mode": _state["mode"],
        "cumulative_index_ops": _state["last_total"],
        "last_cycle_delta": _state["last_delta"],
    }


def run_forever() -> None:
    while True:
        try:
            run_cycle()
        except Exception as e:
            print(f"[elasticsearch] cycle error: {e}", flush=True)
        time.sleep(config.POLL_INTERVAL_SECONDS)
