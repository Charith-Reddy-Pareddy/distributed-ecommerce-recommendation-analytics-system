"""Failure-mode tests for pg_optimizer.run_cycle: what happens when
pg_stat_statements resets, an index already exists, CREATE INDEX
CONCURRENTLY fails, a table disappears mid-cycle, or the optimizer
restarts and loses its in-memory `states`.

Each test drives the real run_cycle() orchestration logic, with only
the DB-touching functions replaced -- so the branching/decision code
under test is the actual production code, not a reimplementation.
"""
from unittest.mock import MagicMock

import psycopg2
import pytest

from conftest import load_app_module

pg_optimizer = load_app_module("serving-optimizer", "pg_optimizer", "optsvc_app")

QUERY = "SELECT * FROM users WHERE users.country = $1"
KEY = ("user_db", "users", "country")


@pytest.fixture(autouse=True)
def _patch_io(monkeypatch):
    monkeypatch.setattr(pg_optimizer, "_optimizer_conn", lambda: MagicMock())
    monkeypatch.setattr(pg_optimizer, "_target_conn", lambda datname: MagicMock())
    monkeypatch.setattr(pg_optimizer, "_table_size_too_small", lambda conn, table: False)
    monkeypatch.setattr(pg_optimizer, "_cardinality_too_low", lambda conn, table, column: False)
    monkeypatch.setattr(pg_optimizer.decision_log, "record", lambda *a, **k: None)


def _above_threshold_rows():
    return [("user_db", QUERY, pg_optimizer.config.PG_CREATE_THRESHOLD_CALLS)]


def test_stats_reset_produces_zero_delta_and_no_index(monkeypatch):
    # pg_stat_statements reset mid-run: _fetch_statement_deltas already
    # clamps this to a zero delta (tested directly in test_optimizer.py).
    # Confirmed here at the run_cycle level: a zero delta never
    # accumulates enough recent_calls to trigger index creation.
    monkeypatch.setattr(pg_optimizer, "_fetch_statement_deltas", lambda conn, snap: ([("user_db", QUERY, 0)], {}))
    create_mock = MagicMock()
    monkeypatch.setattr(pg_optimizer, "_create_index", create_mock)
    monkeypatch.setattr(pg_optimizer, "_index_exists", lambda conn, name: False)

    states = {}
    pg_optimizer.run_cycle({}, states)

    create_mock.assert_not_called()
    # A zero-delta row is skipped entirely (delta == 0), so the column
    # never even enters `states` -- no state to accumulate zero into.
    assert KEY not in states


def test_index_already_exists_is_adopted_not_recreated(monkeypatch):
    # A column crosses the create threshold, but an index with that
    # name already exists. This is the same code path whether that's
    # because of a duplicate detection race or -- the more likely real
    # case -- the optimizer restarted, wiping `states`, and this index
    # was created in a previous run: `states` starting empty is
    # indistinguishable from a fresh column at this point, since
    # nothing here depends on prior in-memory history. Either way,
    # run_cycle must not attempt to create it again, and must record
    # it in state so it's still eligible for stale-index cleanup later
    # instead of being orphaned from cleanup forever.
    monkeypatch.setattr(pg_optimizer, "_fetch_statement_deltas", lambda conn, snap: (_above_threshold_rows(), {}))
    monkeypatch.setattr(pg_optimizer, "_index_exists", lambda conn, name: True)
    create_mock = MagicMock()
    monkeypatch.setattr(pg_optimizer, "_create_index", create_mock)

    states = {}
    pg_optimizer.run_cycle({}, states)

    create_mock.assert_not_called()
    assert states[KEY].index_name == "auto_idx_hash_users_country"
    assert states[KEY].index_type == "hash"


def test_create_index_failure_is_isolated_to_that_column(monkeypatch):
    # CREATE INDEX CONCURRENTLY fails for one column (a real DDL error,
    # or the table disappearing between the size check and the create).
    # That must not stop other columns in the same cycle from being
    # evaluated, and must not crash run_cycle.
    other_query = "SELECT * FROM products WHERE products.category = $1"
    rows = [
        ("user_db", QUERY, pg_optimizer.config.PG_CREATE_THRESHOLD_CALLS),
        ("user_db", other_query, pg_optimizer.config.PG_CREATE_THRESHOLD_CALLS),
    ]
    monkeypatch.setattr(pg_optimizer, "_fetch_statement_deltas", lambda conn, snap: (rows, {}))
    monkeypatch.setattr(pg_optimizer, "_index_exists", lambda conn, name: False)

    def flaky_create(conn, datname, table, column, index_type):
        if table == "users":
            raise psycopg2.OperationalError("could not create index: relation does not exist")
        return f"auto_idx_{index_type}_{table}_{column}"

    monkeypatch.setattr(pg_optimizer, "_create_index", flaky_create)

    states = {}
    new_snapshot = pg_optimizer.run_cycle({}, states)  # must not raise

    assert new_snapshot == {}
    failed_key = ("user_db", "users", "country")
    ok_key = ("user_db", "products", "category")
    assert states[failed_key].index_name is None  # left to retry next cycle
    assert states[ok_key].index_name is not None  # unaffected by the other failure


def test_table_disappears_between_size_check_and_create(monkeypatch):
    # _table_size_too_small (which runs ANALYZE) succeeds, but the
    # table is gone by the time CREATE INDEX CONCURRENTLY runs --
    # a real race, not hypothetical, since these are two separate
    # round trips with no lock held between them.
    monkeypatch.setattr(pg_optimizer, "_fetch_statement_deltas", lambda conn, snap: (_above_threshold_rows(), {}))
    monkeypatch.setattr(pg_optimizer, "_index_exists", lambda conn, name: False)
    monkeypatch.setattr(
        pg_optimizer,
        "_create_index",
        MagicMock(side_effect=psycopg2.errors.UndefinedTable('relation "users" does not exist')),
    )

    states = {}
    pg_optimizer.run_cycle({}, states)  # must not raise

    assert states[KEY].index_name is None
    assert states[KEY].recent_calls == pg_optimizer.config.PG_CREATE_THRESHOLD_CALLS
    # Next cycle will see recent_calls already at/above threshold and
    # retry -- confirmed by calling run_cycle a second time.


def test_failed_create_retries_on_the_next_cycle(monkeypatch):
    monkeypatch.setattr(pg_optimizer, "_fetch_statement_deltas", lambda conn, snap: (_above_threshold_rows(), {}))
    monkeypatch.setattr(pg_optimizer, "_index_exists", lambda conn, name: False)

    attempts = []

    def create_fails_once(conn, datname, table, column, index_type):
        attempts.append(1)
        if len(attempts) == 1:
            raise psycopg2.OperationalError("transient failure")
        return f"auto_idx_{index_type}_{table}_{column}"

    monkeypatch.setattr(pg_optimizer, "_create_index", create_fails_once)

    states = {}
    pg_optimizer.run_cycle({}, states)
    assert states[KEY].index_name is None

    pg_optimizer.run_cycle({}, states)
    assert states[KEY].index_name == "auto_idx_hash_users_country"
    assert len(attempts) == 2
