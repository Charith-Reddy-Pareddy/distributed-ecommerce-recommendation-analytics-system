from unittest.mock import MagicMock

from conftest import load_app_module

pg_optimizer = load_app_module("serving-optimizer", "pg_optimizer", "optsvc_app")


def test_guess_table_select():
    assert pg_optimizer._guess_table("SELECT * FROM users WHERE id = $1", "select") == "users"


def test_guess_table_insert():
    assert pg_optimizer._guess_table("INSERT INTO products (name) VALUES ($1)", "insert") == "products"


def test_guess_table_update():
    assert pg_optimizer._guess_table("UPDATE users SET country = $1", "update") == "users"


def test_guess_table_delete_uses_from():
    assert pg_optimizer._guess_table("DELETE FROM sessions WHERE id = $1", "delete") == "sessions"


def test_guess_table_none_for_other_statement_type():
    assert pg_optimizer._guess_table("BEGIN", "other") is None


def test_guess_table_strips_quotes():
    assert pg_optimizer._guess_table('SELECT * FROM "users"', "select") == "users"


def _mock_conn(rows):
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value.fetchall.return_value = rows
    return conn


def test_fetch_statement_deltas_first_seen_query_has_zero_delta():
    conn = _mock_conn([(1, "user_db", "SELECT 1", 5)])
    rows, new_snapshot = pg_optimizer._fetch_statement_deltas(conn, last_snapshot={})
    assert rows == [("user_db", "SELECT 1", 0)]
    assert new_snapshot == {1: 5}


def test_fetch_statement_deltas_computes_call_delta():
    conn = _mock_conn([(1, "user_db", "SELECT 1", 25)])
    rows, new_snapshot = pg_optimizer._fetch_statement_deltas(conn, last_snapshot={1: 20})
    assert rows == [("user_db", "SELECT 1", 5)]
    assert new_snapshot == {1: 25}


def test_fetch_statement_deltas_clamps_negative_delta_to_zero():
    # calls dropped below the last snapshot -- a counter reset (e.g.
    # server restart), not a real decrease in traffic.
    conn = _mock_conn([(1, "user_db", "SELECT 1", 3)])
    rows, new_snapshot = pg_optimizer._fetch_statement_deltas(conn, last_snapshot={1: 100})
    assert rows == [("user_db", "SELECT 1", 0)]
    assert new_snapshot == {1: 3}


def test_fetch_statement_deltas_handles_multiple_independent_rows():
    conn = _mock_conn(
        [
            (1, "user_db", "SELECT 1", 30),
            (2, "analytics_db", "SELECT 2", 10),
        ]
    )
    rows, new_snapshot = pg_optimizer._fetch_statement_deltas(
        conn, last_snapshot={1: 10, 2: 10}
    )
    assert ("user_db", "SELECT 1", 20) in rows
    assert ("analytics_db", "SELECT 2", 0) in rows
    assert new_snapshot == {1: 30, 2: 10}
