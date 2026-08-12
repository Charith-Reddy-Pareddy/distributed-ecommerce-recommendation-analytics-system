from unittest.mock import MagicMock

from psycopg2 import sql

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


def _mock_cursor_conn():
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    return conn, cur


def _identifier_values(composed):
    """Raw strings wrapped in sql.Identifier within a Composed query --
    i.e. the parts psycopg2 will quote/escape at render time."""
    return [part.strings[0] for part in composed.seq if isinstance(part, sql.Identifier)]


def _literal_sql_text(composed):
    """The plain (non-identifier) SQL text, joined -- what would leak a
    payload if it were interpolated into the query instead of wrapped."""
    return "".join(part._wrapped for part in composed.seq if isinstance(part, sql.SQL))


def test_create_index_uses_sql_identifier_not_fstring(monkeypatch):
    # Regression guard: identifiers must go through psycopg2.sql,
    # not be interpolated into the query string directly.
    monkeypatch.setattr(pg_optimizer.decision_log, "record", lambda *a, **k: None)
    conn, cur = _mock_cursor_conn()
    malicious_table = 'users"; DROP TABLE users; --'
    pg_optimizer._create_index(conn, "user_db", malicious_table, "country", "hash")
    (query,), _ = cur.execute.call_args
    assert isinstance(query, sql.Composed)
    assert malicious_table in _identifier_values(query)
    assert "DROP TABLE" not in _literal_sql_text(query)


def test_drop_index_uses_sql_identifier_not_fstring(monkeypatch):
    monkeypatch.setattr(pg_optimizer.decision_log, "record", lambda *a, **k: None)
    conn, cur = _mock_cursor_conn()
    malicious_index = 'idx"; DROP TABLE users; --'
    pg_optimizer._drop_index(conn, "user_db", malicious_index, reason="test")
    (query,), _ = cur.execute.call_args
    assert isinstance(query, sql.Composed)
    assert malicious_index in _identifier_values(query)
    assert "DROP TABLE" not in _literal_sql_text(query)


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
