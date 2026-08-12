"""Autonomous Postgres index tuner.

Polls pg_stat_statements (one connection to optimizer_db sees every
target database's query stats via the dbid column -- verified directly:
the `postgres` user in this project's docker-compose.yml is a superuser),
snapshot-diffs call counts to get a per-cycle delta, extracts WHERE-clause
predicates from SELECT queries via sql_analyzer, and creates or drops
`auto_idx_*` indexes based on real usage -- reimplementing the heuristics
from github.com/nimit-pasricha/auto-indexing (call-count threshold,
cardinality guard, table-size guard, write-ratio volatility guard,
hash-vs-btree selection, staleness cleanup) against pg_stat_statements as
the input instead of tailed log lines.
"""
import time

import psycopg2

from . import config, decision_log, sql_analyzer


def _optimizer_conn():
    return psycopg2.connect(
        host=config.PG_HOST,
        port=config.PG_PORT,
        user=config.PG_USER,
        password=config.PG_PASSWORD,
        dbname=config.PG_OPTIMIZER_DB,
    )


def _target_conn(datname: str):
    conn = psycopg2.connect(
        host=config.PG_HOST,
        port=config.PG_PORT,
        user=config.PG_USER,
        password=config.PG_PASSWORD,
        dbname=datname,
    )
    conn.autocommit = True
    return conn


class ColumnState:
    __slots__ = ("recent_calls", "idle_cycles", "index_name", "index_type", "operators")

    def __init__(self):
        self.recent_calls = 0
        self.idle_cycles = 0
        self.index_name = None
        self.index_type = None
        self.operators: set[str] = set()


def _fetch_statement_deltas(conn, last_snapshot: dict) -> tuple[dict, dict]:
    """Returns (rows, updated_snapshot). Each row is
    (datname, query, delta_calls). Negative deltas (counter resets --
    server restart, someone else calling pg_stat_statements_reset(), or
    LRU eviction/recreation of an entry) are clamped to 0 for this cycle
    rather than corrupting the threshold logic.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.queryid, d.datname, s.query, s.calls
            FROM pg_stat_statements s
            JOIN pg_database d ON s.dbid = d.oid
            WHERE d.datname = ANY(%s)
            """,
            (config.PG_TARGET_DBS,),
        )
        current = cur.fetchall()

    rows = []
    new_snapshot = {}
    for queryid, datname, query, calls in current:
        previous = last_snapshot.get(queryid)
        delta = 0 if previous is None else calls - previous
        if delta < 0:
            delta = 0
        new_snapshot[queryid] = calls
        rows.append((datname, query, delta))
    return rows, new_snapshot


def _table_size_too_small(conn, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(f'ANALYZE "{table}"')
        cur.execute("SELECT relpages, reltuples FROM pg_class WHERE relname = %s", (table,))
        row = cur.fetchone()
    if not row:
        return True
    relpages, reltuples = row
    return reltuples < config.PG_MIN_TABLE_ROWS


def _cardinality_too_low(conn, table: str, column: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.n_distinct, c.reltuples
            FROM pg_stats s
            JOIN pg_class c ON c.relname = s.tablename
            WHERE s.tablename = %s AND s.attname = %s
            """,
            (table, column),
        )
        row = cur.fetchone()
    if not row or row[1] in (None, 0):
        return False  # no stats yet -- don't block on missing data
    n_distinct, reltuples = row
    ratio = abs(n_distinct) if n_distinct < 0 else (n_distinct / reltuples if reltuples else 0)
    return ratio < config.PG_MIN_DISTINCT_RATIO


def _index_exists(conn, index_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_indexes WHERE indexname = %s", (index_name,))
        return cur.fetchone() is not None


def _create_index(conn, datname: str, table: str, column: str, index_type: str) -> str:
    name = f"auto_idx_{index_type}_{table}_{column}"
    with conn.cursor() as cur:
        cur.execute(f'CREATE INDEX CONCURRENTLY "{name}" ON "{table}" USING {index_type} ("{column}")')
    decision_log.record(
        "postgres",
        "create_index",
        f"{datname}.{table}.{column}",
        f"{index_type} index created after sustained query volume",
        {"index_name": name, "index_type": index_type},
    )
    return name


def _drop_index(conn, datname: str, index_name: str, reason: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'DROP INDEX CONCURRENTLY IF EXISTS "{index_name}"')
    decision_log.record("postgres", "drop_index", f"{datname}.{index_name}", reason, {"index_name": index_name})


def run_cycle(last_snapshot: dict, states: dict[tuple, ColumnState]) -> dict:
    opt_conn = _optimizer_conn()
    try:
        rows, new_snapshot = _fetch_statement_deltas(opt_conn, last_snapshot)
    finally:
        opt_conn.close()

    table_reads: dict[tuple, int] = {}
    table_writes: dict[tuple, int] = {}
    touched: set[tuple] = set()

    for datname, query, delta in rows:
        stype = sql_analyzer.statement_type(query)
        table = _guess_table(query, stype)
        if table:
            if stype == "select":
                table_reads[(datname, table)] = table_reads.get((datname, table), 0) + delta
            elif stype in ("insert", "update", "delete"):
                table_writes[(datname, table)] = table_writes.get((datname, table), 0) + delta

        if stype != "select" or delta == 0:
            continue
        for table, column, operator in sql_analyzer.extract_predicates(query):
            key = (datname, table, column)
            touched.add(key)
            state = states.setdefault(key, ColumnState())
            state.recent_calls += delta
            state.idle_cycles = 0
            state.operators.add(operator)

    for key, state in states.items():
        if key not in touched:
            state.idle_cycles += 1

    for (datname, table, column), state in list(states.items()):
        target_conn = None
        try:
            if state.index_name is None and state.recent_calls >= config.PG_CREATE_THRESHOLD_CALLS:
                target_conn = _target_conn(datname)
                writes = table_writes.get((datname, table), 0)
                reads = table_reads.get((datname, table), 0)
                write_ratio = writes / reads if reads > 0 else 0.0

                if write_ratio > config.PG_WRITE_RATIO_THRESHOLD:
                    decision_log.record(
                        "postgres", "skip_high_writes", f"{datname}.{table}.{column}",
                        f"write/read ratio {write_ratio:.2f} exceeds threshold {config.PG_WRITE_RATIO_THRESHOLD}",
                    )
                elif _table_size_too_small(target_conn, table):
                    decision_log.record(
                        "postgres", "skip_table_too_small", f"{datname}.{table}.{column}",
                        f"fewer than {config.PG_MIN_TABLE_ROWS} rows",
                    )
                elif _cardinality_too_low(target_conn, table, column):
                    decision_log.record(
                        "postgres", "skip_low_cardinality", f"{datname}.{table}.{column}",
                        f"distinct-value ratio below {config.PG_MIN_DISTINCT_RATIO}",
                    )
                else:
                    index_type = "btree" if any(sql_analyzer.is_range_operator(op) for op in state.operators) else "hash"
                    name = f"auto_idx_{index_type}_{table}_{column}"
                    if not _index_exists(target_conn, name):
                        state.index_name = _create_index(target_conn, datname, table, column, index_type)
                        state.index_type = index_type

            elif state.index_name is not None and state.idle_cycles >= config.PG_STALE_CYCLES:
                target_conn = target_conn or _target_conn(datname)
                _drop_index(
                    target_conn, datname, state.index_name,
                    f"no matching query in the last {config.PG_STALE_CYCLES} poll cycles",
                )
                state.index_name = None
                state.index_type = None
                state.recent_calls = 0
        finally:
            if target_conn:
                target_conn.close()

    return new_snapshot


def _guess_table(query: str, stype: str) -> str | None:
    """Best-effort single-table name for the write-ratio guard's table-
    level aggregation. Every query in this codebase touches exactly one
    table (no joins), so `FROM <table>` / `INTO <table>` / `UPDATE <table>`
    covers the real query mix.
    """
    tokens = query.replace("\n", " ").split()
    markers = {"select": "FROM", "insert": "INTO", "update": "UPDATE", "delete": "FROM"}
    marker = markers.get(stype)
    if not marker:
        return None
    upper_tokens = [t.upper() for t in tokens]
    if marker not in upper_tokens:
        return None
    idx = upper_tokens.index(marker)
    if idx + 1 >= len(tokens):
        return None
    return tokens[idx + 1].strip('"').split(".")[0]


def get_status() -> dict:
    active_indexes = []
    for datname in config.PG_TARGET_DBS:
        conn = _target_conn(datname)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT indexname FROM pg_indexes WHERE indexname LIKE 'auto_idx_%'")
                active_indexes.extend(f"{datname}.{row[0]}" for row in cur.fetchall())
        finally:
            conn.close()
    return {"active_auto_indexes": active_indexes}


def run_forever() -> None:
    last_snapshot: dict = {}
    states: dict[tuple, ColumnState] = {}
    while True:
        try:
            last_snapshot = run_cycle(last_snapshot, states)
        except Exception as e:
            print(f"[postgres] cycle error: {e}", flush=True)
        time.sleep(config.POLL_INTERVAL_SECONDS)
