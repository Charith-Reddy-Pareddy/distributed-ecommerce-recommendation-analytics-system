"""Extracts (table, column, operator) predicates from pg_stat_statements'
normalized query text.
"""
import re

import sqlparse
from sqlparse.sql import Where
from sqlparse.tokens import DML

_PREDICATE_RE = re.compile(
    r"(\w+)\.(\w+)\s*(=|!=|<>|<=|>=|<|>|LIKE|ILIKE|IN|BETWEEN|NOT\s+LIKE|NOT\s+IN)\s*\$\d+",
    re.IGNORECASE,
)

RANGE_OPERATORS = {"<", ">", "<=", ">=", "!=", "<>", "LIKE", "ILIKE", "BETWEEN", "IN", "NOT IN", "NOT LIKE"}


def statement_type(query: str) -> str:
    """Return the leading SQL statement type."""
    # Use the leading DML token so INSERT ... ON CONFLICT DO UPDATE
    # is still classified as INSERT.
    parsed = sqlparse.parse(query)
    if not parsed:
        return "other"
    stmt = parsed[0]
    first = stmt.token_first(skip_ws=True, skip_cm=True)
    if first is None or first.ttype is not DML:
        return "other"
    return first.value.lower()


def extract_predicates(query: str) -> list[tuple[str, str, str]]:
    """Return (table, column, operator) predicates from the WHERE clause."""
    if statement_type(query) != "select":
        return []

    parsed = sqlparse.parse(query)
    if not parsed:
        return []

    where_text = None
    for token in parsed[0].tokens:
        if isinstance(token, Where):
            where_text = str(token)
            break
    if not where_text:
        return []

    predicates = []
    for table, column, operator in _PREDICATE_RE.findall(where_text):
        normalized_op = " ".join(operator.upper().split())
        predicates.append((table, column, normalized_op))
    return predicates


def is_range_operator(operator: str) -> bool:
    return operator.upper() in RANGE_OPERATORS
