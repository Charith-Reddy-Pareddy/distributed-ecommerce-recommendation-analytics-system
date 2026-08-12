from conftest import load_app_module

sql_analyzer = load_app_module("serving-optimizer", "sql_analyzer", "optsvc_app")


def test_statement_type_select():
    assert sql_analyzer.statement_type("SELECT * FROM users WHERE id = $1") == "select"


def test_statement_type_plain_insert():
    query = "INSERT INTO product_stats (product_id, views) VALUES ($1, $2)"
    assert sql_analyzer.statement_type(query) == "insert"


def test_statement_type_upsert_is_insert_not_update():
    # The real bug this leading-keyword approach avoids: a naive
    # substring search for "UPDATE" would misclassify this as an
    # update, double-counting it against both insert and update totals.
    query = (
        "INSERT INTO product_stats (product_id, views) VALUES ($1, $2) "
        "ON CONFLICT (product_id) DO UPDATE SET views = product_stats.views + $2"
    )
    assert sql_analyzer.statement_type(query) == "insert"


def test_statement_type_delete():
    assert sql_analyzer.statement_type("DELETE FROM sessions WHERE user_id = $1") == "delete"


def test_statement_type_other_for_garbage():
    assert sql_analyzer.statement_type("not sql at all") == "other"


def test_statement_type_other_for_empty_string():
    assert sql_analyzer.statement_type("") == "other"


def test_extract_predicates_simple_equality():
    query = "SELECT * FROM users WHERE users.country = $1"
    predicates = sql_analyzer.extract_predicates(query)
    assert predicates == [("users", "country", "=")]


def test_extract_predicates_normalizes_operator_case_and_spacing():
    query = "SELECT * FROM products WHERE products.price not   like $1"
    predicates = sql_analyzer.extract_predicates(query)
    assert predicates == [("products", "price", "NOT LIKE")]


def test_extract_predicates_multiple_columns():
    query = "SELECT * FROM users WHERE users.country = $1 AND users.age > $2"
    predicates = sql_analyzer.extract_predicates(query)
    assert ("users", "country", "=") in predicates
    assert ("users", "age", ">") in predicates


def test_extract_predicates_empty_for_non_select():
    query = "UPDATE users SET country = $1 WHERE users.id = $2"
    assert sql_analyzer.extract_predicates(query) == []


def test_extract_predicates_empty_when_no_where_clause():
    assert sql_analyzer.extract_predicates("SELECT * FROM users") == []


def test_is_range_operator_true_for_range_operators():
    assert sql_analyzer.is_range_operator(">") is True
    assert sql_analyzer.is_range_operator("like") is True
    assert sql_analyzer.is_range_operator("BETWEEN") is True


def test_is_range_operator_false_for_equality():
    assert sql_analyzer.is_range_operator("=") is False
