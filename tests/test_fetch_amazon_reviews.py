from scripts.fetch_amazon_reviews import report_cf_viability


def test_report_cf_viability_counts_rows_asins_and_users():
    rows = [
        ("u1", "A1", 5.0, 100),
        ("u1", "A2", 4.0, 200),
        ("u2", "A1", 3.0, 150),
        ("u3", "A3", 5.0, 300),
    ]
    stats = report_cf_viability(rows, catalog_asins={"A1", "A2", "A3", "A4"})
    assert stats["matched_rows"] == 4
    assert stats["matched_asins"] == 3
    assert stats["catalog_asins"] == 4
    assert stats["distinct_users"] == 3


def test_report_cf_viability_identifies_multi_item_users():
    # u1 reviewed two catalog items (real CF signal); u2 and u3 each
    # reviewed only one -- no shared-user pair exists for their items.
    rows = [
        ("u1", "A1", 5.0, 100),
        ("u1", "A2", 4.0, 200),
        ("u2", "A3", 3.0, 150),
        ("u3", "A3", 5.0, 300),
    ]
    stats = report_cf_viability(rows, catalog_asins={"A1", "A2", "A3"})
    assert stats["multi_item_users"] == 1
    assert stats["multi_item_overlap_distribution"] == {2: 1}


def test_report_cf_viability_zero_signal_when_no_overlap():
    rows = [
        ("u1", "A1", 5.0, 100),
        ("u2", "A2", 4.0, 200),
        ("u3", "A3", 3.0, 300),
    ]
    stats = report_cf_viability(rows, catalog_asins={"A1", "A2", "A3"})
    assert stats["multi_item_users"] == 0
    assert stats["multi_item_overlap_distribution"] == {}


def test_report_cf_viability_empty_rows():
    stats = report_cf_viability([], catalog_asins={"A1", "A2"})
    assert stats["matched_rows"] == 0
    assert stats["matched_asins"] == 0
    assert stats["distinct_users"] == 0
    assert stats["multi_item_users"] == 0
