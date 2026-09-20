"""Tests for scripts/generate_interactions.py -- the sole generator behind
the synthetic interaction log every recommendation experiment in
docs/RESEARCH_REPORT.md is built on. Runs against the real, checked-in
catalog (data/amazon_products.json), not a mock, since it's small and
this is exactly the production code path, not a reimplementation of it.
"""
import importlib.util
import sys
from pathlib import Path

import numpy as np

# generate_interactions.py lives under scripts/, and imports numpy/pandas
# only -- safe to load directly the same way load_app_module does for
# services, without needing its services/<name>/app/ layout assumption.

REPO_ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location(
    "generate_interactions", REPO_ROOT / "scripts" / "generate_interactions.py"
)
gen = importlib.util.module_from_spec(spec)
sys.modules["generate_interactions"] = gen
spec.loader.exec_module(gen)


def test_zipf_weights_sum_to_one():
    for n in (1, 2, 5, 50):
        assert np.isclose(gen.zipf_weights(n).sum(), 1.0)


def test_zipf_weights_are_monotonically_decreasing():
    weights = gen.zipf_weights(10)
    assert all(weights[i] > weights[i + 1] for i in range(len(weights) - 1))


def test_zipf_weights_single_item_is_certain():
    assert gen.zipf_weights(1).tolist() == [1.0]


def test_generate_events_has_expected_columns():
    df = gen.generate_events(n_users=10, weeks=2, seed=0)
    assert list(df.columns) == ["user_id", "product_id", "event_type", "timestamp", "week"]


def test_generate_events_covers_every_requested_user():
    # n_sessions = poisson(...) + 1 guarantees at least one session, and
    # each session draws at least one item -- every user should appear.
    n_users = 15
    df = gen.generate_events(n_users=n_users, weeks=2, seed=1)
    assert df["user_id"].nunique() == n_users
    assert set(df["user_id"]) == set(range(1, n_users + 1))


def test_generate_events_week_numbers_stay_in_requested_range():
    weeks = 4
    df = gen.generate_events(n_users=30, weeks=weeks, seed=2)
    assert df["week"].min() >= 1
    assert df["week"].max() <= weeks


def test_generate_events_only_produces_known_event_types():
    df = gen.generate_events(n_users=20, weeks=3, seed=3)
    assert set(df["event_type"].unique()) <= {"view", "add_to_cart", "purchase"}


def test_generate_events_product_ids_are_valid_catalog_ids():
    df = gen.generate_events(n_users=20, weeks=3, seed=4)
    product_ids, _ = gen.load_catalog(gen.CATALOG_PATH)
    valid_ids = set(product_ids)
    assert set(df["product_id"].unique()) <= valid_ids


def test_generate_events_funnel_counts_are_monotonically_decreasing():
    # Every purchase requires a preceding add_to_cart, which requires a
    # preceding view -- so in aggregate, purchases <= carts <= views.
    df = gen.generate_events(n_users=50, weeks=6, seed=5)
    counts = df["event_type"].value_counts()
    assert counts.get("purchase", 0) <= counts.get("add_to_cart", 0) <= counts.get("view", 0)
    # With 50 users over 6 weeks, all three event types should actually
    # occur -- an empty funnel stage would silently pass the <= chain
    # above without this.
    assert counts.get("view", 0) > 0
    assert counts.get("add_to_cart", 0) > 0
    assert counts.get("purchase", 0) > 0


def test_generate_events_is_deterministic_given_a_fixed_seed():
    # Absolute timestamps are NOT expected to match between calls --
    # window_start is anchored to pd.Timestamp.now() (line 70), which
    # genuinely differs by however many milliseconds elapse between the
    # two invocations, by design (the log always ends "now", not at some
    # fixed historical date). What the seed actually pins down is
    # everything else: which user, which product, which event type, in
    # what order, and each event's offset relative to the window start.
    df1 = gen.generate_events(n_users=10, weeks=2, seed=42)
    df2 = gen.generate_events(n_users=10, weeks=2, seed=42)
    assert df1.drop(columns=["timestamp"]).equals(df2.drop(columns=["timestamp"]))
    offsets1 = df1["timestamp"] - df1["timestamp"].iloc[0]
    offsets2 = df2["timestamp"] - df2["timestamp"].iloc[0]
    assert offsets1.equals(offsets2)


def test_generate_events_different_seeds_produce_different_data():
    df1 = gen.generate_events(n_users=10, weeks=2, seed=1)
    df2 = gen.generate_events(n_users=10, weeks=2, seed=2)
    assert not df1.equals(df2)


def test_generate_events_timestamps_sorted_ascending():
    df = gen.generate_events(n_users=10, weeks=2, seed=6)
    assert df["timestamp"].is_monotonic_increasing
