"""Regression tests for two real bugs found live-auditing this repo.

1. jobs/spark-streaming/session_and_anomaly.py's Welford running-mean/
variance state used to be a plain dict mutated unconditionally inside
foreachBatch's process_batch. Spark's own docs (and this project's
docs/ARCHITECTURE.md "Delivery semantics" section) say foreachBatch
"can be called more than once for the same batch ID" during failure
recovery -- the Cassandra write of the same per-window count right
next to it in the same function was already made an idempotent upsert
specifically because of that, but the Welford update had no equivalent
guard, so a replayed batch would double-count that window's
event_count into every subsequent z-score, permanently.

2. The idempotency guard's own state (_applied_windows) was an
unpruned set that grew by one entry per (product_id, window_start)
pair for the entire life of a streaming query that runs indefinitely
(main() calls awaitAnyTermination()) -- an unbounded memory leak
protecting against a replay scenario that only ever happens near the
current window (checkpoint recovery reprocesses the most recent
uncommitted micro-batch, not one from hours or days ago).

WelfordAnomalyTracker was pulled out of session_and_anomaly.py into
its own module specifically so this logic doesn't need a pyspark test
dependency to verify. window_start values here are real datetimes,
matching what Spark's window column actually provides in production
(row["window"]["start"]) -- the pruning logic does timedelta
arithmetic on them, so a test using bare integers wouldn't exercise
the real code path.
"""
import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "jobs" / "spark-streaming"))

from welford import WelfordAnomalyTracker  # noqa: E402

BASE = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)


def minute(i):
    return BASE + datetime.timedelta(minutes=i)


def test_no_verdict_before_min_observations():
    tracker = WelfordAnomalyTracker(z_score_threshold=2.5, min_observations=3)
    assert tracker.observe(product_id=1, window_start=minute(1), event_count=10) is None
    assert tracker.observe(product_id=1, window_start=minute(2), event_count=10) is None
    assert tracker.observe(product_id=1, window_start=minute(3), event_count=10) is None


def test_flags_a_genuine_spike_as_anomalous():
    tracker = WelfordAnomalyTracker(z_score_threshold=2.5, min_observations=3)
    # Some variance in the history -- a constant history has stddev=0,
    # which both old and new code correctly refuse to divide by (no
    # verdict, not an infinite z-score); that's a real edge case, not
    # what this test is about.
    for w, c in enumerate([10, 12, 9, 11, 10]):
        tracker.observe(product_id=1, window_start=minute(w), event_count=c)
    stats = tracker.observe(product_id=1, window_start=minute(5), event_count=200)
    assert stats is not None
    assert stats["is_anomaly"] is True
    assert stats["z_score"] > 2.5


def test_does_not_flag_normal_variation():
    tracker = WelfordAnomalyTracker(z_score_threshold=2.5, min_observations=3)
    counts = [10, 12, 9, 11, 10, 13, 8]
    stats = None
    for w, c in enumerate(counts):
        stats = tracker.observe(product_id=1, window_start=minute(w), event_count=c)
    assert stats is not None
    assert stats["is_anomaly"] is False


def test_replaying_the_same_window_does_not_double_count():
    # The actual bug. Same (product_id, window_start) observed twice --
    # e.g. Spark re-invoking process_batch for the same batch_id after a
    # driver restart -- must only be folded into running stats once.
    tracker = WelfordAnomalyTracker(z_score_threshold=2.5, min_observations=3)
    history = [10, 12, 9, 11]
    for w, c in enumerate(history):
        tracker.observe(product_id=1, window_start=minute(w), event_count=c)

    # First (real) application of window 4.
    first = tracker.observe(product_id=1, window_start=minute(4), event_count=50)
    # A replay of the exact same batch/window.
    replay = tracker.observe(product_id=1, window_start=minute(4), event_count=50)

    assert first is not None
    assert replay is None, "a replayed window must be a no-op, not a second application"

    # Confirm the internal state actually reflects a *single* application,
    # not two -- observe() one more genuinely new window and check the
    # running mean is consistent with n having advanced by exactly 1 for
    # window 4, not 2.
    without_replay = WelfordAnomalyTracker(z_score_threshold=2.5, min_observations=3)
    for w, c in enumerate(history):
        without_replay.observe(product_id=1, window_start=minute(w), event_count=c)
    without_replay.observe(product_id=1, window_start=minute(4), event_count=50)
    expected_next = without_replay.observe(product_id=1, window_start=minute(5), event_count=10)

    with_replay_attempted = tracker.observe(product_id=1, window_start=minute(5), event_count=10)

    assert with_replay_attempted["mean"] == expected_next["mean"]
    assert with_replay_attempted["stddev"] == expected_next["stddev"]


def test_different_products_tracked_independently():
    tracker = WelfordAnomalyTracker(z_score_threshold=2.5, min_observations=3)
    for w in range(4):
        tracker.observe(product_id=1, window_start=minute(w), event_count=1000)
    # product 2 has no history at all yet -- must not be affected by
    # product 1's completely different scale.
    assert tracker.observe(product_id=2, window_start=minute(0), event_count=5) is None
    assert tracker.tracked_product_count() == 2


def test_applied_windows_are_pruned_once_far_enough_in_the_past():
    # Regression test for the memory-leak fix: _applied_windows must not
    # grow forever. Observe windows spanning well past the retention
    # period (default 2h) and confirm old entries actually get dropped,
    # not just "eventually stop mattering."
    tracker = WelfordAnomalyTracker(
        z_score_threshold=2.5, min_observations=3, applied_window_retention=datetime.timedelta(minutes=10)
    )
    for w in range(30):  # 30 minutes of 1-minute windows, > the 10-minute retention
        tracker.observe(product_id=1, window_start=minute(w), event_count=10)

    # The earliest windows are long past the retention cutoff relative to
    # the latest one observed (minute(29)) and must have been pruned.
    assert (1, minute(0)) not in tracker._applied_windows
    assert (1, minute(1)) not in tracker._applied_windows
    # Recent windows must still be present, or a genuine near-term replay
    # would go undetected.
    assert (1, minute(29)) in tracker._applied_windows
    assert (1, minute(28)) in tracker._applied_windows


def test_pruning_does_not_break_replay_detection_for_recent_windows():
    # The fix must not overcorrect: a replay of a *recent* window (well
    # within the retention period) must still be caught even after many
    # other windows have been observed and pruning has run repeatedly.
    tracker = WelfordAnomalyTracker(
        z_score_threshold=2.5, min_observations=3, applied_window_retention=datetime.timedelta(minutes=10)
    )
    for w in range(20):
        tracker.observe(product_id=1, window_start=minute(w), event_count=10)

    replay = tracker.observe(product_id=1, window_start=minute(19), event_count=10)
    assert replay is None, "a recent window's replay must still be detected after pruning has run"
