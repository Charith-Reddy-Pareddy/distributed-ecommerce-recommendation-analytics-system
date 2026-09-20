"""Per-product running mean/variance (Welford's online algorithm) for
anomaly detection, with the idempotency guard session_and_anomaly.py's
process_batch() needs: Spark's foreachBatch docs warn it "can be called
more than once for the same batch ID" during failure recovery. The
Cassandra write of the same per-window count is a natural upsert
(re-inserting the same primary key just overwrites), but an online
increment to n/mean/m2 has no such natural idempotency -- a replayed
batch would double-count that window's event_count into every
subsequent z-score, permanently, with no way to detect or undo it
later. Pulled out of session_and_anomaly.py into its own pure,
Spark-independent module specifically so this logic -- the exact thing
that had the bug -- is unit-testable without a Spark test dependency.
"""
import datetime

# A checkpoint-recovery replay reprocesses the most recent uncommitted
# micro-batch (seconds to minutes old, not hours), so remembering every
# (product_id, window_start) pair ever seen for the life of a streaming
# query that runs indefinitely (main() calls awaitAnyTermination()) is
# an unbounded memory leak protecting against a scenario that only ever
# happens near the current window. Retaining a generous multiple of
# that catches every realistic replay without growing forever.
DEFAULT_APPLIED_WINDOW_RETENTION = datetime.timedelta(hours=2)


class WelfordAnomalyTracker:
    def __init__(self, z_score_threshold: float, min_observations: int = 3, applied_window_retention=None):
        self.z_score_threshold = z_score_threshold
        self.min_observations = min_observations
        self.applied_window_retention = applied_window_retention or DEFAULT_APPLIED_WINDOW_RETENTION
        self._running_stats: dict[int, tuple[int, float, float]] = {}
        self._applied_windows: set[tuple] = set()  # (product_id, window_start) pairs
        self._max_window_start = None

    def tracked_product_count(self) -> int:
        return len(self._running_stats)

    def _prune_applied_windows(self, window_start):
        # Only worth scanning when a genuinely new, later window boundary
        # shows up -- with one Spark trigger per window interval, that's
        # roughly once per micro-batch, not once per observe() call.
        if self._max_window_start is not None and window_start <= self._max_window_start:
            return
        self._max_window_start = window_start
        cutoff = window_start - self.applied_window_retention
        self._applied_windows = {k for k in self._applied_windows if k[1] >= cutoff}

    def observe(self, product_id: int, window_start, event_count: int) -> dict | None:
        """Folds one (product_id, window_start, event_count) observation into
        the running stats, unless that exact window was already applied
        (a replayed batch) -- in which case it's a no-op returning None.
        Returns a dict with the z-score info (mean/stddev/z_score) if
        enough history exists to compute one, whether or not it crosses
        the anomaly threshold; None otherwise (no-op, or too little
        history yet).
        """
        window_key = (product_id, window_start)
        if window_key in self._applied_windows:
            return None
        self._applied_windows.add(window_key)
        self._prune_applied_windows(window_start)

        n, mean, m2 = self._running_stats.get(product_id, (0, 0.0, 0.0))

        result = None
        if n >= self.min_observations:
            variance = m2 / n
            stddev = variance**0.5
            if stddev > 0:
                z_score = (event_count - mean) / stddev
                result = {
                    "mean": mean,
                    "stddev": stddev,
                    "z_score": z_score,
                    "is_anomaly": abs(z_score) > self.z_score_threshold,
                }

        new_n = n + 1
        delta = event_count - mean
        new_mean = mean + delta / new_n
        new_m2 = m2 + delta * (event_count - new_mean)
        self._running_stats[product_id] = (new_n, new_mean, new_m2)

        return result
