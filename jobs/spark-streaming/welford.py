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


class WelfordAnomalyTracker:
    def __init__(self, z_score_threshold: float, min_observations: int = 3):
        self.z_score_threshold = z_score_threshold
        self.min_observations = min_observations
        self._running_stats: dict[int, tuple[int, float, float]] = {}
        self._applied_windows: set[tuple] = set()  # (product_id, window_start) pairs

    def tracked_product_count(self) -> int:
        return len(self._running_stats)

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
