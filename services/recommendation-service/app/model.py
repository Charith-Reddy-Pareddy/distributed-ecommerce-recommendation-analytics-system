"""In-memory item-based collaborative filtering engine.

The model rebuilds its state from Kafka when the service starts and
then updates as new events arrive.

similar_items() served from a periodically-refreshed precomputed
top-N-neighbor cache, not live O(n_items) cosine per request -- see
`refresh_neighbor_cache`. At this project's real scale (300 items) the
live computation was already fast; the cache exists because it clearly
would not be at catalog sizes an order of magnitude or two larger, and
"the model works at 300 items" isn't the same claim as "the model
scales" -- see experiments/recommendation/scalability_benchmark.py for
the measured difference at 300 / 10K / 50K items.
"""
import json
import math
import os
import threading
import time
from collections import defaultdict

from .kafka_consumer import new_consumer

EVENT_WEIGHTS = {"view": 1.0, "add_to_cart": 3.0, "purchase": 5.0}
NEIGHBOR_REFRESH_INTERVAL_SECONDS = int(os.getenv("NEIGHBOR_REFRESH_INTERVAL_SECONDS", "30"))
NEIGHBOR_CACHE_SIZE = 50  # generous headroom over any top_n callers actually request


class RecommendationEngine:
    def __init__(self):
        self._lock = threading.Lock()
        self.user_item: dict[int, dict[int, float]] = defaultdict(lambda: defaultdict(float))
        self.item_users: dict[int, dict[int, float]] = defaultdict(lambda: defaultdict(float))
        self._neighbor_cache: dict[int, list[tuple[int, float]]] = {}

    def _apply_event(self, event: dict) -> None:
        user_id = event["user_id"]
        product_id = event["product_id"]
        weight = EVENT_WEIGHTS.get(event["event_type"], 1.0)

        with self._lock:
            self.user_item[user_id][product_id] += weight
            self.item_users[product_id][user_id] += weight

    def consume_forever(self) -> None:
        # This runs in a background daemon thread (see start()) -- an
        # uncaught exception here just kills the thread silently, with
        # nothing in the logs to say so and the HTTP server staying up
        # and "healthy" the whole time. Log loudly instead.
        print("[recommendation-engine] consumer thread starting", flush=True)
        try:
            consumer = new_consumer()
        except Exception as e:
            print(f"[recommendation-engine] failed to create consumer: {e!r}", flush=True)
            raise
        print("[recommendation-engine] consumer created, subscribed, polling...", flush=True)

        processed = 0
        try:
            while True:
                try:
                    msg = consumer.poll(timeout=1.0)
                except Exception as e:
                    print(f"[recommendation-engine] poll() raised: {e!r}", flush=True)
                    continue
                if msg is None:
                    continue
                if msg.error():
                    print(f"[recommendation-engine] message error: {msg.error()}", flush=True)
                    continue
                try:
                    self._apply_event(json.loads(msg.value()))
                except Exception as e:
                    print(f"[recommendation-engine] failed to apply event: {e!r}", flush=True)
                    continue
                processed += 1
                if processed % 5000 == 0:
                    print(f"[recommendation-engine] processed {processed} events so far", flush=True)
        finally:
            print(f"[recommendation-engine] consumer thread exiting after {processed} events", flush=True)
            consumer.close()

    def start(self) -> None:
        threading.Thread(target=self.consume_forever, daemon=True).start()
        threading.Thread(target=self.refresh_neighbors_forever, daemon=True).start()

    @staticmethod
    def _cosine(a: dict[int, float], b: dict[int, float]) -> float:
        common = set(a) & set(b)
        if not common:
            return 0.0
        dot = sum(a[k] * b[k] for k in common)
        norm_a = math.sqrt(sum(v * v for v in a.values()))
        norm_b = math.sqrt(sum(v * v for v in b.values()))
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return dot / (norm_a * norm_b)

    def similar_items(self, product_id: int, top_n: int = 10) -> list[tuple[int, float]]:
        cached = self._neighbor_cache.get(product_id)
        if cached is not None:
            return cached[:top_n]
        # Not in the cache yet -- e.g. a brand-new item added since the
        # last refresh cycle. Fall back to computing it live rather than
        # returning nothing; refresh_neighbor_cache() will pick it up on
        # its next pass.
        return self._similar_items_live(product_id, top_n)

    def _similar_items_live(self, product_id: int, top_n: int) -> list[tuple[int, float]]:
        with self._lock:
            target = self.item_users.get(product_id)
            if not target:
                return []
            scores = [
                (other_id, self._cosine(target, other_users))
                for other_id, other_users in self.item_users.items()
                if other_id != product_id
            ]

        scores = [(pid, score) for pid, score in scores if score > 0]
        scores.sort(key=lambda x: x[1], reverse=True)
        return scores[:top_n]

    def refresh_neighbor_cache(self) -> None:
        """Rebuilds the top-N-neighbor cache for every item -- O(n_items^2),
        meant to run periodically in the background
        (see refresh_neighbors_forever), not per-request.

        Snapshots each item's user-weight dict (not just the outer dict)
        under the lock before computing, so the O(n^2) cosine pass itself
        runs lock-free against a private copy -- live event processing and
        requests aren't blocked for the duration, and reading a dict a
        concurrent writer is still mutating can't raise mid-iteration.
        """
        with self._lock:
            items = [(pid, dict(users)) for pid, users in self.item_users.items()]

        new_cache: dict[int, list[tuple[int, float]]] = {}
        for product_id, target in items:
            scores = [
                (other_id, self._cosine(target, other_users))
                for other_id, other_users in items
                if other_id != product_id
            ]
            scores = [(pid, score) for pid, score in scores if score > 0]
            scores.sort(key=lambda x: x[1], reverse=True)
            new_cache[product_id] = scores[:NEIGHBOR_CACHE_SIZE]

        self._neighbor_cache = new_cache

    def refresh_neighbors_forever(self) -> None:
        while True:
            time.sleep(NEIGHBOR_REFRESH_INTERVAL_SECONDS)
            try:
                self.refresh_neighbor_cache()
            except Exception as e:
                print(f"[recommendation-engine] neighbor cache refresh failed: {e!r}", flush=True)

    def recommend_for_user(self, user_id: int, top_n: int = 10) -> list[tuple[int, float]]:
        with self._lock:
            interacted = dict(self.user_item.get(user_id, {}))

        if not interacted:
            return self.popular_items(top_n)

        candidate_scores: dict[int, float] = defaultdict(float)
        for product_id, weight in interacted.items():
            # 50, not some smaller round number: rides NEIGHBOR_CACHE_SIZE
            # rather than under-cutting it, since considering more
            # candidates only costs more at cache-refresh time (a periodic
            # background job), not on the request path. (An ablation
            # comparing 5/10/20/50 on a synthetic interaction log used to
            # justify this with a specific number -- removed along with
            # the rest of that log; see git history if you want the
            # reasoning for why fabricated interactions aren't kept
            # around just because they're labeled synthetic.)
            for similar_id, sim_score in self.similar_items(product_id, top_n=NEIGHBOR_CACHE_SIZE):
                if similar_id in interacted:
                    continue
                candidate_scores[similar_id] += sim_score * weight

        if not candidate_scores:
            return self.popular_items(top_n)

        ranked = sorted(candidate_scores.items(), key=lambda x: x[1], reverse=True)
        return ranked[:top_n]

    def popular_items(self, top_n: int = 10) -> list[tuple[int, float]]:
        with self._lock:
            totals = [
                (product_id, sum(users.values()))
                for product_id, users in self.item_users.items()
            ]
        totals.sort(key=lambda x: x[1], reverse=True)
        return totals[:top_n]


engine = RecommendationEngine()
