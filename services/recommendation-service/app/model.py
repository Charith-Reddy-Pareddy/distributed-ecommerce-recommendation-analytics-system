"""In-memory item-based collaborative filtering engine.

The engine rebuilds its state by replaying the full Redis Stream on
startup (so restarts don't lose history), then keeps listening for new
events and updates its interaction matrices incrementally. This keeps
the service stateless from a deployment point of view -- Redis is the
durable source of truth, this process just holds a fast in-memory
projection of it.

For a small/medium catalog this dense-dict approach is simple and fast
enough. At larger scale you'd swap this for a proper ANN/matrix-
factorization pipeline (e.g. implicit, Spark ALS) writing precomputed
recommendations to a store like Redis or a feature store.
"""
import json
import math
import threading
from collections import defaultdict

from .redis_client import STREAM_NAME, redis_client

EVENT_WEIGHTS = {"view": 1.0, "add_to_cart": 3.0, "purchase": 5.0}


class RecommendationEngine:
    def __init__(self):
        self._lock = threading.Lock()
        self.user_item: dict[int, dict[int, float]] = defaultdict(dict)
        self.item_users: dict[int, dict[int, float]] = defaultdict(dict)
        self._last_id = "0-0"

    def _apply_event(self, event: dict) -> None:
        user_id = event["user_id"]
        product_id = event["product_id"]
        weight = EVENT_WEIGHTS.get(event["event_type"], 1.0)

        with self._lock:
            self.user_item[user_id][product_id] = (
                self.user_item[user_id].get(product_id, 0.0) + weight
            )
            self.item_users[product_id][user_id] = (
                self.item_users[product_id].get(user_id, 0.0) + weight
            )

    def bootstrap(self) -> None:
        for entry_id, fields in redis_client.xrange(STREAM_NAME, min="-", max="+"):
            self._apply_event(json.loads(fields["data"]))
            self._last_id = entry_id

    def listen_forever(self) -> None:
        while True:
            response = redis_client.xread(
                {STREAM_NAME: self._last_id}, block=5000, count=100
            )
            if not response:
                continue
            for _, entries in response:
                for entry_id, fields in entries:
                    self._apply_event(json.loads(fields["data"]))
                    self._last_id = entry_id

    def start(self) -> None:
        self.bootstrap()
        thread = threading.Thread(target=self.listen_forever, daemon=True)
        thread.start()

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

    def recommend_for_user(self, user_id: int, top_n: int = 10) -> list[tuple[int, float]]:
        with self._lock:
            interacted = dict(self.user_item.get(user_id, {}))

        if not interacted:
            return self.popular_items(top_n)

        candidate_scores: dict[int, float] = defaultdict(float)
        for product_id, weight in interacted.items():
            for similar_id, sim_score in self.similar_items(product_id, top_n=20):
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
