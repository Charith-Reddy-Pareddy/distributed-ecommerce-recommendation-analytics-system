"""In-memory item-based collaborative filtering engine.

Rebuilds its state by replaying the full Kafka `events` topic on
startup, then updates incrementally as new events arrive -- stateless
from a deployment standpoint, since Kafka is the source of truth and
this is just a fast in-memory projection of it.

The *speed-layer-adjacent* recommender: fast and always-current, but
rebuilt from scratch on restart. The Spark MLlib ALS model (batch
layer) is the higher-quality one, trained offline and served from
HBase; this engine is the real-time fallback / comparison baseline.
"""
import json
import math
import threading
from collections import defaultdict

from .kafka_consumer import new_consumer

EVENT_WEIGHTS = {"view": 1.0, "add_to_cart": 3.0, "purchase": 5.0}


class RecommendationEngine:
    def __init__(self):
        self._lock = threading.Lock()
        self.user_item: dict[int, dict[int, float]] = defaultdict(lambda: defaultdict(float))
        self.item_users: dict[int, dict[int, float]] = defaultdict(lambda: defaultdict(float))

    def _apply_event(self, event: dict) -> None:
        user_id = event["user_id"]
        product_id = event["product_id"]
        weight = EVENT_WEIGHTS.get(event["event_type"], 1.0)

        with self._lock:
            self.user_item[user_id][product_id] += weight
            self.item_users[product_id][user_id] += weight

    def consume_forever(self) -> None:
        consumer = new_consumer()
        try:
            while True:
                msg = consumer.poll(timeout=1.0)
                if msg is None or msg.error():
                    continue
                self._apply_event(json.loads(msg.value()))
        finally:
            consumer.close()

    def start(self) -> None:
        thread = threading.Thread(target=self.consume_forever, daemon=True)
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
