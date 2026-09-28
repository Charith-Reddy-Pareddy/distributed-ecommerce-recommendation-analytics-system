"""One bounded population and candidate set shared by every offline model."""
from collections import Counter, defaultdict
from dataclasses import dataclass
import random


@dataclass
class EvaluationScope:
    train: list
    items: tuple[int, ...]
    users: tuple[str, ...]
    actuals: dict[str, set[int]]
    seen: dict[str, set[int]]


def build_scope(train, test, max_items=150, max_users=150, min_history=5, seed=42):
    if min(max_items, max_users, min_history) < 1:
        raise ValueError("Scope limits must be positive")
    # Only training frequency selects the candidate universe. Sort ties by id.
    counts = Counter(row[1] for row in train)
    items = tuple(sorted(sorted(counts, key=lambda i: (-counts[i], i))[:max_items]))
    allowed = set(items)
    bounded_train = [row for row in train if row[1] in allowed]
    seen = defaultdict(set)
    for user, item, *_ in bounded_train:
        seen[user].add(item)
    actuals = defaultdict(set)
    for user, item, *_ in test:
        if item in allowed and item not in seen[user]:
            actuals[user].add(item)
    users = sorted(u for u in actuals if actuals[u] and len(seen[u]) >= min_history)
    if len(users) > max_users:
        users = sorted(random.Random(seed).sample(users, max_users))
    if not users:
        raise ValueError("No eligible users with unseen held-out items in this candidate scope")
    return EvaluationScope(bounded_train, items, tuple(users),
                           {u: actuals[u] for u in users}, {u: seen[u] for u in users})
