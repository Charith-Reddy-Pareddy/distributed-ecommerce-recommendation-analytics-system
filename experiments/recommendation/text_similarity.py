"""Minimal TF-IDF + cosine similarity, pure Python (no numpy/sklearn --
this project stopped carrying either as a dependency once the synthetic
recommendation pipeline that needed them was removed; a hand-rolled
version over ~150 short product-text documents is simple enough not to
justify reintroducing a heavy dependency for it).
"""
import math
import re
from collections import Counter

_TOKEN_RE = re.compile(r"[a-z0-9]+")


def tokenize(text: str) -> list[str]:
    return _TOKEN_RE.findall(text.lower())


def build_tfidf(documents: dict[int, str]) -> dict[int, dict[str, float]]:
    """documents: {item_id: raw text}. Returns {item_id: {term: tfidf}}."""
    tokenized = {item_id: tokenize(text) for item_id, text in documents.items()}
    n_docs = len(tokenized)

    doc_freq: Counter = Counter()
    for tokens in tokenized.values():
        doc_freq.update(set(tokens))

    idf = {term: math.log((n_docs + 1) / (df + 1)) + 1 for term, df in doc_freq.items()}

    vectors: dict[int, dict[str, float]] = {}
    for item_id, tokens in tokenized.items():
        term_counts = Counter(tokens)
        total = sum(term_counts.values()) or 1
        vectors[item_id] = {term: (count / total) * idf[term] for term, count in term_counts.items()}
    return vectors


def cosine(a: dict[str, float], b: dict[str, float]) -> float:
    common = set(a) & set(b)
    if not common:
        return 0.0
    dot = sum(a[k] * b[k] for k in common)
    norm_a = math.sqrt(sum(v * v for v in a.values()))
    norm_b = math.sqrt(sum(v * v for v in b.values()))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def add_vectors(vectors: list[dict[str, float]], weights: list[float]) -> dict[str, float]:
    """Weighted sum of several TF-IDF vectors -- used to build a user's
    content profile from the items they've interacted with.
    """
    profile: dict[str, float] = {}
    for vec, weight in zip(vectors, weights):
        for term, value in vec.items():
            profile[term] = profile.get(term, 0.0) + value * weight
    return profile
