from experiments.recommendation.split_interactions import random_split, temporal_split


def test_random_split_keeps_sparse_users_entirely_in_train():
    rows = [("u1", 1, 5.0, 100), ("u1", 2, 4.0, 101), ("u1", 3, 3.0, 102)]
    train, test = random_split(rows, seed=1, min_interactions=5)
    assert len(train) == 3
    assert len(test) == 0


def test_random_split_holds_out_fraction_for_active_users():
    rows = [("u1", i, 5.0, 100 + i) for i in range(10)]
    train, test = random_split(rows, seed=1, min_interactions=5)
    assert len(test) == 2  # 20% of 10
    assert len(train) == 8
    assert set(train) | set(test) == set(rows)
    assert set(train) & set(test) == set()


def test_random_split_is_deterministic_for_same_seed():
    rows = [("u1", i, 5.0, 100 + i) for i in range(10)]
    first = random_split(rows, seed=7)
    second = random_split(rows, seed=7)
    assert first == second


def test_temporal_split_orders_by_timestamp_not_input_order():
    rows = [("u1", 1, 5.0, 300), ("u1", 2, 4.0, 100), ("u1", 3, 3.0, 200)]
    train, test = temporal_split(rows, train_fraction=0.67)
    assert train == [("u1", 2, 4.0, 100), ("u1", 3, 3.0, 200)]
    assert test == [("u1", 1, 5.0, 300)]


def test_temporal_split_respects_train_fraction():
    rows = [("u1", i, 5.0, i) for i in range(10)]
    train, test = temporal_split(rows, train_fraction=0.8)
    assert len(train) == 8
    assert len(test) == 2
