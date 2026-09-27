from experiments.recommendation.scalability_benchmark_real import build_real_engine


def test_build_real_engine_keeps_only_top_n_items():
    rows = [
        ("u1", 1, 5.0), ("u2", 1, 4.0), ("u3", 1, 5.0),  # item 1: 3 interactions
        ("u1", 2, 5.0), ("u2", 2, 4.0),                   # item 2: 2 interactions
        ("u4", 3, 5.0),                                    # item 3: 1 interaction -- dropped
    ]
    engine, n_users = build_real_engine(rows, n_items=2)
    assert set(engine.item_users.keys()) == {1, 2}
    assert n_users == 3  # u1, u2, u3 all touch item 1 or 2; u4 only touched the dropped item


def test_build_real_engine_counts_only_real_users_touching_kept_items():
    rows = [("u1", 1, 5.0), ("u2", 1, 5.0), ("u3", 2, 5.0)]
    engine, n_users = build_real_engine(rows, n_items=1)
    assert set(engine.item_users.keys()) == {1}
    assert n_users == 2


def test_build_real_engine_sums_weight_for_repeated_user_item_pairs():
    rows = [("u1", 1, 3.0), ("u1", 1, 2.0)]
    engine, _n_users = build_real_engine(rows, n_items=1)
    assert engine.user_item["u1"][1] == 5.0
