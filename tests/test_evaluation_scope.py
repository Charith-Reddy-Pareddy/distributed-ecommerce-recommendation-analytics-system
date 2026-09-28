import pytest
from experiments.recommendation.evaluation_scope import build_scope


def test_scope_uses_training_frequency_and_same_eligible_population():
    train = [('a', 1, 5., 1), ('a', 2, 4., 2), ('b', 2, 3., 1), ('b', 3, 5., 2)]
    test = [('a', 3, 5., 3), ('b', 1, 4., 3), ('a', 99, 5., 3), ('cold', 1, 5., 3)]
    scope = build_scope(train, test, max_items=3, min_history=2)
    assert scope.items == (1, 2, 3)
    assert scope.users == ('a', 'b')
    assert scope.actuals == {'a': {3}, 'b': {1}}
    assert scope.seen == {'a': {1, 2}, 'b': {2, 3}}
    assert scope.train == train


def test_scope_filters_training_rows_and_excludes_overlap():
    train = [('a', 1, 5., 1), ('a', 9, 5., 1), ('b', 2, 5., 1), ('c', 2, 5., 1)]
    scope = build_scope(train, [('a', 1, 5., 2), ('a', 2, 5., 2)], max_items=2, min_history=1)
    assert scope.items == (1, 2)
    assert scope.actuals == {'a': {2}}
    assert all(row[1] != 9 for row in scope.train)


def test_sampling_is_independent_of_input_order():
    train = [(str(u), 1, 1., 1) for u in range(30)] + [('donor', 2, 1., 1)]
    test = [(str(u), 2, 1., 2) for u in range(30)]
    first = build_scope(train, test, max_users=5, min_history=1)
    second = build_scope(list(reversed(train)), list(reversed(test)), max_users=5, min_history=1)
    assert first.users == second.users


def test_empty_scope_fails_explicitly():
    with pytest.raises(ValueError, match='No eligible'):
        build_scope([], [])
