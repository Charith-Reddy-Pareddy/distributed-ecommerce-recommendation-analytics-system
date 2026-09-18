import numpy as np
import pandas as pd
import torch

from experiments.recommendation.neural_cf.model import NeuMF
from experiments.recommendation.neural_cf.train import ImplicitFeedbackDataset, train_one_epoch


def test_forward_returns_one_logit_per_pair():
    model = NeuMF(n_users=10, n_items=20, gmf_dim=4, mlp_embedding_dim=4, mlp_hidden_dims=(8, 4))
    user_ids = torch.tensor([1, 2, 3])
    item_ids = torch.tensor([5, 6, 7])
    logits = model(user_ids, item_ids)
    assert logits.shape == (3,)
    assert logits.dtype == torch.float32


def test_forward_is_deterministic_given_fixed_weights():
    torch.manual_seed(0)
    model = NeuMF(n_users=5, n_items=5, gmf_dim=4, mlp_embedding_dim=4, mlp_hidden_dims=(8, 4))
    model.eval()
    user_ids, item_ids = torch.tensor([1, 2]), torch.tensor([1, 2])
    first = model(user_ids, item_ids)
    second = model(user_ids, item_ids)
    assert torch.equal(first, second)


def test_score_all_items_covers_every_item_and_is_a_probability():
    model = NeuMF(n_users=5, n_items=20, gmf_dim=4, mlp_embedding_dim=4, mlp_hidden_dims=(8, 4))
    scores = model.score_all_items(user_id=1, n_items=20)
    assert scores.shape == (20,)
    assert torch.all(scores >= 0) and torch.all(scores <= 1)


def test_score_all_items_orders_item_1_first_in_the_output():
    # score_all_items scores item ids 1..n_items in order (see model.py's
    # torch.arange(1, n_items + 1)) -- this is what recommend_for_test_users
    # relies on when it maps argsort results back to +1-indexed item ids.
    model = NeuMF(n_users=3, n_items=5, gmf_dim=4, mlp_embedding_dim=4, mlp_hidden_dims=(8, 4))
    scores = model.score_all_items(user_id=1, n_items=5)
    manual_first_item_score = torch.sigmoid(model(torch.tensor([1]), torch.tensor([1])))
    assert torch.isclose(scores[0], manual_first_item_score[0])


def make_toy_positives():
    # user 1 interacted with items 1, 2; user 2 interacted with item 3.
    return pd.DataFrame({
        "user_id": [1, 1, 2],
        "product_id": [1, 2, 3],
        "weight": [1.0, 3.0, 5.0],
    })


def test_dataset_never_samples_a_negative_the_user_already_interacted_with():
    positives = make_toy_positives()
    rng = np.random.default_rng(0)
    dataset = ImplicitFeedbackDataset(positives, n_items=3, rng=rng)  # only 3 items total, forces exhaustive checking

    for user_id, item_id, label in zip(dataset.users, dataset.items, dataset.labels):
        if label == 0.0:
            assert item_id not in dataset.user_positive_items.get(user_id, set())


def test_dataset_produces_neg_samples_per_positive_negatives_for_each_positive():
    positives = make_toy_positives()
    rng = np.random.default_rng(0)
    dataset = ImplicitFeedbackDataset(positives, n_items=10, rng=rng)

    n_positives = len(positives)
    n_negatives = (dataset.labels == 0.0).sum()
    assert n_negatives == n_positives * 4  # NEG_SAMPLES_PER_POSITIVE in train.py


def test_dataset_positive_sample_weight_uses_alpha_confidence_formula():
    # weight column [1.0, 3.0, 5.0] -> sample weight 1 + ALPHA*w = [2.0, 4.0, 6.0] at ALPHA=1.0
    positives = make_toy_positives()
    rng = np.random.default_rng(0)
    dataset = ImplicitFeedbackDataset(positives, n_items=10, rng=rng)

    positive_mask = dataset.labels == 1.0
    assert sorted(dataset.sample_weights[positive_mask].tolist()) == [2.0, 4.0, 6.0]
    negative_mask = dataset.labels == 0.0
    assert (dataset.sample_weights[negative_mask] == 1.0).all()


def test_training_reduces_loss_on_a_toy_dataset():
    positives = pd.DataFrame({
        "user_id": np.repeat(np.arange(1, 11), 3),
        "product_id": np.tile(np.arange(1, 4), 10),
        "weight": np.tile([1.0, 3.0, 5.0], 10),
    })
    rng = np.random.default_rng(0)
    torch.manual_seed(0)
    model = NeuMF(n_users=10, n_items=10, gmf_dim=4, mlp_embedding_dim=4, mlp_hidden_dims=(8, 4))
    optimizer = torch.optim.Adam(model.parameters(), lr=1e-2)

    first_loss = train_one_epoch(
        model, lambda: ImplicitFeedbackDataset(positives, 10, rng), optimizer, "cpu"
    )
    for _ in range(9):
        last_loss = train_one_epoch(
            model, lambda: ImplicitFeedbackDataset(positives, 10, rng), optimizer, "cpu"
        )
    assert last_loss < first_loss
