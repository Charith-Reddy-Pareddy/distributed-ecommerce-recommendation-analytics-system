"""Neural Collaborative Filtering (He et al., WWW 2017) -- the fifth
recommender in the RQ2 comparison, and the only one in this project
that's an actual trained neural network rather than a closed-form
factorization (ALS) or a similarity heuristic (item-CF, content-based).

NeuMF fuses two paths that each turn a (user, item) pair into a score:

- **GMF** (Generalized Matrix Factorization) -- an element-wise product
  of a user and an item embedding, the same interaction ALS's dot
  product uses, but with a learned linear layer on top instead of a
  fixed sum, so the model can weight latent dimensions unequally.
- **MLP** -- a *separate* pair of user/item embeddings, concatenated
  (not multiplied) and passed through a shrinking stack of dense
  layers, so the model can also learn interactions a simple product
  can't express -- two dimensions that only matter together in a
  nonlinear way, for instance.

Both paths' final hidden vectors are concatenated and passed through
one more linear layer to a single logit, trained with a sigmoid +
binary cross-entropy loss against implicit positive/negative labels.
This is the standard NeuMF architecture; nothing about it is specific
to this catalog beyond the embedding table sizes.
"""
import torch
from torch import nn


class NeuMF(nn.Module):
    def __init__(
        self,
        n_users: int,
        n_items: int,
        gmf_dim: int = 16,
        mlp_embedding_dim: int = 16,
        mlp_hidden_dims: tuple[int, ...] = (32, 16, 8),
    ):
        super().__init__()
        # +1 on every embedding table: ids in this project are 1-indexed
        # (user_id 1..n_users, product_id 1..n_items), so index 0 is
        # never looked up but keeps `id` usable directly as an index
        # without an off-by-one remap at every call site.
        self.user_gmf = nn.Embedding(n_users + 1, gmf_dim)
        self.item_gmf = nn.Embedding(n_items + 1, gmf_dim)
        self.user_mlp = nn.Embedding(n_users + 1, mlp_embedding_dim)
        self.item_mlp = nn.Embedding(n_items + 1, mlp_embedding_dim)

        mlp_layers = []
        in_dim = mlp_embedding_dim * 2
        for hidden_dim in mlp_hidden_dims:
            mlp_layers += [nn.Linear(in_dim, hidden_dim), nn.ReLU()]
            in_dim = hidden_dim
        self.mlp = nn.Sequential(*mlp_layers)

        self.output = nn.Linear(gmf_dim + mlp_hidden_dims[-1], 1)

        # He et al.'s own initialization: small random embeddings, not
        # PyTorch's default (which is fine for the Linear layers as-is).
        for embedding in (self.user_gmf, self.item_gmf, self.user_mlp, self.item_mlp):
            nn.init.normal_(embedding.weight, std=0.01)

    def forward(self, user_ids: torch.Tensor, item_ids: torch.Tensor) -> torch.Tensor:
        """Returns raw logits, shape (batch,) -- apply sigmoid separately
        (BCEWithLogitsLoss during training; explicit sigmoid at inference,
        see score_all_items below) rather than baking it into the module,
        so the loss function gets the more numerically stable logit form.
        """
        gmf_vector = self.user_gmf(user_ids) * self.item_gmf(item_ids)

        mlp_input = torch.cat([self.user_mlp(user_ids), self.item_mlp(item_ids)], dim=-1)
        mlp_vector = self.mlp(mlp_input)

        fused = torch.cat([gmf_vector, mlp_vector], dim=-1)
        return self.output(fused).squeeze(-1)

    @torch.no_grad()
    def score_all_items(self, user_id: int, n_items: int, device: str = "cpu") -> torch.Tensor:
        """Scores one user against every item id 1..n_items in a single
        batched forward pass -- the inference-time equivalent of ALS's
        recommendForUserSubset, used to rank the full catalog per test
        user rather than a small pre-selected candidate set.
        """
        self.eval()
        item_ids = torch.arange(1, n_items + 1, device=device)
        user_ids = torch.full_like(item_ids, fill_value=user_id)
        logits = self(user_ids, item_ids)
        return torch.sigmoid(logits)
