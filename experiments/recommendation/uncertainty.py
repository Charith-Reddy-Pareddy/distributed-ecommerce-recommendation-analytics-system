"""Per-user bootstrap summaries for models evaluated on a common population."""
from experiments.recommendation.bootstrap import bootstrap_ci, paired_bootstrap_diff

METRICS = ('precision', 'recall', 'map', 'ndcg')


def summarize_user_metrics(by_model, reference, n_resamples=5000, ci=.95, seed=42):
    if reference not in by_model:
        raise ValueError(f"reference model {reference!r} is missing")
    users = set(by_model[reference])
    if not users:
        raise ValueError("bootstrap requires at least one evaluated user")
    for model, per_user in by_model.items():
        if set(per_user) != users:
            raise ValueError(f"model {model!r} has a different user population")
        for metrics in per_user.values():
            if not set(METRICS) <= set(metrics):
                raise ValueError(f"model {model!r} is missing user metrics")

    summary = {'n_users': len(users), 'confidence_level': ci, 'n_resamples': n_resamples,
               'seed': seed, 'models': {}, 'paired_differences_vs_reference': {}}
    ordered_users = sorted(users)
    ref_rows = by_model[reference]
    for model, per_user in by_model.items():
        summary['models'][model] = {}
        if model != reference:
            summary['paired_differences_vs_reference'][model] = {}
        for metric in METRICS:
            values = [per_user[user][metric] for user in ordered_users]
            estimate, lower, upper = bootstrap_ci(values, n_resamples, ci, seed)
            summary['models'][model][metric] = {'mean': estimate, 'lower': lower, 'upper': upper}
            if model != reference:
                ref_values = [ref_rows[user][metric] for user in ordered_users]
                diff, low, high = paired_bootstrap_diff(values, ref_values, n_resamples, ci, seed)
                summary['paired_differences_vs_reference'][model][metric] = {
                    'mean_difference': diff, 'lower': low, 'upper': high,
                    'reference': reference,
                }
    return summary
