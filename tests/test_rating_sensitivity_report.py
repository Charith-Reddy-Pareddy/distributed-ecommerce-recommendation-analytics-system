import json
import statistics
from pathlib import Path


def test_report_table_matches_recorded_sensitivity_runs() -> None:
    root = Path(__file__).parents[1]
    results = json.loads(
        (root / "results" / "tabular_rating_sensitivity.json").read_text(
            encoding="utf-8"
        )
    )
    report = (root / "docs" / "research_report.md").read_text(encoding="utf-8")
    section = report.split("## Tabular rating sensitivity", 1)[1]

    labels = {0.7: "70th percentile", 0.8: "80th percentile", 0.9: "90th percentile"}
    for cutoff, label in labels.items():
        rows = [r for r in results["runs"] if r["cutoff_quantile"] == cutoff]
        base = rows[0]
        mae = [r["model_mae"] for r in rows]
        rmse = [r["model_rmse"] for r in rows]
        mae_drop = (base["baseline_mae"] - statistics.mean(mae)) / base["baseline_mae"]
        rmse_drop = (base["baseline_rmse"] - statistics.mean(rmse)) / base["baseline_rmse"]
        expected = (
            f"| {label} | {base['train_rows']:,} / {base['test_rows']:,} "
            f"| {base['baseline_mae']:.4f} "
            f"| {statistics.mean(mae):.4f} ({min(mae):.4f}–{max(mae):.4f}) "
            f"| {mae_drop:.2%} | {base['baseline_rmse']:.4f} "
            f"| {statistics.mean(rmse):.4f} ({min(rmse):.4f}–{max(rmse):.4f}) "
            f"| {rmse_drop:.2%} |"
        )
        assert expected in section


def test_report_intervals_match_recorded_sensitivity_runs() -> None:
    root = Path(__file__).parents[1]
    results = json.loads(
        (root / "results" / "tabular_rating_sensitivity.json").read_text(
            encoding="utf-8"
        )
    )
    report = (root / "docs" / "research_report.md").read_text(encoding="utf-8")
    section = report.split("### Paired error intervals", 1)[1]

    labels = {0.7: "70th percentile", 0.8: "80th percentile", 0.9: "90th percentile"}
    for row in results["runs"]:
        expected = (
            f"| {labels[row['cutoff_quantile']]} | {row['seed']} "
            f"| {row['mae_delta']:.4f} "
            f"[{row['mae_ci_low']:.4f}, {row['mae_ci_high']:.4f}] "
            f"| {row['rmse_delta']:.4f} "
            f"[{row['rmse_ci_low']:.4f}, {row['rmse_ci_high']:.4f}] |"
        )
        assert expected in section
