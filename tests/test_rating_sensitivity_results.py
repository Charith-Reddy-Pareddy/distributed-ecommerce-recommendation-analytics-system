import json
import math
from pathlib import Path


def test_recorded_sensitivity_runs_match_the_real_source_and_grid() -> None:
    path = Path(__file__).parents[1] / "results" / "tabular_rating_sensitivity.json"
    result = json.loads(path.read_text(encoding="utf-8"))

    assert result["source_file"] == "amazon_reviews_2023_subscription_boxes.jsonl.gz"
    assert result["source_sha256"] == (
        "640a26e1a77a59245d11e98e3c0fd300815ede6f9c670932609bf2c672ea0fde"
    )
    config = result["config"]
    assert config["cutoffs"] == [0.7, 0.8, 0.9]
    assert config["seeds"] == [42, 7, 17]
    assert config["num_trees"] == 30
    assert config["max_depth"] == 6
    assert config["relative_error"] == 0.01
    assert config["spark_version"] == "4.2.1"
    assert config["timezone"] == "UTC"

    runs = result["runs"]
    expected = {
        (cutoff, seed)
        for cutoff in config["cutoffs"]
        for seed in config["seeds"]
    }
    assert len(runs) == len(expected)
    assert {(row["cutoff_quantile"], row["seed"]) for row in runs} == expected
    for cutoff in config["cutoffs"]:
        rows = [row for row in runs if row["cutoff_quantile"] == cutoff]
        assert len({row["cutoff_timestamp"] for row in rows}) == 1
        assert len({row["train_rows"] for row in rows}) == 1
        assert len({row["test_rows"] for row in rows}) == 1
        assert rows[0]["train_rows"] + rows[0]["test_rows"] == 16216
    for row in runs:
        metrics = (
            row["baseline_mae"],
            row["baseline_rmse"],
            row["model_mae"],
            row["model_rmse"],
        )
        assert all(math.isfinite(value) and value >= 0 for value in metrics)
        assert row["model_mae"] < row["baseline_mae"]
        assert row["model_rmse"] < row["baseline_rmse"]
