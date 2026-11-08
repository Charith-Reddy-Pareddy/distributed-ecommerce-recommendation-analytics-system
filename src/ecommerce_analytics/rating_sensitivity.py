"""Repeat the temporal rating evaluation across cutoffs and model seeds."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path
from typing import Any, NamedTuple, Sequence

from ecommerce_analytics.rating_evaluation import evaluate_rating_predictions
from ecommerce_analytics.rating_bootstrap import bootstrap_rating_errors
from ecommerce_analytics.rating_matrix import FEATURES, build_rating_matrix
from ecommerce_analytics.rating_model import fit_rating_model
from ecommerce_analytics.tabular_rating import build_rating_split


class RatingSensitivityRun(NamedTuple):
    cutoff_quantile: float
    cutoff_timestamp: int
    seed: int
    num_trees: int
    max_depth: int
    train_rows: int
    test_rows: int
    baseline_mae: float
    baseline_rmse: float
    model_mae: float
    model_rmse: float
    bootstrap_resamples: int
    bootstrap_seed: int
    mae_delta: float
    mae_ci_low: float
    mae_ci_high: float
    rmse_delta: float
    rmse_ci_low: float
    rmse_ci_high: float


def _validate(
    cutoffs: Sequence[float], seeds: Sequence[int], num_trees: int, max_depth: int
) -> None:
    if not cutoffs or any(
        type(q) not in (int, float) or not math.isfinite(q) or not 0 < q < 1
        for q in cutoffs
    ):
        raise ValueError("cutoffs must contain quantiles between 0 and 1")
    if len(set(cutoffs)) != len(cutoffs):
        raise ValueError("cutoffs must be unique")
    if not seeds or any(type(seed) is not int or seed < 0 for seed in seeds):
        raise ValueError("seeds must contain non-negative integers")
    if len(set(seeds)) != len(seeds):
        raise ValueError("seeds must be unique")
    if type(num_trees) is not int or num_trees < 1:
        raise ValueError("num_trees must be a positive integer")
    if type(max_depth) is not int or max_depth < 1:
        raise ValueError("max_depth must be a positive integer")


def run_rating_sensitivity(
    reviews: Any,
    cutoffs: Sequence[float] = (0.7, 0.8, 0.9),
    seeds: Sequence[int] = (42, 7, 17),
    num_trees: int = 30,
    max_depth: int = 6,
    relative_error: float = 0.01,
    bootstrap_resamples: int = 1000,
    bootstrap_seed: int = 42,
) -> list[RatingSensitivityRun]:
    """Run real review rows at each cutoff and seed combination."""
    _validate(cutoffs, seeds, num_trees, max_depth)
    if not math.isfinite(relative_error) or not 0 <= relative_error <= 1:
        raise ValueError("relative_error must be between 0 and 1")
    if type(bootstrap_resamples) is not int or bootstrap_resamples < 1:
        raise ValueError("bootstrap_resamples must be a positive integer")
    if type(bootstrap_seed) is not int:
        raise ValueError("bootstrap_seed must be an integer")
    missing = {"user_id", "asin", "rating", "timestamp"}.difference(reviews.columns)
    if missing:
        raise ValueError(f"reviews are missing columns: {', '.join(sorted(missing))}")

    timestamps = reviews.approxQuantile("timestamp", list(cutoffs), relative_error)
    if len(timestamps) != len(cutoffs):
        raise ValueError("Spark returned an unexpected number of cutoff timestamps")
    runs = []
    for quantile, timestamp in zip(cutoffs, timestamps):
        cutoff = int(timestamp)
        matrix = build_rating_matrix(build_rating_split(reviews, cutoff))
        train_rows = matrix.train.count()
        test_rows = matrix.test.count()
        if not train_rows or not test_rows:
            raise ValueError(f"cutoff {quantile} produced an empty train or test split")
        for seed in seeds:
            predictions = fit_rating_model(
                matrix, seed=seed, num_trees=num_trees, max_depth=max_depth
            )
            metrics = evaluate_rating_predictions(predictions)
            intervals = bootstrap_rating_errors(
                predictions, resamples=bootstrap_resamples, seed=bootstrap_seed
            )
            runs.append(
                RatingSensitivityRun(
                    cutoff_quantile=float(quantile),
                    cutoff_timestamp=cutoff,
                    seed=seed,
                    num_trees=num_trees,
                    max_depth=max_depth,
                    train_rows=train_rows,
                    test_rows=test_rows,
                    baseline_mae=metrics.baseline_mae,
                    baseline_rmse=metrics.baseline_rmse,
                    model_mae=metrics.model_mae,
                    model_rmse=metrics.model_rmse,
                    bootstrap_resamples=intervals.resamples,
                    bootstrap_seed=bootstrap_seed,
                    mae_delta=intervals.mae_delta,
                    mae_ci_low=intervals.mae_ci_low,
                    mae_ci_high=intervals.mae_ci_high,
                    rmse_delta=intervals.rmse_delta,
                    rmse_ci_low=intervals.rmse_ci_low,
                    rmse_ci_high=intervals.rmse_ci_high,
                )
            )
    return runs


def _read_reviews(spark: Any, path: Path) -> Any:
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    schema = StructType(
        [
            StructField("user_id", StringType()),
            StructField("asin", StringType()),
            StructField("rating", DoubleType()),
            StructField("timestamp", LongType()),
        ]
    )
    if path.name.endswith(".jsonl.gz"):
        return spark.read.schema(schema).json(str(path))
    return spark.read.option("header", "true").schema(schema).csv(str(path))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--cutoffs", nargs="+", type=float, default=(0.7, 0.8, 0.9))
    parser.add_argument("--seeds", nargs="+", type=int, default=(42, 7, 17))
    parser.add_argument("--trees", type=int, default=30)
    parser.add_argument("--depth", type=int, default=6)
    parser.add_argument("--relative-error", type=float, default=0.01)
    parser.add_argument("--bootstrap-resamples", type=int, default=1000)
    parser.add_argument("--bootstrap-seed", type=int, default=42)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    path = args.input.expanduser().resolve()
    if not path.is_file():
        parser.error(f"input file does not exist: {path}")
    if not (path.name.endswith(".jsonl.gz") or path.suffix.lower() == ".csv"):
        parser.error("input must be an Amazon .jsonl.gz file or a review .csv")

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("real-rating-sensitivity")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        reviews = _read_reviews(spark, path)
        runs = run_rating_sensitivity(
            reviews,
            cutoffs=args.cutoffs,
            seeds=args.seeds,
            num_trees=args.trees,
            max_depth=args.depth,
            relative_error=args.relative_error,
            bootstrap_resamples=args.bootstrap_resamples,
            bootstrap_seed=args.bootstrap_seed,
        )
        result = {
            "source_file": path.name,
            "source_sha256": _sha256(path),
            "config": {
                "cutoffs": args.cutoffs,
                "seeds": args.seeds,
                "num_trees": args.trees,
                "max_depth": args.depth,
                "relative_error": args.relative_error,
                "bootstrap_resamples": args.bootstrap_resamples,
                "bootstrap_seed": args.bootstrap_seed,
                "spark_version": spark.version,
                "timezone": "UTC",
                "features": list(FEATURES),
                "split": "global timestamp quantile; train before cutoff, test at or after",
            },
            "runs": [run._asdict() for run in runs],
        }
        output = json.dumps(result, indent=2, sort_keys=True) + "\n"
        if args.output:
            args.output.write_text(output, encoding="utf-8")
        else:
            print(output, end="")
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
