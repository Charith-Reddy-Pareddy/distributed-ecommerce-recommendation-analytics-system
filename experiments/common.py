"""Shared helper for recording experiment results as one JSON record per run.

Every experiment script calls record_result() instead of hand-rolling its
own output format, so every result -- recommendation quality, throughput,
optimizer impact, fault tolerance -- is reproducible from the same fields:
what ran, on what data, on what hardware, and when.
"""

import json
import platform
from datetime import datetime, timezone
from pathlib import Path


def record_result(results_dir, name, config, dataset, model, metric, result):
    """Append one JSON result record to <results_dir>/<name>.jsonl."""
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "name": name,
        "config": config,
        "dataset": dataset,
        "model": model,
        "metric": metric,
        "result": result,
        "hardware": _hardware_info(),
    }
    results_dir = Path(results_dir)
    results_dir.mkdir(parents=True, exist_ok=True)
    path = results_dir / f"{name}.jsonl"
    with path.open("a") as f:
        f.write(json.dumps(record) + "\n")
    return record


def _hardware_info():
    return {
        "platform": platform.platform(),
        "processor": platform.processor(),
        "python_version": platform.python_version(),
    }
