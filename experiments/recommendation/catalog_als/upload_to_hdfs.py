"""Bridges the catalog-native ALS job's local output (trained offline via
local-mode PySpark, no Docker -- see train.py) into HDFS, so it can be
loaded into HBase the same way the RetailRocket job's Docker-Spark
output already is.

Rewriting train.py to run inside Dockerized Spark just to reach HDFS
would mean re-training there too; this is simpler and faster: train
once locally, then upload the already-computed result.

Uses `docker cp` + `hdfs dfs -put` run inside the namenode container --
the same pattern docs/RUNNING_LOCALLY.md already uses for the
RetailRocket dataset -- rather than WebHDFS from the host: a WebHDFS
write redirects to the datanode's *internal* Docker hostname, which
the host can't resolve, so a host-side HDFS client can't actually
write data (only the initial request reaches the namenode; confirmed
by hitting a NameResolutionError for the datanode's container id).

Usage (stack must be up):
    python experiments/recommendation/catalog_als/upload_to_hdfs.py
"""
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

RECS_PATH = Path(__file__).resolve().parent / "recommendations.parquet"
LOCAL_JSONL_PATH = Path(__file__).resolve().parent / "recommendations.jsonl"
NAMENODE_CONTAINER = "distributede-commercerecommendationanalyticssystem-hdfs-namenode-1"
HDFS_OUTPUT_PATH = "/output/catalog-als-recommendations"


def main():
    df = pd.read_parquet(RECS_PATH)
    print(f"Loaded {len(df):,} rows for {df['user_id'].nunique():,} users from {RECS_PATH}", flush=True)

    lines = [
        json.dumps({"user_id": int(row.user_id), "product_id": int(row.product_id), "score": float(row.score)})
        for row in df.itertuples(index=False)
    ]
    LOCAL_JSONL_PATH.write_text("\n".join(lines) + "\n")
    print(f"Wrote {len(lines):,} lines to {LOCAL_JSONL_PATH}", flush=True)

    subprocess.run(
        ["docker", "cp", str(LOCAL_JSONL_PATH), f"{NAMENODE_CONTAINER}:/tmp/catalog-als-recommendations.jsonl"],
        cwd=REPO_ROOT, check=True,
    )
    subprocess.run(
        [
            "docker", "compose", "exec", "-T", "hdfs-namenode", "sh", "-c",
            f"hdfs dfs -mkdir -p {HDFS_OUTPUT_PATH} && "
            f"hdfs dfs -put -f /tmp/catalog-als-recommendations.jsonl {HDFS_OUTPUT_PATH}/part-00000.jsonl",
        ],
        cwd=REPO_ROOT, check=True,
    )
    print(f"Uploaded {len(lines):,} recommendation rows to hdfs://{HDFS_OUTPUT_PATH}/part-00000.jsonl", flush=True)


if __name__ == "__main__":
    main()
