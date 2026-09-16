"""Bridges the catalog-native ALS job's local output (trained offline via
local-mode PySpark, no Docker -- see train.py) into HDFS, so it can be
loaded into HBase the same way the RetailRocket job's Docker-Spark
output already is.

Rewriting train.py to run inside Dockerized Spark just to reach HDFS
would mean re-training there too; this is simpler and faster: train
once locally, then upload the already-computed result. Uses WebHDFS
directly from the host -- hdfs-namenode's port 9870 is already exposed
there (see docker-compose.yml).

Usage (stack must be up):
    pip install hdfs
    python experiments/recommendation/catalog_als/upload_to_hdfs.py
"""
import json
import sys
from pathlib import Path

import pandas as pd
from hdfs import InsecureClient

REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

RECS_PATH = Path(__file__).resolve().parent / "recommendations.parquet"
WEBHDFS_URL = "http://localhost:9870"
HDFS_OUTPUT_PATH = "/output/catalog-als-recommendations"


def main():
    df = pd.read_parquet(RECS_PATH)
    print(f"Loaded {len(df):,} rows for {df['user_id'].nunique():,} users from {RECS_PATH}", flush=True)

    lines = [
        json.dumps({"user_id": int(row.user_id), "product_id": int(row.product_id), "score": float(row.score)})
        for row in df.itertuples(index=False)
    ]
    content = "\n".join(lines) + "\n"

    client = InsecureClient(WEBHDFS_URL, user="root")
    client.makedirs(HDFS_OUTPUT_PATH)
    dest = f"{HDFS_OUTPUT_PATH}/part-00000.jsonl"
    with client.write(dest, encoding="utf-8", overwrite=True) as writer:
        writer.write(content)

    print(f"Uploaded {len(lines):,} recommendation rows to hdfs://{dest}", flush=True)


if __name__ == "__main__":
    main()
