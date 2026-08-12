"""One-shot job: loads the ALS top-10-recommendations table from HDFS
into HBase for low-latency lookups.

Uses HBase's REST server (Stargate) over plain HTTP, keeping this a
lightweight Python service like the rest of the project.
"""
import base64
import json
import os
from collections import defaultdict

import requests
from hdfs import InsecureClient

WEBHDFS_URL = os.getenv("WEBHDFS_URL", "http://hdfs-namenode:9870")
HBASE_REST_URL = os.getenv("HBASE_REST_URL", "http://hbase-rest:8080")
RECS_PATH = "/output/als-recommendations"
TABLE_NAME = "als_recommendations"
COLUMN_FAMILY = "rec"
BATCH_SIZE = 200


def _b64(s: str) -> str:
    return base64.b64encode(s.encode("utf-8")).decode("ascii")


def load_recommendations_from_hdfs() -> dict[int, list[tuple[int, float]]]:
    client = InsecureClient(WEBHDFS_URL, user="root")
    by_user: dict[int, list[tuple[int, float]]] = defaultdict(list)

    for filename in client.list(RECS_PATH):
        if not filename.endswith(".json"):
            continue
        with client.read(f"{RECS_PATH}/{filename}", encoding="utf-8") as reader:
            for line in reader:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                by_user[row["visitorid"]].append((row["itemid"], row["score"]))

    for user_id, recs in by_user.items():
        recs.sort(key=lambda pair: pair[1], reverse=True)

    return by_user


def create_table_if_missing() -> None:
    resp = requests.get(f"{HBASE_REST_URL}/{TABLE_NAME}/schema", timeout=10)
    if resp.status_code == 200:
        return

    schema = {"name": TABLE_NAME, "ColumnSchema": [{"name": COLUMN_FAMILY}]}
    resp = requests.post(
        f"{HBASE_REST_URL}/{TABLE_NAME}/schema",
        json=schema,
        headers={"Content-Type": "application/json"},
        timeout=10,
    )
    resp.raise_for_status()
    print(f"Created table {TABLE_NAME}", flush=True)


def _row_payload(user_id: int, recs: list[tuple[int, float]]) -> dict:
    cells = []
    for rank, (item_id, score) in enumerate(recs[:10]):
        cells.append(
            {"column": _b64(f"{COLUMN_FAMILY}:item_{rank:02d}"), "$": _b64(str(item_id))}
        )
        cells.append(
            {"column": _b64(f"{COLUMN_FAMILY}:score_{rank:02d}"), "$": _b64(f"{score:.6f}")}
        )
    return {"key": _b64(str(user_id)), "Cell": cells}


def write_batch(rows: list[dict]) -> None:
    payload = {"Row": rows}
    resp = requests.post(
        f"{HBASE_REST_URL}/{TABLE_NAME}/false-row-key",
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()


def main() -> None:
    print("Reading ALS recommendations from HDFS...", flush=True)
    by_user = load_recommendations_from_hdfs()
    print(f"Loaded recommendations for {len(by_user)} users", flush=True)

    create_table_if_missing()

    batch: list[dict] = []
    written = 0
    for user_id, recs in by_user.items():
        batch.append(_row_payload(user_id, recs))
        if len(batch) >= BATCH_SIZE:
            write_batch(batch)
            written += len(batch)
            print(f"Wrote {written}/{len(by_user)} rows", flush=True)
            batch = []

    if batch:
        write_batch(batch)
        written += len(batch)

    print(f"Done. Wrote {written} rows to HBase table {TABLE_NAME}.", flush=True)


if __name__ == "__main__":
    main()
