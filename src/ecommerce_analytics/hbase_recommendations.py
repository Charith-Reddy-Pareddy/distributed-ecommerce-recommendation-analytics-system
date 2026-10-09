"""Read and load precomputed recommendations through HBase REST."""

import argparse
import base64
import json
import math
import os
import re
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen


def _b64(value: str) -> str:
    return base64.b64encode(value.encode("utf-8")).decode("ascii")


def _unb64(value: str) -> str:
    return base64.b64decode(value).decode("utf-8")


class HBaseRecommendations:
    def __init__(
        self,
        base_url: str | None = None,
        table: str = "als_recommendations",
        timeout: float = 10,
    ) -> None:
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", table):
            raise ValueError("table must be a valid HBase table name")
        self._base_url = (
            base_url or os.getenv("HBASE_REST_URL", "http://localhost:8080")
        ).rstrip("/")
        self._table = table
        self._timeout = timeout

    def _request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        allow_404: bool = False,
    ) -> dict[str, Any] | None:
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = Request(
            f"{self._base_url}/{path.lstrip('/')}",
            data=data,
            method=method,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
        )
        try:
            with urlopen(req, timeout=self._timeout) as response:
                if response.status == 204:
                    return {}
                body = response.read()
                return json.loads(body.decode("utf-8")) if body else {}
        except HTTPError as exc:
            if allow_404 and exc.code == 404:
                return None
            raise

    def ensure_table(self) -> None:
        path = f"{self._table}/schema"
        if self._request("GET", path, allow_404=True) is not None:
            return
        self._request(
            "PUT",
            path,
            {"name": self._table, "ColumnSchema": [{"name": "rec"}]},
        )

    @staticmethod
    def _row(user_id: str, recommendations: list[dict[str, Any]]) -> dict[str, Any]:
        if not user_id:
            raise ValueError("user_id must not be empty")
        recs = recommendations[:100]
        cells = [
            {"column": _b64("rec:count"), "$": _b64(str(len(recs)))}
        ]
        for rank, rec in enumerate(recs):
            product_id = rec.get("product_id")
            score = float(rec.get("score"))
            if type(product_id) is not int or product_id <= 0:
                raise ValueError("recommendation product IDs must be positive integers")
            if not math.isfinite(score):
                raise ValueError("recommendation scores must be finite")
            cells.extend(
                [
                    {
                        "column": _b64(f"rec:item_{rank:02d}"),
                        "$": _b64(str(product_id)),
                    },
                    {
                        "column": _b64(f"rec:score_{rank:02d}"),
                        "$": _b64(f"{score:.8g}"),
                    },
                ]
            )
        return {"key": _b64(user_id), "Cell": cells}

    def write_batch(self, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        payload = {
            "Row": [self._row(row["user_id"], row["recommendations"]) for row in rows]
        }
        self._request("PUT", f"{self._table}/false-row-key", payload)

    def load_directory(self, path: str | Path, batch_size: int = 200) -> int:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        files = sorted(Path(path).glob("part-*.json"))
        if not files:
            raise ValueError("recommendation directory has no Spark JSON part files")

        self.ensure_table()
        batch: list[dict[str, Any]] = []
        written = 0
        for file in files:
            with file.open(encoding="utf-8") as stream:
                for line in stream:
                    if not line.strip():
                        continue
                    batch.append(json.loads(line))
                    if len(batch) == batch_size:
                        self.write_batch(batch)
                        written += len(batch)
                        batch.clear()
        if batch:
            self.write_batch(batch)
            written += len(batch)
        return written

    def get(self, user_id: str) -> list[dict[str, int | float]] | None:
        row = self._request(
            "GET", f"{self._table}/{quote(user_id, safe='')}", allow_404=True
        )
        if row is None:
            return None

        cells: dict[str, str] = {}
        for result in row.get("Row", []):
            for cell in result.get("Cell", []):
                cells[_unb64(cell["column"])] = _unb64(cell["$"])

        count = int(cells.get("rec:count", "100"))
        if count < 0 or count > 100:
            raise ValueError("stored recommendation count is invalid")
        recs = []
        for rank in range(count):
            item = cells.get(f"rec:item_{rank:02d}")
            score = cells.get(f"rec:score_{rank:02d}")
            if item is None or score is None:
                break
            recs.append({"product_id": int(item), "score": float(score)})
        return recs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=200)
    args = parser.parse_args()
    count = HBaseRecommendations().load_directory(args.input, args.batch_size)
    print(f"Loaded recommendations for {count} users into HBase")


if __name__ == "__main__":
    main()
