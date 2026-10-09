import base64
import json

import pytest

from ecommerce_analytics.hbase_recommendations import HBaseRecommendations


def dec(value: str) -> str:
    return base64.b64decode(value).decode("utf-8")


def test_row_encodes_recommendations() -> None:
    row = HBaseRecommendations._row(
        "customer 7", [{"product_id": 17, "score": 0.625}]
    )

    assert dec(row["key"]) == "customer 7"
    assert [(dec(cell["column"]), dec(cell["$"])) for cell in row["Cell"]] == [
        ("rec:count", "1"),
        ("rec:item_00", "17"),
        ("rec:score_00", "0.625"),
    ]


@pytest.mark.parametrize(
    "rec, message",
    [
        ({"product_id": 0, "score": 1}, "positive integers"),
        ({"product_id": 3, "score": float("nan")}, "finite"),
    ],
)
def test_rejects_invalid_recommendations(rec, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        HBaseRecommendations._row("user", [rec])


def test_loads_spark_parts_in_batches(tmp_path) -> None:
    (tmp_path / "part-00000.json").write_text(
        "\n".join(
            json.dumps(
                {"user_id": f"u{i}", "recommendations": [{"product_id": i + 1, "score": 1.0}]}
            )
            for i in range(3)
        ),
        encoding="utf-8",
    )

    class Store(HBaseRecommendations):
        def __init__(self):
            super().__init__("http://hbase")
            self.batches = []
            self.ready = False

        def ensure_table(self):
            self.ready = True

        def write_batch(self, rows):
            assert self.ready
            self.batches.append([row["user_id"] for row in rows])

    store = Store()

    assert store.load_directory(tmp_path, batch_size=2) == 3
    assert store.batches == [["u0", "u1"], ["u2"]]


def test_requires_spark_part_files(tmp_path) -> None:
    with pytest.raises(ValueError, match="part files"):
        HBaseRecommendations("http://hbase").load_directory(tmp_path)


def test_reads_only_current_recommendation_count() -> None:
    class Store(HBaseRecommendations):
        def __init__(self):
            super().__init__("http://hbase")

        def _request(self, method, path, body=None, allow_404=False):
            assert method == "GET"
            assert path == "als_recommendations/user%20one"
            return {
                "Row": [
                    {
                        "Cell": [
                            {
                                "column": base64.b64encode(b"rec:count").decode(),
                                "$": base64.b64encode(b"1").decode(),
                            },
                            {
                                "column": base64.b64encode(b"rec:item_00").decode(),
                                "$": base64.b64encode(b"17").decode(),
                            },
                            {
                                "column": base64.b64encode(b"rec:score_00").decode(),
                                "$": base64.b64encode(b"0.625").decode(),
                            },
                            {
                                "column": base64.b64encode(b"rec:item_01").decode(),
                                "$": base64.b64encode(b"23").decode(),
                            },
                        ]
                    }
                ]
            }

    assert Store().get("user one") == [{"product_id": 17, "score": 0.625}]
