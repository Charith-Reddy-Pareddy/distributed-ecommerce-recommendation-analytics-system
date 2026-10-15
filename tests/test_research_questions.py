import csv
import os
from pathlib import Path

import pytest


def test_research_brief_matches_real_amazon_schema() -> None:
    path = os.environ.get("AMAZON_REVIEWS_PATH")
    if not path or not Path(path).is_file():
        pytest.skip("set AMAZON_REVIEWS_PATH to the real Amazon review CSV")

    with Path(path).open(newline="", encoding="utf-8") as stream:
        fields = set(next(csv.reader(stream)))

    brief = Path("docs/research_questions.md").read_text(encoding="utf-8")
    assert {"user_id", "asin", "rating", "timestamp"} <= fields
    assert "How are submitted ratings distributed?" in brief
    assert "How do rating volume and average rating vary by product?" in brief
    assert "Do ALS recommendations improve on a popularity baseline" in brief
    assert "not a result" in brief


def test_missing_event_sources_are_marked_unavailable() -> None:
    brief = Path("docs/research_questions.md").read_text(encoding="utf-8")
    source_note = Path("docs/dashboard-data-sources.md").read_text(encoding="utf-8")

    assert "Event questions currently blocked" in brief
    assert "not present" in brief
    assert "did not contain RetailRocket files" in source_note
    assert "does not provide global event aggregates" in source_note
