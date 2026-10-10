from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib


PROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"


def test_dev_extra_uses_httpx_for_fastapi_client() -> None:
    config = tomllib.loads(PROJECT.read_text(encoding="utf-8"))
    dev = config["project"]["optional-dependencies"]["dev"]

    assert any(item.startswith("httpx>=") for item in dev)
    assert not any(item.startswith("httpx2") for item in dev)


def test_als_extra_declares_pyspark() -> None:
    config = tomllib.loads(PROJECT.read_text(encoding="utf-8"))
    als = config["project"]["optional-dependencies"]["als"]

    assert any(item.startswith("pyspark>=") for item in als)
