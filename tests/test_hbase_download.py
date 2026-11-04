import os
import subprocess
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "docker" / "hbase" / "download-release.sh"


def test_download_retries_then_uses_next_mirror(tmp_path: Path) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    curl = bin_dir / "curl"
    curl.write_text(
        """#!/bin/sh
args=$*
output=
while [ "$#" -gt 0 ]; do
    if [ "$1" = --output ]; then
        output=$2
        shift 2
    else
        shift
    fi
done
printf '%s\\n' "$args" >> "$CURL_LOG"
case "$args" in
    *dlcdn.apache.org*) printf partial >> "$output"; exit 28 ;;
    *) printf archive > "$output" ;;
esac
""",
        encoding="utf-8",
    )
    curl.chmod(0o755)
    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    env["CURL_LOG"] = str(tmp_path / "curl.log")
    target = tmp_path / "download"

    result = subprocess.run(
        ["sh", str(SCRIPT), str(target), "https://dlcdn.apache.org/release", "https://apache.uib.no/release"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    calls = Path(env["CURL_LOG"]).read_text(encoding="utf-8").splitlines()
    assert result.returncode == 0, result.stderr
    assert target.read_text(encoding="utf-8") == "archive"
    assert not Path(f"{target}.part").exists()
    assert sum("dlcdn.apache.org" in call for call in calls) == 2
    assert sum("apache.uib.no" in call for call in calls) == 1


def test_download_cleans_partial_files_after_all_mirrors_fail(tmp_path: Path) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    curl = bin_dir / "curl"
    curl.write_text(
        """#!/bin/sh
while [ "$#" -gt 0 ]; do
    if [ "$1" = --output ]; then
        output=$2
        shift 2
    else
        shift
    fi
done
printf partial >> "$output"
exit 28
""",
        encoding="utf-8",
    )
    curl.chmod(0o755)
    env = os.environ.copy()
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    target = tmp_path / "download"

    result = subprocess.run(
        ["sh", str(SCRIPT), str(target), "https://mirror-one/release", "https://mirror-two/release"],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1
    assert not target.exists()
    assert not Path(f"{target}.part").exists()
