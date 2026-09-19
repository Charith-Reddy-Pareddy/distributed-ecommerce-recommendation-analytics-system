"""Regression test for a real stored-XSS bug found live-auditing this
repo: escapeHtml() in services/dashboard/app/static/dashboard.js used to
round-trip through the DOM (textContent -> innerHTML), which only
escapes &, <, > -- what text-node serialization needs -- not double
quotes. It's used both as text content and inside double-quoted HTML
attribute values (src="${escapeHtml(x)}", href="${escapeHtml(x)}"), so
a value containing a literal `"` could break out of an attribute it was
meant to be safely contained in. product-service's POST /products is
unauthenticated with no validation on `images`/`asin`, so this was
exploitable via a normal API call, confirmed live in a real browser
before the fix (an injected onerror handler and <script> tag both
executed) and confirmed inert after it.

Runs escapeHtml() itself under Node rather than reimplementing its
logic in Python, so this actually exercises the shipped browser code,
not a parallel description of what it's supposed to do. Skips if Node
isn't installed (CI currently doesn't set it up -- see
.github/workflows/ci.yml) rather than failing the whole suite over a
missing optional tool, the same pattern tests/integration uses for a
stack that isn't up.
"""
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

DASHBOARD_JS = Path(__file__).resolve().parent.parent / "services" / "dashboard" / "app" / "static" / "dashboard.js"

pytestmark = pytest.mark.skipif(shutil.which("node") is None, reason="Node.js not installed")


def _extract_escape_html_source() -> str:
    text = DASHBOARD_JS.read_text()
    match = re.search(r"^function escapeHtml\(str\) \{.*?\n\}\n", text, re.DOTALL | re.MULTILINE)
    assert match, "escapeHtml() not found in dashboard.js -- has it been renamed or moved?"
    return match.group(0)


def _escape_html(value: str) -> str:
    script = f"{_extract_escape_html_source()}\nconsole.log(JSON.stringify(escapeHtml({json.dumps(value)})));"
    result = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, f"node failed: {result.stderr}"
    return json.loads(result.stdout)


def test_escapes_double_quotes():
    # The actual bug: the old DOM-round-trip implementation left `"`
    # untouched, which is exactly what lets a value break out of a
    # double-quoted HTML attribute it's interpolated into.
    assert _escape_html('x" onerror="alert(1)" x="')  == 'x&quot; onerror=&quot;alert(1)&quot; x=&quot;'


def test_escapes_angle_brackets_and_ampersand():
    assert _escape_html("<script>alert(1)</script>") == "&lt;script&gt;alert(1)&lt;/script&gt;"
    assert _escape_html("Q&A") == "Q&amp;A"


def test_escapes_single_quotes():
    assert _escape_html("it's") == "it&#39;s"


def test_null_and_non_string_input_handled():
    assert _escape_html(None) == ""


def test_amazon_url_construction_escapes_asin():
    """The other half of the same bug: amazonUrl (built from p.asin) used
    to be interpolated into href="${amazonUrl}" with no escapeHtml call
    at all. Confirms the call site, not just the function in isolation.
    """
    text = DASHBOARD_JS.read_text()
    assert 'href="${escapeHtml(amazonUrl)}"' in text, (
        "amazonUrl is interpolated into an href attribute without escapeHtml -- "
        "this is exactly the stored-XSS pattern this test suite exists to catch"
    )
