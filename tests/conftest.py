"""Makes the shared app-module loader available to every test file."""
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.load_app_module import load_app_module  # noqa: E402,F401
