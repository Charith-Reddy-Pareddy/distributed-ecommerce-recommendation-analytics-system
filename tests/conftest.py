"""Loads app/ modules directly from each service's source tree.

Every service's package is named "app", so a plain `import app.model`
would collide once more than one service is under test in the same
process. Each module gets imported under a unique alias instead
(e.g. "recsvc_app"), with that alias registered as a real package
pointing at the service's app/ directory, so relative imports inside
the module (like `from .kafka_consumer import new_consumer`) resolve
normally.
"""
import importlib.util
import sys
import types
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# confluent-kafka is a C extension with no prebuilt wheel for every
# platform (confirmed: no manylinux wheel exists for the pinned
# version). Nothing under test calls it -- recommendation-service's
# model.py imports it transitively via kafka_consumer.py just to
# reference the Consumer class -- so a stub avoids that install risk
# entirely instead of gambling on it working in CI.
if "confluent_kafka" not in sys.modules:
    stub = types.ModuleType("confluent_kafka")
    stub.Consumer = type("Consumer", (), {})
    sys.modules["confluent_kafka"] = stub


def load_app_module(service_dirname: str, module_name: str, alias: str):
    app_dir = REPO_ROOT / "services" / service_dirname / "app"

    if alias not in sys.modules:
        pkg_spec = importlib.util.spec_from_file_location(
            alias, app_dir / "__init__.py", submodule_search_locations=[str(app_dir)]
        )
        pkg = importlib.util.module_from_spec(pkg_spec)
        sys.modules[alias] = pkg
        pkg_spec.loader.exec_module(pkg)

    full_name = f"{alias}.{module_name}"
    if full_name not in sys.modules:
        mod_spec = importlib.util.spec_from_file_location(full_name, app_dir / f"{module_name}.py")
        mod = importlib.util.module_from_spec(mod_spec)
        mod.__package__ = alias
        sys.modules[full_name] = mod
        mod_spec.loader.exec_module(mod)
    return sys.modules[full_name]
