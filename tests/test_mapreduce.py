# Claude was used to suggest a few edge cases for this test module.
# The final tests were reviewed and adapted to the implementation.
import importlib.util
import io
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load(name):
    path = REPO_ROOT / "jobs" / "product-popularity" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


mapper = _load("mapper")
reducer = _load("reducer")


def run_with_stdin(module, input_text):
    old_stdin, old_stdout = sys.stdin, sys.stdout
    sys.stdin = io.StringIO(input_text)
    sys.stdout = io.StringIO()
    try:
        module.main()
        return sys.stdout.getvalue()
    finally:
        sys.stdin, sys.stdout = old_stdin, old_stdout


def test_mapper_emits_correct_weight_per_event_type():
    events = "\n".join(
        [
            '{"product_id": 1, "event_type": "view"}',
            '{"product_id": 1, "event_type": "add_to_cart"}',
            '{"product_id": 1, "event_type": "purchase"}',
        ]
    )
    output = run_with_stdin(mapper, events)
    assert output == "1\t1\n1\t3\n1\t5\n"


def test_mapper_skips_malformed_json():
    output = run_with_stdin(mapper, "not json\n" + '{"product_id": 1, "event_type": "view"}')
    assert output == "1\t1\n"


def test_mapper_skips_blank_lines():
    output = run_with_stdin(mapper, "\n\n" + '{"product_id": 1, "event_type": "view"}' + "\n\n")
    assert output == "1\t1\n"


def test_mapper_skips_unknown_event_type():
    output = run_with_stdin(mapper, '{"product_id": 1, "event_type": "click"}')
    assert output == ""


def test_mapper_skips_missing_product_id():
    output = run_with_stdin(mapper, '{"event_type": "view"}')
    assert output == ""


def test_reducer_sums_consecutive_same_product():
    output = run_with_stdin(reducer, "1\t1\n1\t3\n1\t5\n")
    assert output == "1\t9\n"


def test_reducer_emits_separate_totals_per_product():
    output = run_with_stdin(reducer, "1\t1\n1\t3\n2\t5\n2\t5\n")
    assert output == "1\t4\n2\t10\n"


def test_reducer_flushes_the_final_product_after_the_loop():
    # The trickiest part of the compare-to-previous-key pattern: the
    # last group has no following different key to trigger a flush,
    # so it has to be printed once the loop ends.
    output = run_with_stdin(reducer, "1\t1\n2\t5\n")
    assert output == "1\t1\n2\t5\n"


def test_reducer_empty_input_produces_no_output():
    assert run_with_stdin(reducer, "") == ""


def test_reducer_single_line():
    assert run_with_stdin(reducer, "7\t2\n") == "7\t2\n"
