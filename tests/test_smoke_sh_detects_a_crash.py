"""`assert_no_function_error` exits non-zero on a crashed handler, and zero
on a clean one — the closes-when demonstration for alpha-engine-config-I7535.

Deliberately does NOT invoke AWS Lambda. CLAUDE.md forbids running --smoke
against production (`pipeline-watchdog` publishes a real alert,
`eod-success-friday-shell-trigger` starts a real Saturday shell run), so the
"break a handler locally" demonstration is done here instead: synthetic
`aws lambda invoke` output — exactly the two shapes AWS emits (invoke stdout
carrying `FunctionError`; response body carrying `errorType`/`errorMessage`)
— fed straight to the shared assertion function. This is a faithful
before/after: before I7535, every deploy.sh discarded this exact stdout and
printed this exact body, then exited 0 regardless.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SMOKE_SH = REPO / "infrastructure" / "lambdas" / "_shared" / "smoke.sh"


def _run_assert(invoke_stdout: str, resp_body: dict, tmp_path: Path) -> subprocess.CompletedProcess:
    resp_file = tmp_path / "resp.json"
    resp_file.write_text(json.dumps(resp_body))
    script = f'''
set -euo pipefail
source "{SMOKE_SH}"
assert_no_function_error '{invoke_stdout}' "{resp_file}"
'''
    return subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )


def test_clean_invoke_exits_zero(tmp_path: Path):
    """BEFORE the crash: a normal successful invoke. No FunctionError, no
    errorType/errorMessage in the body."""
    invoke_stdout = json.dumps({"StatusCode": 200, "ExecutedVersion": "$LATEST"})
    result = _run_assert(invoke_stdout, {"ok": True, "fired": True}, tmp_path)
    assert result.returncode == 0, result.stderr


def test_crashed_handler_exits_nonzero_and_names_the_error(tmp_path: Path):
    """AFTER the crash: the exact shape `aws lambda invoke` emits when a
    Python-runtime handler raises — StatusCode 200 (the INVOCATION
    succeeded), FunctionError: Unhandled on the invoke command's own stdout,
    and an errorType/errorMessage body. Every deploy.sh pre-I7535 printed
    this body and exited 0."""
    invoke_stdout = json.dumps(
        {"StatusCode": 200, "FunctionError": "Unhandled", "ExecutedVersion": "$LATEST"}
    )
    resp_body = {
        "errorType": "KeyError",
        "errorMessage": "'FUNCTION_NAME'",
        "requestId": "smoke-test-crash-demo",
        "stackTrace": ["  File \"/var/task/index.py\", line 12, in handler"],
    }
    result = _run_assert(invoke_stdout, resp_body, tmp_path)
    assert result.returncode == 1, (
        f"expected non-zero exit on a crashed handler, got 0. stdout={result.stdout!r}"
    )
    assert "KeyError" in result.stderr
    assert "SMOKE FAILED" in result.stderr


def test_crash_signalled_only_by_body_still_caught(tmp_path: Path):
    """The invoke stdout can be a clean 200 with no FunctionError while the
    BODY still carries errorType (observed shape for some runtimes/error
    paths) — both signals are independently sufficient."""
    invoke_stdout = json.dumps({"StatusCode": 200})
    resp_body = {"errorType": "ValueError", "errorMessage": "boom"}
    result = _run_assert(invoke_stdout, resp_body, tmp_path)
    assert result.returncode == 1
    assert "ValueError" in result.stderr
