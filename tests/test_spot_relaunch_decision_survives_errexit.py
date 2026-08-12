"""``_spot_failure_reason`` must survive the CLI's ordinary "hold" answer.

THE DEFECT CLASS

``krepis.ec2_spot relaunch-decision`` signals its verdict by EXIT CODE — ``0``
= relaunch, ``NO_RELAUNCH_EXIT_CODE`` (75) = hold. Hold is the designed answer
for every failure that is *not* an AWS spot reclaim, i.e. nearly all of them.
Every launcher in the fleet called it as::

    _decide_out="$(... relaunch-decision ... )"
    _decide_rc=$?

under ``set -e``. An assignment fed by a command substitution is a simple
command whose exit status IS the substitution's, so the ordinary answer is an
errexit trigger, and ``_decide_rc=$?`` — the line that exists to READ that
status — is unreachable.

WHAT THIS REPO'S COPIES ACTUALLY DID, MEASURED

Not the same thing the other three repos did, and the difference is worth
pinning rather than assuming. In ``crucible-predictor``, ``crucible-backtester``
and ``crucible-dashboard`` the call sits directly in the body of the EXIT trap,
so errexit destroyed the shell mid-trap, ``terminate-instances`` was never
reached, and the spot instance leaked — silently, because ``set -e`` does not
re-enter a trap it is already running. Confirmed on
``ne-weekly-freshness-pipeline/watch-rerun-2026-08-10-9`` (2026-08-11) via
CloudTrail: no ``TerminateInstances`` for ``i-092854cbb6e62b753``.

Here the call sits inside ``_spot_failure_reason``, whose sole caller writes::

    reason="$(_spot_failure_reason "$rc")" || reason=""

Bash suppresses errexit for the entire dynamic extent of a command on the left
of ``||``, *including inside functions it invokes*. Measured 2026-08-12 with a
harness reproducing both shapes: this one completes, returns 1, and cleanup
runs. **No instance was leaked from this repo.**

WHY IT IS STILL FIXED, AND WHY THIS TEST EXISTS

The correctness of these two functions was a property of one punctuation mark
at one call site, not of the functions themselves. Deleting that ``||``, or
adding a second caller without it, converts this into the live defect the other
three repos had — and nothing would announce it, because the only evidence is
an absent CloudTrail event.

So the test deliberately calls the function in an errexit-ACTIVE context, which
is precisely the context the ``||`` was hiding. With the guard, the function
reaches its diagnostic line and returns 1 (correctly: hold means do not relaunch).
Without it, the function aborts at the substitution and the diagnostic never
appears — which is exactly what the assertion looks for.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

_INFRA = Path(__file__).resolve().parent.parent / "infrastructure"

#: The CLI's documented "do not relaunch" status.
_NO_RELAUNCH_EXIT_CODE = 75

#: (script, instance-id variable read by that copy of the function).
_LAUNCHERS = (
    ("_spot_common.sh", "_INSTANCE_ID"),
    ("spot_data_weekly.sh", "INSTANCE_ID"),
)


def _function_text(source: str, name: str) -> str:
    """Return a shell function's full text, brace-matched."""
    marker = "\n" + name + "() {"
    assert marker in source, f"{name}() not found"
    start = source.index(marker) + 1
    depth = 0
    for idx in range(start, len(source)):
        if source[idx] == "{":
            depth += 1
        elif source[idx] == "}":
            depth -= 1
            if depth == 0:
                return source[start : idx + 1]
    raise AssertionError(f"unbalanced braces in {name}()")


@pytest.fixture(autouse=True)
def _requires_bash():
    if shutil.which("bash") is None:  # pragma: no cover - bash is a hard dep
        pytest.skip("bash unavailable")


@pytest.mark.parametrize(
    ("script_name", "instance_var"),
    _LAUNCHERS,
    ids=[s for s, _ in _LAUNCHERS],
)
def test_hold_verdict_is_read_not_fatal(
    script_name: str, instance_var: str, tmp_path: Path
) -> None:
    script = _INFRA / script_name
    lifted = _function_text(script.read_text(), "_spot_failure_reason")

    # Stand-in for $LIB_PYTHON: answers "hold" exactly as the real CLI does —
    # the TAB-separated line on stdout, then the no-relaunch exit status.
    hold_python = tmp_path / "hold-python"
    hold_python.write_text(
        "#!/usr/bin/env bash\n"
        "printf 'hold\\tnot-reclaim:other\\tother\\t1\\n'\n"
        f"exit {_NO_RELAUNCH_EXIT_CODE}\n"
    )
    hold_python.chmod(0o755)

    harness = tmp_path / "harness.sh"
    harness.write_text(
        "#!/usr/bin/env bash\n"
        # errexit ACTIVE at the call — no `|| reason=""` to hide behind. This
        # is the whole point: the function must be correct on its own terms.
        "set -euo pipefail\n"
        f"{lifted}\n"
        "AWS_REGION=us-east-1\n"
        f"{instance_var}=i-0000000000test0000\n"
        "MAX_RUNTIME_SECONDS=5400\n"
        "SF_EXECUTION_TIMEOUT=''\n"
        "SPOT_ATTEMPT=1\n"
        "MAX_SPOT_ATTEMPTS=2\n"
        f"LIB_PYTHON={hold_python}\n"
        # rc=3 — a workload failure, not the launch-capacity 64 shortcut.
        "_spot_failure_reason 3\n"
        "echo UNREACHABLE_NOT_A_RECLAIM\n"
    )
    harness.chmod(0o755)

    proc = subprocess.run(
        ["bash", str(harness)],
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "HOME": str(tmp_path)},
        timeout=60,
    )

    assert f"rc={_NO_RELAUNCH_EXIT_CODE}" in proc.stderr, (
        f"{script_name}: _spot_failure_reason aborted at the command "
        "substitution instead of reading the CLI's hold verdict — its "
        "diagnostic line never ran. In an EXIT-trap context this is a silently "
        "skipped terminate-instances and a leaked spot instance.\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )

    # Hold means "not a reclaim": the function must return non-zero so the
    # caller does NOT relaunch, and must NOT claim a confirmed reclaim.
    assert proc.returncode != 0, "a held verdict must not read as a reclaim"
    assert "confirmed-reclaim" not in proc.stdout, proc.stdout
    assert "UNREACHABLE_NOT_A_RECLAIM" not in proc.stdout, proc.stdout
