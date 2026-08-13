"""``_spot_failure_reason`` must read the verdict from ``--json``, not $?.

THE MIGRATION

``krepis.ec2_spot relaunch-decision`` grew ``--json`` in krepis-PR133
(released 0.51.0; latest tag v0.54.0). With ``--json`` the verdict is a JSON
field (``relaunch``) on stdout, and the CLI exits 0 whenever it reached ANY
decision — hold included. A non-zero exit now means only "the CLI could not
answer", never a verdict.

This replaces the exit-code contract this repo's two copies of
``_spot_failure_reason`` previously read (``NO_RELAUNCH_EXIT_CODE`` = 75 for
hold, 0 for relaunch), which required a load-bearing ``|| _decide_rc=$?``
guard at the call site to survive ``set -e`` (alpha-engine-config-I6996).
That guard is gone: the CLI call itself now always adds ``--json`` and always
exits 0 on a real verdict, so there is nothing for errexit to trip over.

THE FUNCTION'S OWN CONTRACT IS UNCHANGED

``_spot_failure_reason`` still signals via ITS OWN return status — 1 means
"not a confirmed reclaim, do not relaunch", matching the pre-existing
caller convention (``reason="$(_spot_failure_reason "$rc")" || reason=""``).
Only the CLI call *inside* the function changed.

WHAT THIS TEST COVERS

1. The CLI answers with a JSON verdict (exit 0, ``{"relaunch": false, ...}``)
   — a hold. The function must return 1 (not a reclaim) and must not print
   ``confirmed-reclaim``.
2. The CLI cannot answer at all (non-zero exit, no verdict) — a CLI failure,
   not a "hold" verdict. The function must ALSO return 1: per I7009, a
   non-zero exit with ``--json`` means only "could not answer," so the safe
   default is to treat it as hold (do not relaunch), never to relaunch on an
   ambiguous signal.

``test_json_hold_verdict_is_not_a_reclaim`` fails against the pre-migration
file (``origin/main``, before either alpha-engine-config-I6996's guard or
this --json migration): the old code never passes ``--json`` and keys purely
on the CLI's exit status, so an exit-0 stub reads as a confirmed reclaim
regardless of the JSON body — observed by running this file against a
worktree checked out at ``origin/main``.
``test_cli_failure_is_treated_as_hold_not_a_verdict`` passes against BOTH
the old and new code (a non-zero exit reads as "not a reclaim" under either
contract) — it is a regression test for the new contract, not a
discriminator between the two.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

_INFRA = Path(__file__).resolve().parent.parent / "infrastructure"

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


def _run(script_name: str, instance_var: str, stub_body: str, tmp_path: Path) -> subprocess.CompletedProcess:
    script = _INFRA / script_name
    lifted = _function_text(script.read_text(), "_spot_failure_reason")

    fake_python = tmp_path / f"fake-python-{script_name}"
    fake_python.write_text("#!/usr/bin/env bash\n" + stub_body)
    fake_python.chmod(0o755)

    harness = tmp_path / f"harness-{script_name}.sh"
    harness.write_text(
        "#!/usr/bin/env bash\n"
        "set -euo pipefail\n"
        f"{lifted}\n"
        "AWS_REGION=us-east-1\n"
        f"{instance_var}=i-0000000000test0000\n"
        "MAX_RUNTIME_SECONDS=5400\n"
        "SF_EXECUTION_TIMEOUT=''\n"
        "SPOT_ATTEMPT=1\n"
        "MAX_SPOT_ATTEMPTS=2\n"
        f"LIB_PYTHON={fake_python}\n"
        # rc=3 — a workload failure, not the launch-capacity 64 shortcut.
        "_spot_failure_reason 3\n"
        "echo UNREACHABLE_NOT_A_RECLAIM\n"
    )
    harness.chmod(0o755)

    return subprocess.run(
        ["bash", str(harness)],
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin:/usr/sbin:/sbin", "HOME": str(tmp_path)},
        timeout=60,
    )


@pytest.mark.parametrize(
    ("script_name", "instance_var"),
    _LAUNCHERS,
    ids=[s for s, _ in _LAUNCHERS],
)
def test_json_hold_verdict_is_not_a_reclaim(
    script_name: str, instance_var: str, tmp_path: Path
) -> None:
    """CLI answers (exit 0, --json), verdict is hold -> function returns 1."""
    # Stand-in for $LIB_PYTHON invoked twice: once as `-m krepis.ec2_spot
    # relaunch-decision ... --json` (prints the JSON verdict, exits 0), once
    # as `-c '...'` (the real interpreter reading that JSON from stdin).
    stub = (
        'if [ "$1" = "-c" ]; then exec /usr/bin/python3 "$@"; fi\n'
        "echo '{\"relaunch\": false, \"reason\": \"not-reclaim:other\"}'\n"
        "exit 0\n"
    )
    proc = _run(script_name, instance_var, stub, tmp_path)

    assert proc.returncode != 0, (
        f"{script_name}: a held verdict must not read as a reclaim\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    assert "confirmed-reclaim" not in proc.stdout, proc.stdout
    assert "UNREACHABLE_NOT_A_RECLAIM" not in proc.stdout, proc.stdout
    assert '"relaunch": false' in proc.stderr, (
        f"{script_name}: diagnostic line did not carry the JSON verdict\n"
        f"stderr:\n{proc.stderr}"
    )


@pytest.mark.parametrize(
    ("script_name", "instance_var"),
    _LAUNCHERS,
    ids=[s for s, _ in _LAUNCHERS],
)
def test_cli_failure_is_treated_as_hold_not_a_verdict(
    script_name: str, instance_var: str, tmp_path: Path
) -> None:
    """CLI cannot answer (non-zero exit) -> function returns 1, no crash.

    Per I7009: with --json, a non-zero exit means only "the CLI could not
    answer" — never a verdict. The function must not relaunch on that
    ambiguous signal, and must not raise (errexit-safe on its own terms —
    there is no caller-side `||` to hide behind in this harness).
    """
    stub = "echo 'not json' >&2\nexit 1\n"
    proc = _run(script_name, instance_var, stub, tmp_path)

    assert proc.returncode != 0, (
        f"{script_name}: a CLI failure must not read as a reclaim\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    assert "confirmed-reclaim" not in proc.stdout, proc.stdout
    assert "UNREACHABLE_NOT_A_RECLAIM" not in proc.stdout, proc.stdout
