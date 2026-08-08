"""`--apply-iam` must apply IAM and nothing else.

Bug class — ``narrowing-flag-does-not-narrow``, nous-ergon-ops-I520, 2026-08-08.

The flag was added under config#2825 and documented as "re-apply
iam-policy.json only (no bootstrap side effects)". It set ``APPLY_IAM=true``,
ran the IAM block, and then **fell through** into the code deploy, the Lambda
environment merge, and the registry upload — because the block had no exit.

The registry upload publishes ``private-docs/ARTIFACT_REGISTRY.yaml`` from a
SIBLING repository's local working copy. On 2026-08-08 an operator ran
``--apply-iam`` to add two read grants for alpha-engine-config-I6661, and it
also republished the registry from a checkout four commits behind
``origin/main`` — silently reverting the two ``producer_trigger`` rows merged
an hour earlier (alpha-engine-config-PR6660). Detected by reading the command's
own stdout; nothing in the system would have reported it.

**These tests assert the side effects NOT taken.** That is the countermeasure
the failure-mode class names, and it is what the existing coverage lacked: a
test that ``--apply-iam`` applies IAM passes just as happily on the broken
script. Only "and nothing else happened" distinguishes them.
"""

from __future__ import annotations

import os
import re
import subprocess
import textwrap
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
_DEPLOY = _REPO / "infrastructure" / "lambdas" / "freshness-monitor" / "deploy.sh"
_SRC = _DEPLOY.read_text(encoding="utf-8")
_CODE = "\n".join(
    line for line in _SRC.splitlines() if not line.lstrip().startswith("#")
)


def test_deploy_script_exists():
    assert _DEPLOY.is_file(), f"missing {_DEPLOY}"


def test_the_apply_iam_block_exits():
    """The structural invariant, checked without running anything.

    A narrowing flag's block ends the run. Without this the block is a prefix
    of the full deploy, not an alternative to it.
    """
    match = re.search(r"if \$APPLY_IAM; then(.*?)\nfi\n", _CODE, re.DOTALL)
    assert match, "the --apply-iam block is gone or no longer matches `if $APPLY_IAM; then`"
    body = match.group(1)
    assert re.search(r"^\s*exit 0\s*$", body, re.MULTILINE), (
        "the --apply-iam block does not exit. Control falls through into the "
        "code deploy, the environment merge and the registry upload — the "
        "exact defect recorded as nous-ergon-ops-I520."
    )


def test_apply_iam_runs_before_packaging():
    """"Only" covers the whole run, not just the effects at the end.

    Everything below the packaging step installs pip deps, runs the handler
    suite and builds a ~29MB zip. An IAM re-apply needs none of it, and doing
    it anyway is the same promise broken more cheaply.
    """
    iam_at = _CODE.index("if $APPLY_IAM; then")
    package_at = _CODE.index("lambda_pip_install.sh")
    assert iam_at < package_at, (
        "the --apply-iam block runs after packaging — it should short-circuit "
        "before any pip install, test run or zip is built"
    )


@pytest.mark.parametrize(
    "forbidden,what",
    [
        ("update-function-code", "a code deploy"),
        ("update-function-configuration", "an environment merge"),
        ("ARTIFACT_REGISTRY.yaml", "a registry upload"),
        ("publish-version", "a version publish"),
    ],
)
def test_apply_iam_dry_run_takes_no_other_action(tmp_path, forbidden, what):
    """Behavioural half: run the script for real with a stubbed ``aws`` and
    assert the forbidden operation never appears in its output.

    Hermetic — ``aws`` is a stub on PATH that echoes its arguments, so nothing
    reaches an account, and ``--dry-run`` routes the intended call through the
    script's own ``run`` helper.
    """
    stub_dir = tmp_path / "bin"
    stub_dir.mkdir()
    aws_stub = stub_dir / "aws"
    aws_stub.write_text(textwrap.dedent("""\
        #!/usr/bin/env bash
        echo "AWSCALL: $*"
        # get-role must succeed so the script takes the "role exists" path.
        exit 0
    """))
    aws_stub.chmod(0o755)

    child = dict(os.environ)
    child["PATH"] = f"{stub_dir}:{child['PATH']}"
    child["DRY_RUN"] = "true"

    proc = subprocess.run(
        ["bash", str(_DEPLOY), "--apply-iam", "--dry-run"],
        capture_output=True, text=True, timeout=120, cwd=str(_REPO),
        env=child,
    )
    combined = proc.stdout + proc.stderr

    assert "Applying IAM" in combined, (
        f"--apply-iam did not reach the IAM block at all:\n{combined[-2000:]}"
    )
    assert forbidden not in combined, (
        f"--apply-iam performed {what} ({forbidden!r}) — the flag promises IAM "
        f"and nothing else (nous-ergon-ops-I520):\n{combined[-2000:]}"
    )


def test_registry_upload_refuses_a_stale_source():
    """The second-order half of I520: the upload publishes from a sibling
    repo's mutable working copy, which concurrent sessions routinely leave
    behind ``origin/main``. A publish step sourced from a local checkout is a
    supply chain with no version in it, so it must fail closed.
    """
    assert "REFUSING to upload the registry" in _SRC, (
        "the registry upload no longer guards against a stale ${CONFIG_REPO} — "
        "it would silently republish an older registry over a newer one "
        "(nous-ergon-ops-I520)"
    )
    assert "rev-list --count HEAD..origin/main" in _CODE, (
        "the staleness guard must compare against origin/main, not merely "
        "check that the checkout exists"
    )
