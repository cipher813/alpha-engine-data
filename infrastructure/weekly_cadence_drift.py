#!/usr/bin/env python3
"""weekly_cadence_drift.py — two-directional check between the declared
weekly-exercise cadence (``infrastructure/weekly_cadence.json``) and its live
copy at SSM Parameter Store ``/alpha-engine/weekly-sf/exercise-cadence``.

**Background (alpha-engine-config#6689 deliverable 2).** Before this file, the
weekly pipeline's exercise cadence lived on TWO uncoordinated surfaces (an SF
Choice hardcoded in ``step_function_eod.json``, no declared parameter
anywhere) — flipping daily<->weekly meant an SF-topology edit. The manifest is
now the single declared source; ``infrastructure/deploy-infrastructure.sh``
writes it to SSM in the same step that updates the postclose SF definition
(config#6689 deliverable 2), and ``infrastructure/step_function_eod.json``'s
``ReadExerciseCadence`` task reads the SSM copy live at execution time. This
script is the mirror of ``automation_pause.py``'s two-directional check for
that pair: a manifest value nobody deployed is as much a bug as a live value
nobody declared.

Two-directional by design, same rationale as ``automation_pause.py``: a drift
that silently reverted the deployed cadence and a manifest edit that was never
actually deployed are both findings — a check that can never fail either
direction is a comment, not a record.

Usage:
  ./infrastructure/weekly_cadence_drift.py --check     # compare manifest vs live SSM; exit 1 on drift
  ./infrastructure/weekly_cadence_drift.py --enforce    # write the manifest value to SSM
  ./infrastructure/weekly_cadence_drift.py --check --json

``--check`` needs ``ssm:GetParameter`` on
``arn:aws:ssm:us-east-1:711398986525:parameter/alpha-engine/weekly-sf/exercise-cadence``;
``--enforce`` additionally needs ``ssm:PutParameter`` on the same resource.
NEITHER grant exists yet on any of the three identities that touch this
parameter (the GitHub Actions deploy role, the SF execution role, and the IAM
drift-check role) — measured 2026-08-09 by grepping
``nous-ergon-ops/infrastructure/iam/`` for ``ssm:GetParameter`` /
``ssm:PutParameter`` against ``nousergon-data``. Until a nous-ergon-ops IAM PR
adds them, both ``--check`` and ``--enforce`` fail loud with the AWS
AccessDenied error rather than silently reporting success — see this repo's
PR body for the exact grants needed. IAM is owned by nous-ergon-ops
(infrastructure-ownership-policy.md §35); this repo does not write IAM.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

MANIFEST = Path(__file__).parent.resolve() / "weekly_cadence.json"
SSM_PARAM_NAME = "/alpha-engine/weekly-sf/exercise-cadence"
REGION = "us-east-1"
ALLOWED_VALUES = {"daily", "weekly-only", "off"}


def load_manifest(path: Path = MANIFEST) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def declared_cadence(manifest: dict | None = None) -> str:
    m = manifest if manifest is not None else load_manifest()
    value = m.get("exercise_cadence")
    if value not in ALLOWED_VALUES:
        raise ValueError(
            f"infrastructure/weekly_cadence.json exercise_cadence={value!r} is not one of "
            f"{sorted(ALLOWED_VALUES)}"
        )
    return value


def _aws(args: list[str]) -> tuple[int, str, str]:
    proc = subprocess.run(
        ["aws"] + args + ["--region", REGION], capture_output=True, text=True, check=False
    )
    return proc.returncode, proc.stdout.strip(), proc.stderr.strip()


def _live_value() -> str | None:
    """Return the live SSM parameter value, or None if it does not exist.

    Any failure that is NOT a genuine not-found (including AccessDenied) is
    raised — a permissions error read as "not deployed yet" would let this
    check grade itself green by losing its own access, the same failure mode
    ``automation_pause.py``'s ``_live_state`` guards against.
    """
    rc, out, err = _aws(
        ["ssm", "get-parameter", "--name", SSM_PARAM_NAME, "--query", "Parameter.Value", "--output", "text"]
    )
    if rc != 0:
        if "ParameterNotFound" in err:
            return None
        raise RuntimeError(f"aws ssm get-parameter --name {SSM_PARAM_NAME}: {err}")
    return out


def check() -> list[dict]:
    declared = declared_cadence()
    live = _live_value()
    findings: list[dict] = []
    if live is None:
        findings.append({
            "kind": "missing-in-aws",
            "detail": (
                f"{SSM_PARAM_NAME} does not exist live — the manifest declares "
                f"exercise_cadence={declared!r} but nothing has deployed it. Run "
                "infrastructure/deploy-infrastructure.sh (or "
                f"./infrastructure/weekly_cadence_drift.py --enforce)."
            ),
        })
    elif live != declared:
        findings.append({
            "kind": "value-mismatch",
            "detail": (
                f"manifest declares exercise_cadence={declared!r} but live SSM value is "
                f"{live!r} — a deploy did not run, or the parameter was edited out of band. "
                "Fix: ./infrastructure/weekly_cadence_drift.py --enforce"
            ),
        })
    return findings


def enforce() -> bool:
    """Write the manifest value to SSM. Returns True iff a write was made."""
    declared = declared_cadence()
    live = _live_value()
    if live == declared:
        return False
    rc, _, err = _aws([
        "ssm", "put-parameter",
        "--name", SSM_PARAM_NAME,
        "--type", "String",
        "--value", declared,
        "--overwrite",
    ])
    if rc != 0:
        raise RuntimeError(f"aws ssm put-parameter --name {SSM_PARAM_NAME}: {err}")
    return True


def main() -> int:
    ap = argparse.ArgumentParser(description="weekly exercise-cadence manifest vs live SSM drift check")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true", help="verify manifest == live SSM value")
    mode.add_argument("--enforce", action="store_true", help="write the manifest value to SSM")
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    args = ap.parse_args()

    try:
        if args.enforce:
            wrote = enforce()
            if args.json:
                print(json.dumps({"wrote": wrote}, indent=2))
            else:
                print("✓ SSM value already matches the manifest" if not wrote else "✓ wrote manifest value to SSM")
            return 0

        findings = check()
    except (RuntimeError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps({"findings": findings}, indent=2))
    else:
        if not findings:
            print(f"✓ {SSM_PARAM_NAME} matches infrastructure/weekly_cadence.json")
        for f in findings:
            print(f"  ✗ [{f['kind']}]")
            print(f"      {f['detail']}")

    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
