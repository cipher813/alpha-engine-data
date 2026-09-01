#!/usr/bin/env python3
"""check-lambda-timeout-drift.py — the codified Lambda timeout table vs live AWS.

`sf-pipeline-policy.md` §2.4: verification reads the DEPLOYED artifact, never
the source that claims to produce it.

`infrastructure/sf_definitions.py::CODIFIED_FUNCTION_TIMEOUTS_SEC` is what the
timeout-ordering guard grades every stage's declared `TimeoutSeconds` against.
It is a claim about the live account — the functions are deployed from four
other repositories, so no merge here moves them and nothing here notices when
they move. A stage timeout graded against a stale table is graded against
nothing.

WHY THIS SCRIPT EXISTS (alpha-engine-config-I9702 arc, 2026-09-01). The
assertion already existed, as `test_the_codified_function_timeouts_match_live`
in tests/test_sf_lambda_timeout_ordering.py — skipped unless
`SF_TIMEOUT_LIVE_CHECK` is set, and that variable is set in NO workflow in this
repo. The one test proving the table was not a stale claim had never executed.
Measured 2026-09-01 by running it by hand: the table happened to be accurate,
which is luck, not enforcement.

It lives here, beside its sibling drift checkers, because it compares CODIFIED
source against LIVE AWS — the criterion `sf-arn-drift-check.yml`'s header sets
for belonging on that workflow's trigger set (post-merge and daily, never on a
PR, so a red a PR author did not cause can never sit on their change).

A missing function is drift, not an absence: the ordering guard demands a row
for every invoked function, so a row naming a function that does not exist
means the guard is asserting against a ghost.

Usage:
  ./infrastructure/step-functions/check-lambda-timeout-drift.py

Exit 0 when every codified timeout matches live; 1 on any drift; 2 on an AWS
call failure (an AccessDenied must never be reported as "no drift").

Requires `lambda:GetFunction` — the same grant
`check-lambda-existence.py` already relies on, on the same
`github-actions-iam-drift-check` OIDC role.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from infrastructure.sf_definitions import (  # noqa: E402
    CODIFIED_FUNCTION_TIMEOUTS_SEC,
    all_lambda_invoke_states,
)


def _live_timeout(function_name: str) -> int | None:
    """The live `Timeout`, or None when the function does not exist.

    Raises on any other failure. A checker that maps AccessDenied onto "no
    drift" reports green on the one condition it most needs to report — the
    exact shape that let a `cloudwatch:DisableAlarmActions` denial hide behind
    `continue-on-error` on this same workflow for 42 runs.
    """
    proc = subprocess.run(
        [
            "aws", "lambda", "get-function",
            "--function-name", function_name,
            "--query", "Configuration.Timeout",
            "--output", "text",
        ],
        capture_output=True,
        text=True,
    )
    if proc.returncode == 0:
        return int(proc.stdout.strip())
    if "ResourceNotFoundException" in proc.stderr:
        return None
    raise RuntimeError(
        f"aws lambda get-function --function-name {function_name} failed "
        f"(rc={proc.returncode}): {proc.stderr.strip()}"
    )


def main() -> int:
    invoked = {s.normalized_name for s in all_lambda_invoke_states()}
    findings: list[str] = []

    for function_name, codified in sorted(CODIFIED_FUNCTION_TIMEOUTS_SEC.items()):
        try:
            live = _live_timeout(function_name)
        except RuntimeError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2
        if live is None:
            findings.append(
                f"[function-missing] {function_name}: codified {codified}s, "
                "no such function live"
            )
        elif live != codified:
            findings.append(
                f"[timeout-drift] {function_name}: codified {codified}s, live {live}s"
            )

    # A row for a function no definition invokes is an unchecked claim that
    # reads as coverage — the shape the retired eval-judge rows were removed
    # for on 2026-08-29.
    for function_name in sorted(set(CODIFIED_FUNCTION_TIMEOUTS_SEC) - invoked):
        findings.append(
            f"[codified-but-uninvoked] {function_name}: in the table, invoked by "
            "no codified SF state — remove the row or wire the state"
        )

    if findings:
        print(f"Lambda timeout drift detected ({len(findings)} finding(s)):")
        for finding in findings:
            print(f"  {finding}")
        print(
            "\nFix: re-measure and update CODIFIED_FUNCTION_TIMEOUTS_SEC in "
            "infrastructure/sf_definitions.py, and re-derive every stage "
            "TimeoutSeconds that grades against the changed row."
        )
        return 1

    print(
        f"OK: all {len(CODIFIED_FUNCTION_TIMEOUTS_SEC)} codified Lambda timeouts "
        "match live"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
