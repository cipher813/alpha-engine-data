"""alpha-engine-config-I7047 deliverable 3: assert the ABSENCE of the inline
`trap 'aws s3 cp ... EXIT'` log-ship anti-pattern across every state of every
scheduled SF definition in this repo — not the presence of the krepis
wrapper on the two health-observe stages the issue named.

Why absence, not presence: enumerating the states that already got fixed is
blind to the one that is still missing. Measured 2026-08-08: the scheduled
Saturday weekly run succeeded on every real work stage and still reported
DEGRADED, solely because SaturdayHealthCheck and WeeklySubstrateHealthCheck
carried this pattern — `WeeklySubstrateHealthCheck`'s copy additionally
collapsed under ASL's `commands.$`/States.Array `\\'`-escape semantics
(`trap: s3: invalid signal specification`, rc=127; ASL does not unescape
`\\'` to `'`, it passes the backslash through literally and bash word-splits
the trap's own command line into bogus signal names).

The sweep this test backs found a THIRD, differently-shaped instance:
`RunMorningPlanner` in step_function_daily.json carried the same textual
pattern inside a plain (non-`commands.$`) `commands` array — not exhibiting
the ASL-escape bug specifically, but the same anti-pattern this fleet is
retiring fleet-wide in favor of `krepis.ssm_log_capture` (18+ stages now,
across all three scheduled SF definitions). Fixed in the same PR.

The krepis module's own docstring names this exact failure register
(`krepis/src/krepis/ssm_log_capture.py`), and is the institutional
replacement — not a per-state hand patch.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_INFRA_DIR = Path(__file__).resolve().parent.parent / "infrastructure"

# alpha-engine-config-I7047's Closes-when line names these three explicitly:
# "zero occurrences of `trap 'aws s3 cp` remain in any SF definition in
# nousergon-data." step_function_groom.json is a fourth scheduled SF
# definition in this repo; included here too since the issue's own
# deliverable-3 language ("assert the absence of the anti-pattern across
# ALL stages") does not carve it out, and a future groom-pipeline log-ship
# state should not get to reintroduce the pattern unnoticed.
_SF_FILES = [
    "step_function.json",
    "step_function_eod.json",
    "step_function_daily.json",
    "step_function_groom.json",
]

# The exact substring that identifies the anti-pattern regardless of
# surrounding quoting/escaping style (`trap 'aws s3 cp` / `trap \'aws s3 cp`
# both contain this literal run).
_ANTI_PATTERN = "trap 'aws s3 cp"


def _iter_command_strings(states: dict):
    """Yield (state_name, command_string) for every SSM command line in
    every state, descending into Parallel branches and Map iterators.
    Reads both the static `commands` list and the `commands.$` intrinsic
    (the raw ASL expression string — sufficient for a substring check,
    since the anti-pattern's characteristic text survives ASL's escaping
    either way)."""
    for name, body in states.items():
        if not isinstance(body, dict):
            continue
        params = (body.get("Parameters") or {}).get("Parameters") or {}
        cmds = params.get("commands")
        if isinstance(cmds, list):
            for c in cmds:
                if isinstance(c, str):
                    yield name, c
        cmds_expr = params.get("commands.$")
        if isinstance(cmds_expr, str):
            yield name, cmds_expr

        if body.get("Type") == "Parallel":
            for branch in body.get("Branches", []):
                yield from _iter_command_strings(branch.get("States", {}))
        if body.get("Type") == "Map":
            proc = body.get("ItemProcessor") or body.get("Iterator") or {}
            yield from _iter_command_strings(proc.get("States", {}))


@pytest.mark.parametrize("sf_file", _SF_FILES)
def test_no_inline_trap_aws_s3_cp_anywhere(sf_file):
    path = _INFRA_DIR / sf_file
    assert path.exists(), f"{sf_file} not found under {_INFRA_DIR}"
    states = json.loads(path.read_text())["States"]

    offenders = [
        (name, cmd) for name, cmd in _iter_command_strings(states)
        if _ANTI_PATTERN in cmd
    ]
    assert not offenders, (
        f"{sf_file}: {len(offenders)} state(s) still carry the inline "
        f"`trap 'aws s3 cp ... EXIT` log-ship anti-pattern — migrate to "
        f"krepis.ssm_log_capture (see the 18+ sibling stages across this "
        f"repo's SF definitions for the idiom): "
        f"{sorted(n for n, _ in offenders)}"
    )
