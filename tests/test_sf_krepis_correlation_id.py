"""Every SF krepis ``ssm_log_capture run`` call must pass ``--correlation-id``.

Bug class this guards (2026-07-25 weekly-SF incident): krepis 0.18.8 made the
correlation id mandatory — ``ssm_log_capture run`` exits 2 when neither
``--correlation-id`` nor ``$RUN_TOKEN`` is present. No SF definition passed
either, so all 11 weekly SSM workload states failed at launch. The pipeline
burned 8h26m and 20 operator reruns before completing degraded.

The incident was unblocked by setting ``RUN_TOKEN`` in the dashboard box's
systemd manager environment (``systemctl set-environment``). That value lives
**in memory only** — nothing under ``/etc/systemd/`` persists it — so the next
reboot or SSM-agent replacement reproduces the failure exactly. Depending on
box-local environment state for a hard CLI precondition is the defect; passing
the argument from the definition is the fix.

``$$.Execution.Name`` is the correlation value: krepis uses it as the S3 log
key suffix (``{hostname}-{HHMMSSZ}-{correlation_id}.log``) and as a
``# correlation-id:`` header line, so keying on the execution name groups every
stage's logs under the run that produced them — including reruns, which get
their own name and therefore their own log set.

This test walks the definitions themselves rather than pinning strings, so a
newly-added SSM workload state is covered automatically.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_INFRA = _REPO_ROOT / "infrastructure"

# Every state-machine definition in this repo. A definition with no krepis
# call simply yields no sites — it is not an error.
_SF_DEFINITIONS = [
    _INFRA / "step_function.json",
    _INFRA / "step_function_daily.json",
    _INFRA / "step_function_eod.json",
    _INFRA / "step_function_groom.json",
]

_KREPIS_CALL = "ssm_log_capture run"
# NOTE: no `\s*` after the comma — `[^,()]+` already matches spaces, and having
# both makes the argument-list group ambiguous, which CodeQL correctly flags as
# exponential backtracking (py/redos). Leading whitespace on each argument is
# stripped in code instead.
_STATES_FORMAT = re.compile(r"States\.Format\('((?:[^'\\]|\\.)*)'((?:,[^,()]+)*)\)")


def _iter_command_sites(node, definition: str, path: str = ""):
    """Yield (definition, state, command_string) for every krepis SSM command."""
    if isinstance(node, dict):
        for key, val in node.items():
            if (
                key == "commands.$"
                and isinstance(val, str)
                and _KREPIS_CALL in val
            ):
                state = path.split("/States/")[-1].split("/")[0] or path
                yield definition, state, val
            else:
                yield from _iter_command_sites(val, definition, f"{path}/{key}")
    elif isinstance(node, list):
        for i, item in enumerate(node):
            yield from _iter_command_sites(item, definition, f"{path}[{i}]")


def _all_sites():
    sites = []
    for path in _SF_DEFINITIONS:
        if not path.exists():
            continue
        doc = json.loads(path.read_text(encoding="utf-8"))
        sites.extend(_iter_command_sites(doc, path.name))
    return sites


_SITES = _all_sites()


def test_definitions_contain_krepis_sites():
    """Guard the guard: if the walker stops finding sites, this test is inert."""
    assert _SITES, "no krepis ssm_log_capture sites found — walker or definitions changed"


@pytest.mark.parametrize(
    "definition,state,command",
    _SITES,
    ids=[f"{d}:{s}" for d, s, _ in _SITES],
)
def test_krepis_call_passes_correlation_id(definition: str, state: str, command: str):
    """Each krepis invocation passes --correlation-id explicitly.

    ``$RUN_TOKEN`` is deliberately NOT accepted as a substitute here: it is the
    box-local environment dependency the 2026-07-25 incident proved fragile.
    """
    assert "--correlation-id" in command, (
        f"{definition}:{state} invokes `{_KREPIS_CALL}` without --correlation-id. "
        "krepis exits 2 without it; do not rely on $RUN_TOKEN being set on the box."
    )


@pytest.mark.parametrize(
    "definition,state,command",
    _SITES,
    ids=[f"{d}:{s}" for d, s, _ in _SITES],
)
def test_correlation_id_is_the_execution_name(definition: str, state: str, command: str):
    """The correlation value is the SF execution name, not a static literal.

    A static token would collide across runs in the S3 log key, defeating the
    grouping the correlation id exists to provide.
    """
    assert "--correlation-id {}" in command, (
        f"{definition}:{state} does not pass --correlation-id as a States.Format "
        "placeholder — a hardcoded correlation id collides across runs"
    )
    assert "$$.Execution.Name" in command, (
        f"{definition}:{state} passes --correlation-id but not $$.Execution.Name"
    )


@pytest.mark.parametrize(
    "definition,state,command",
    _SITES,
    ids=[f"{d}:{s}" for d, s, _ in _SITES],
)
def test_states_format_arity_matches(definition: str, state: str, command: str):
    """Every States.Format placeholder count equals its argument count.

    Inserting a leading placeholder without prepending its argument yields a
    States.Runtime error at execution time, not at deploy time — exactly the
    class that killed two reruns during the incident.
    """
    for match in _STATES_FORMAT.finditer(command):
        template, raw_args = match.group(1), match.group(2)
        placeholders = template.count("{}")
        args = [a for a in raw_args.split(",") if a.strip()]
        assert placeholders == len(args), (
            f"{definition}:{state} States.Format arity mismatch — "
            f"{placeholders} placeholder(s) vs {len(args)} argument(s): {args}"
        )
