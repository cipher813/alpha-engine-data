"""A per-stage script's identity must survive sourcing ``_spot_common.sh``.

Ported from ``crucible-predictor`` per alpha-engine-config-I6922 — this repo
carried the identical defect and it was never hand-carried back.

Each per-stage launcher sources ``_spot_common.sh`` and then declares who it
is::

    source "$SCRIPT_DIR/_spot_common.sh"
    _SSM_SLUG="${_SSM_SLUG:-spot-morning-enrich}"

``_spot_common.sh`` used to assign the same parameters itself, with generic
values (``data-weekly``, ``spot-data``). ``${VAR:-default}`` expands to
``default`` only when ``VAR`` is unset or empty — so by the time the stage
script ran, every one of its assignments was a no-op and it silently kept the
shared value.

Measured on this repo 2026-08-13: ``spot_morning_enrich.sh`` asks for
``morning-enrich``, ``spot_data_phase1.sh`` for ``data-phase1`` and
``spot_rag_ingestion.sh`` for ``rag-ingestion`` — and all three ran as
``data-weekly``. Three stages of the weekly Step Function shared one identity,
so the instance ``Name`` tag, the ``run_ssm`` command description, the
``Process`` dimension on the CloudWatch ``Heartbeat`` metric and the ``Process``
dimension on ``SpotInterruptionRetry`` all named the wrong stage. A heartbeat
gap could not be attributed to a stage and a spot retry could not be counted
per stage.

The predictor measured the same class on ``watch-rerun-2026-08-10-9``
(2026-08-11): the heartbeat emitted under ``spot-spot-training`` although the
launcher asks for ``spot-full-training``.

This is the rename-blindness class: the identity a tool searches by is not the
identity the run recorded, and the resulting silence reads as "nothing
happened" rather than "you looked in the wrong place".
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

_INFRA = Path(__file__).resolve().parents[1] / "infrastructure"
_COMMON = _INFRA / "_spot_common.sh"

#: Every launcher that sources `_spot_common.sh`, with the identity it declares
#: for the default invocation (no flags). The expected values here are the
#: ASSERTION — they are what each stage's own prologue asks for.
_STAGES = {
    "spot_morning_enrich.sh": {
        "_SPOT_NAME": "morning-enrich",
        "_SSM_SLUG": "spot-morning-enrich",
        "_PROCESS_NAME": "morning-enrich",
    },
    "spot_data_phase1.sh": {
        "_SPOT_NAME": "data-phase1",
        "_SSM_SLUG": "spot-data-phase1",
        "_PROCESS_NAME": "data-phase1",
    },
    "spot_rag_ingestion.sh": {
        "_SPOT_NAME": "rag-ingestion",
        "_SSM_SLUG": "spot-rag-ingestion",
        "_PROCESS_NAME": "rag-ingestion",
    },
}

_IDENTITY_VARS = ("_SPOT_NAME", "_SSM_SLUG", "_PROCESS_NAME", "MAX_RUNTIME_SECONDS")

#: `_SPOT_NAME="${_SPOT_NAME:-value}"` at the start of a line. This is the
#: SWALLOWABLE form — the one whose whole failure mode is expanding to nothing
#: because the parameter is already set — so it is what the no-op assertions
#: below are written against.
_ASSIGN_RE = re.compile(
    r"^(?P<var>_SPOT_NAME|_SSM_SLUG|_PROCESS_NAME|MAX_RUNTIME_SECONDS)="
    r'"\$\{(?P=var):-(?P<value>[^}]*)\}"',
    re.MULTILINE,
)

#: Lines the identity harness must replay to reproduce the stage's prologue.
#: Not only the `${VAR:-...}` form: `spot_rag_ingestion.sh` sets
#: `MAX_RUNTIME_SECONDS=21900` unconditionally, and lifting only the
#: defaultable form would leave it empty in the harness and report a swallowed
#: identity that is not there.
_PROLOGUE_ASSIGN_RE = re.compile(
    r"^(?:_SPOT_NAME|_SSM_SLUG|_PROCESS_NAME|MAX_RUNTIME_SECONDS)=.*$",
    re.MULTILINE,
)


def test_common_does_not_default_the_stage_identity() -> None:
    """`_spot_common.sh` must declare the identity empty, never populate it.

    A non-empty value here is invisible at the stage script's own assignment —
    it is the whole defect, and it reappears the moment someone "restores a
    sensible default".
    """
    source = _COMMON.read_text()
    for match in _ASSIGN_RE.finditer(source):
        assert match.group("value") == "", (
            f"_spot_common.sh defaults {match.group('var')} to "
            f"{match.group('value')!r}. Any non-empty value makes every "
            f"per-stage `${{{match.group('var')}:-...}}` a no-op, and the stage "
            f"silently runs under the shared identity."
        )


@pytest.mark.parametrize("script_name", sorted(_STAGES))
def test_stage_identity_wins_over_the_shared_defaults(
    script_name: str, tmp_path: Path
) -> None:
    """Sourcing then declaring must yield the STAGE's identity."""
    script = _INFRA / script_name
    source = script.read_text()
    assert _ASSIGN_RE.search(source), f"{script_name}: declares no identity at all"
    stage_lines = [match.group(0) for match in _PROLOGUE_ASSIGN_RE.finditer(source)]

    harness = tmp_path / "identity.sh"
    harness.write_text(
        "#!/usr/bin/env bash\n"
        f"source {_COMMON}\n"
        + "\n".join(stage_lines)
        + "\n"
        + "".join(f'echo "{var}=${var}"\n' for var in _IDENTITY_VARS)
    )

    proc = subprocess.run(
        ["bash", str(harness)], capture_output=True, text=True, timeout=60
    )
    assert proc.returncode == 0, proc.stderr
    observed = dict(
        line.split("=", 1) for line in proc.stdout.strip().splitlines() if "=" in line
    )

    for var, expected in _STAGES[script_name].items():
        assert observed.get(var) == expected, (
            f"{script_name}: {var} resolved to {observed.get(var)!r}, expected "
            f"{expected!r}. The stage's own assignment was swallowed by a "
            f"default in _spot_common.sh."
        )
    assert observed.get("MAX_RUNTIME_SECONDS"), (
        f"{script_name}: MAX_RUNTIME_SECONDS is empty — spot_launch will refuse"
    )


def test_spot_launch_refuses_an_unset_identity(tmp_path: Path) -> None:
    """A stage that forgets to declare itself must fail BEFORE it is billable.

    Removing the defaults trades a silent wrong identity for an unset one. That
    is only an improvement if the unset case is loud, so the assertion lives in
    `spot_launch`, ahead of the instance request.
    """
    harness = tmp_path / "no_identity.sh"
    harness.write_text(
        "#!/usr/bin/env bash\n"
        f"source {_COMMON}\n"
        # Every identity parameter left as _spot_common.sh declared it: empty.
        "spot_launch\n"
    )
    proc = subprocess.run(
        ["bash", str(harness)], capture_output=True, text=True, timeout=60
    )
    assert proc.returncode == 2, (
        "spot_launch should exit 2 on an unset stage identity, got "
        f"{proc.returncode}.\nstdout: {proc.stdout}\nstderr: {proc.stderr}"
    )
    combined = proc.stdout + proc.stderr
    for var in _IDENTITY_VARS:
        assert var in combined, f"the refusal does not name {var}"
    assert "Requesting spot instance" not in combined, (
        "spot_launch reached the instance request before refusing — the whole "
        "point of asserting here is that nothing is billable yet."
    )
