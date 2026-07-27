"""`infrastructure/deploy.sh` must converge Lambda config on EVERY deploy.

Bug class this guards (2026-07-25 weekly-SF incident): memory and timeout were
passed only to ``create-function``. On an existing function the update path
touched code alone, so the live configuration was whatever someone had last set
by hand — un-versioned state with nothing reconciling it to this repo.

That is how DataPhase2 died. Memory had been raised to 1024 MB on ``$LATEST``,
but ``publish-version`` snapshots configuration at publish time and the ``live``
alias still pointed at version 269, published at 512 MB. The Saturday SF
invokes ``alpha-engine-data-collector:live``, so it kept running at 512 MB and
hit ``Runtime.OutOfMemory`` twice. The raise was real and entirely invisible to
the pipeline.

Two invariants therefore matter, and the ordering one is the subtle half:

1. The update path applies ``update-function-configuration``.
2. It does so BEFORE ``publish-version`` — a config change applied after the
   version is cut never reaches the alias the pipeline actually invokes.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_DEPLOY = Path(__file__).resolve().parents[1] / "infrastructure" / "deploy.sh"
_SRC = _DEPLOY.read_text(encoding="utf-8")

# Comment-stripped view. The header documents the incident in prose and names
# the same commands the code calls, so any positional reasoning must run
# against executable lines only.
_CODE = "\n".join(
    line for line in _SRC.splitlines() if not line.lstrip().startswith("#")
)


def test_deploy_script_exists():
    assert _DEPLOY.is_file(), f"missing {_DEPLOY}"


def test_update_path_applies_configuration():
    """The existing-function branch must reconcile config, not just code."""
    assert "update-function-configuration" in _CODE, (
        "deploy.sh updates function CODE but never CONFIG — memory/timeout on an "
        "existing function will drift from this file, and the drift is invisible "
        "to the SF because it invokes the `live` alias"
    )


def test_configuration_is_applied_before_version_is_published():
    """Ordering invariant: config must land on $LATEST before the version cut."""
    cfg = _CODE.index("update-function-configuration")
    pub = _CODE.index("publish-version")
    assert cfg < pub, (
        "update-function-configuration appears AFTER publish-version — the "
        "published version (and therefore the `live` alias the SF invokes) "
        "would not carry the declared memory/timeout"
    )


@pytest.mark.parametrize("var", ["LAMBDA_MEMORY_MB", "LAMBDA_TIMEOUT"])
def test_runtime_envelope_is_declared_once(var: str):
    """Memory/timeout come from a named variable, not scattered literals."""
    assert re.search(rf'^{var}="\$\{{{var}:-\d+\}}"$', _CODE, re.MULTILINE), (
        f"{var} must be declared once with an overridable default so every "
        f"create/update path converges to the same declared value"
    )


@pytest.mark.parametrize("flag,var", [("--memory-size", "LAMBDA_MEMORY_MB"),
                                      ("--timeout", "LAMBDA_TIMEOUT")])
def test_no_hardcoded_config_literals(flag: str, var: str):
    """Every --memory-size/--timeout passes the variable, never a literal.

    A literal left behind in one branch reintroduces exactly the drift this
    guards: create and update converging on different values.
    """
    for match in re.finditer(rf"{flag}\s+(\S+)", _CODE):
        value = match.group(1).strip('"')
        assert value == f"${{{var}}}" or value == f"${var}", (
            f"{flag} is passed a hardcoded {value!r} — use ${var} so all paths "
            f"converge on the declared envelope"
        )


def test_memory_is_above_the_observed_oom_ceiling():
    """1024 MB floor: phase-2 OOM'd at 512 MB on 2026-07-25.

    Guards against a well-meaning 'right-size it down' that lands back on the
    value the incident already disproved.
    """
    match = re.search(r'LAMBDA_MEMORY_MB="\$\{LAMBDA_MEMORY_MB:-(\d+)\}"', _CODE)
    assert match, "could not parse LAMBDA_MEMORY_MB default"
    assert int(match.group(1)) >= 1024, (
        f"memory default {match.group(1)}MB is at or below the 512MB that OOM'd "
        "on 2026-07-25 plus its margin — do not reduce without new profiling"
    )
