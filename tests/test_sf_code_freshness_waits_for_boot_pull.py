"""CodeFreshnessGate must let boot-pull.service finish before it reads the
checkouts.

alpha-engine-config-I9829. The gate's SSM command is sent as soon as the SSM
agent reports ``PingStatus=Online``, which happens seconds into boot. On
2026-09-02 that was 12:16:01 — six seconds after ``boot-pull.service`` started
and thirty-one seconds before it finished. The gate then fetched the same three
repos boot-pull was mid-way through repairing, and its fetch of the one private
repo returned:

    remote: Write access to repository not granted.
    fatal: unable to access '.../alpha-engine-config.git/': ... error: 403

because boot-pull had not yet reached the step that removes the credential
dotfile shadowing the App credential helper. ``git_retry`` retries only the
``Another git process seems to be running`` lock message, so a 403 fell straight
through, the gate failed, and the session was lost.

The existing lock discipline (test_sf_code_freshness_freeze_under_lock,
I8684) serialises the two writers when they overlap. It cannot express the
different property this file pins: boot-pull's work must be *complete* before
the gate forms a verdict about it, because a checkout read mid-repair is
neither the pre-repair state nor the post-repair one, and the gate has no way
to tell which it got.

Deliberately NOT a halt. The wait is bounded and expiry proceeds, because this
gate is the enforcing layer and its own verdict still stands afterwards. A
second halt branch here would trade one lost session for a different one, which
is the failure mode sf-pipeline-policy.md §3 weighs against.
"""

from __future__ import annotations

import json
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_daily.json"

_UNIT = "boot-pull.service"


def _commands() -> list[str]:
    doc = json.loads(_SF_PATH.read_text())
    return doc["States"]["CodeFreshnessGate"]["Parameters"]["Parameters"]["commands"]


def _wait_command() -> str:
    matches = [c for c in _commands() if _UNIT in c and "ActiveState" in c]
    assert len(matches) == 1, (
        f"expected exactly one command inspecting {_UNIT}'s ActiveState, "
        f"found {len(matches)}"
    )
    return matches[0]


def test_gate_waits_on_boot_pull_before_reading_the_checkouts() -> None:
    cmds = _commands()
    wait_idx = cmds.index(_wait_command())
    fetch_idx = next(
        i for i, c in enumerate(cmds) if "fetch --quiet origin main" in c
    )
    assert wait_idx < fetch_idx, (
        "the boot-pull settle wait must precede the first fetch — a checkout "
        "read while boot-pull is mid-repair is neither the pre- nor the "
        "post-repair state"
    )


def test_the_wait_is_actually_invoked_not_merely_defined() -> None:
    """A shell function that is defined and never called is the quietest way
    for this fix to regress."""
    cmds = _commands()
    assert any(c.strip() == "bp_wait" for c in cmds), (
        "bp_wait is defined but never called"
    )


def test_the_wait_treats_activating_as_the_only_non_terminal_state() -> None:
    cmd = _wait_command()
    assert "activating" in cmd, (
        "the wait must key on systemd's transient states; 'failed' and "
        "'inactive' are terminal and must NOT be waited on — boot-pull failing "
        "is exactly when this gate most needs to run"
    )
    for terminal in ("failed", "inactive"):
        assert f"{terminal})" not in cmd, (
            f"'{terminal}' is a terminal state and must not extend the wait"
        )


def test_the_wait_is_bounded() -> None:
    cmd = _wait_command()
    assert "24" in cmd and "sleep 5" in cmd, (
        "an unbounded wait converts a hung boot-pull into a gate that never "
        "returns, inside a 300s executionTimeout"
    )


def test_wait_expiry_proceeds_and_never_halts() -> None:
    """The one property that keeps this from becoming a new way to lose a
    session."""
    cmd = _wait_command()
    assert "proceeding anyway" in cmd, (
        "expiry must proceed and say so; this gate is the enforcing layer"
    )
    assert "exit 1" not in cmd, (
        "the settle wait must not introduce a halt branch — see "
        "sf-pipeline-policy.md §7a.4 on changes that alter when a halt is taken"
    )


def test_the_wait_fits_inside_the_stage_timeout() -> None:
    """120s of wait plus the gate's own work must stay under the declared
    budget; timeouts are budgets, not accommodations (§3)."""
    doc = json.loads(_SF_PATH.read_text())
    gate = doc["States"]["CodeFreshnessGate"]
    timeout = int(gate["TimeoutSeconds"])
    execution_timeout = int(gate["Parameters"]["Parameters"]["executionTimeout"][0])
    max_wait = 24 * 5
    assert max_wait < timeout, (
        f"the {max_wait}s settle wait must leave room inside the {timeout}s "
        "stage timeout"
    )
    assert max_wait < execution_timeout, (
        f"the {max_wait}s settle wait must leave room inside the "
        f"{execution_timeout}s SSM executionTimeout"
    )


def test_the_wait_reports_what_it_observed() -> None:
    """A wait that is silent on a healthy boot leaves the next diagnosis with
    the same blank the 2026-09-02 one had."""
    cmd = _wait_command()
    assert "already terminal" in cmd, (
        "the no-wait path must still log, so its absence is evidence"
    )
    assert "settled to" in cmd, "a wait that ended must name what it waited for"
