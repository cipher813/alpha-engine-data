"""CodeFreshnessGate must freeze the executor SHA under the shared lock, and
must assert HEAD == origin/main at the moment it freezes.

alpha-engine-config-I8684. The gate ends by writing
``/home/ec2-user/.frozen_executor_sha`` — the T0 pin that
``executor/preflight.py::check_deploy_drift`` validates against for the rest of
the session. It used to write ``git rev-parse HEAD`` unconditionally, outside
any lock, several statements after the CODE-STALE-AFTER-HEAL re-check that
established HEAD was correct. Those are two unsynchronised reads with a
concurrent git writer free to run between them.

Measured on the trading box 2026-08-25/26. ``boot-pull.service``'s deploy-gate
rollback (a ``git reset --hard`` to the previous SHA, itself outside the lock
until crucible-executor-PR494) raced this gate:

* 2026-08-25 — the rollback landed AFTER the re-check. The gate froze
  ``20ca44aa`` (#492) while ``origin/main`` was ``c5edc712`` (#493). Because the
  frozen pin and the checkout agreed with each other, ``check_deploy_drift``
  raised nothing, the preopen reported SUCCESS, and the session traded stale
  code with no signal on any surface.
* 2026-08-26 — the same two writers finished in the other order and the
  pipeline died at ``CODE-STALE-AFTER-HEAL`` instead, losing the session.

One race, two outcomes, and the silent one is the worse of the pair. Taking the
lock serialises the writers; asserting the invariant at the point it is recorded
means a writer that still slips through fails loudly instead of being frozen in.
"""

from __future__ import annotations

import json
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF_PATH = _REPO_ROOT / "infrastructure" / "step_function_daily.json"

_SHARED_LOCK = "/home/ec2-user/.ae-git-sync.lock"
_FROZEN = "/home/ec2-user/.frozen_executor_sha"


def _freeze_command() -> str:
    doc = json.loads(_SF_PATH.read_text())
    cmds = doc["States"]["CodeFreshnessGate"]["Parameters"]["Parameters"]["commands"]
    matches = [c for c in cmds if _FROZEN in c]
    assert len(matches) == 1, (
        f"expected exactly one command writing {_FROZEN}, found {len(matches)}"
    )
    return matches[0]


def test_freeze_takes_the_shared_git_lock() -> None:
    cmd = _freeze_command()
    assert "flock" in cmd and _SHARED_LOCK in cmd, (
        f"the {_FROZEN} write must run under {_SHARED_LOCK}, the same advisory "
        "lock boot-pull.sh, this gate's own git_retry, and ChronicGapSelfHeal "
        "take. Unlocked, a concurrent rollback can move HEAD between the "
        "CODE-STALE-AFTER-HEAL re-check and this write."
    )


def test_freeze_asserts_head_matches_origin_main() -> None:
    cmd = _freeze_command()
    assert "rev-parse origin/main" in cmd, (
        "the freeze must re-read origin/main and compare, not trust the "
        "re-check that ran several statements earlier."
    )
    assert 'if [ "$h" != "$u" ]' in cmd, (
        "the freeze must compare HEAD against origin/main and fail on mismatch."
    )
    assert "exit 1" in cmd, (
        "a HEAD/origin-main mismatch at freeze time must FAIL the gate. Writing "
        "the moved SHA is what made 2026-08-25 trade stale code and report "
        "green — check_deploy_drift compares the checkout against this pin, so "
        "a wrong pin that matches a wrong checkout is self-consistent and "
        "silent."
    )


def test_freeze_names_the_race_in_what_the_operator_reads() -> None:
    cmd = _freeze_command()
    assert "FREEZE-RACE" in cmd, (
        "sf-pipeline-policy.md §2.3: the terminal failure message must carry "
        "the actual error. A concurrent-writer race and a genuinely stale box "
        "are different incidents with different fixes, and the operator reads "
        "only this line."
    )
