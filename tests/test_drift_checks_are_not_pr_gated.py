#!/usr/bin/env python3
"""A live-AWS drift check may never run on the ``pull_request`` path.

Brian ruling 2026-08-10: a pull request whose only failing check is a drift
check must read GREEN, so that the merge decision is never clouded by a signal
that is not about the diff under review.

The reasoning, so a later change does not re-add one:

  A drift check compares CODIFIED source against LIVE AWS. Its answer is a
  property of the account at the moment it runs — not of the branch. On a pull
  request that produces two distinct failure modes, and BOTH are wrong there:

    * Someone else's out-of-band change (a console edit, a paused rule
      re-enabled, an un-deployed merge) reddens a PR whose author did not cause
      it and cannot clear it — the gate-only-the-blocked-actor-can-clear trap.
    * The PR's OWN intended change is the divergence, because the apply or
      restamp happens at deploy, post-merge. The check is then red exactly when
      the author did the right thing (alpha-engine-config-I6591, ops-I305).

  Both train the reader to merge past a red X, which is how a real drift
  finding goes unread (nous-ergon-ops-I563, alarm-red-by-construction).

Nothing is lost by moving these to ``push: [main]`` + ``schedule``: the merge
arm grades the same comparison minutes later, with a deploy behind it, and the
daily sweep catches what no merge can see. What IS lost by leaving them on the
PR path is the meaning of a red check.

This file is the class-level guard. ``sf-arn-drift-check.yml`` is pinned by
name because it is the workflow the 2026-08-10 instance came from; the generic
test covers every workflow added later.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = REPO_ROOT / ".github" / "workflows"

# A step matching any of these reads live AWS and compares it against codified
# source. Substring match against the step's `run:` block.
LIVE_DRIFT_INVOCATIONS = (
    "automation_pause.py --check",
    "check-drift.py",
    "check-definition-drift.py",
    "check-schedule-drift.py",
    "check-manifest-drift.py",
    "check-lambda-existence.py",
    "weekly_cadence_drift.py",
)


def _workflows() -> list[Path]:
    return sorted(p for p in WORKFLOWS.glob("*.yml"))


def _triggers(doc: dict) -> dict:
    # PyYAML parses the bare key `on` as the boolean True (YAML 1.1).
    return doc.get("on") or doc.get(True) or {}


def _run_blocks(doc: dict):
    for job in (doc.get("jobs") or {}).values():
        for step in (job or {}).get("steps") or []:
            run = (step or {}).get("run")
            if isinstance(run, str):
                yield (step.get("name") or "<unnamed>"), run, step.get("if")


def test_sf_arn_drift_check_has_no_pull_request_trigger():
    """The 2026-08-10 instance, pinned by name.

    #1291 was a Lambda capability-profile fix. Its only red check was this
    workflow's automation-pause step, reporting that
    `alpha-research-thinktank-daily` had been re-enabled out of band hours
    earlier. Nothing in the diff could have made it green.
    """
    doc = yaml.safe_load((WORKFLOWS / "sf-arn-drift-check.yml").read_text(encoding="utf-8"))
    triggers = _triggers(doc)
    assert "pull_request" not in triggers, (
        "sf-arn-drift-check.yml grades live AWS against codified source; on a "
        "pull_request it reports drift the author did not cause and cannot "
        "clear. Keep it on push:[main] + schedule."
    )
    # And the coverage it moved to must actually exist.
    assert "push" in triggers and "main" in (triggers["push"].get("branches") or [])
    assert "schedule" in triggers


@pytest.mark.parametrize("path", _workflows(), ids=lambda p: p.name)
def test_no_live_drift_step_runs_on_pull_request(path: Path):
    doc = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(doc, dict):
        pytest.skip(f"{path.name} is not a mapping")
    if "pull_request" not in _triggers(doc):
        return

    for name, run, guard in _run_blocks(doc):
        hit = next((s for s in LIVE_DRIFT_INVOCATIONS if s in run), None)
        if hit is None:
            continue
        # A step explicitly excluded from the PR path is fine — that is the
        # same fix, applied per step instead of per workflow.
        if guard and re.search(r"github\.event_name\s*!=\s*'pull_request'", str(guard)):
            continue
        if guard and "pull_request" not in str(guard) and "schedule" in str(guard):
            continue
        pytest.fail(
            f"{path.name} step {name!r} runs {hit} on the pull_request path. "
            "A live-AWS drift comparison says nothing about a PR's diff and "
            "reddens PRs their authors cannot fix (Brian ruling 2026-08-10). "
            "Move the workflow to push:[main] + schedule, or guard the step "
            "with `if: github.event_name != 'pull_request'`."
        )
