"""No dispatcher/probe role may reach an instance it does not own.

Generalises `test_preflight_sweep_dispatcher_iam_scope.py` (from
`nousergon-data-PR1365` / `alpha-engine-config-I7249`) across EVERY
`infrastructure/lambdas/*/iam-policy.json` in this repo, discovered by glob
rather than enumerated by hand — a new dispatcher directory is covered the
moment it exists, with no test-file edit required.

The bug class (`alpha-engine-config-I7254`, filed after
`nousergon-data-PR1365` fixed one instance of it): `ssm:SendCommand`,
`ec2:CreateTags`, and `ec2:TerminateInstances` are all instance-scoped
actions. Granted against `instance/*` — or, worse, against a bare `"*"`
Resource, which reaches instance/* just as surely and is the shape all three
of I7254's roles actually shipped — with no `Condition`, each is arbitrary
root shell (`SendCommand`), a self-granted discriminator tag that defeats a
sibling terminate condition (`CreateTags`), or an unconditioned kill switch
(`TerminateInstances`) reaching every SSM-managed instance in the account,
including the trading box.

Non-inferable gotcha this guard also pins (I7254's root cause): an IAM
`Condition` applies to every `Resource` in its statement. The
`ssm:SendCommand` document ARN (`arn:...:document/AWS-RunShellScript`)
carries no such tag, so a fix that adds the instance-tag condition IN PLACE
on a statement that also lists the document ARN denies the document and
breaks every send — a failure invisible to a policy-shape review, only
observable on the next live dispatch. The grant must be split into a
document statement (unconditioned) and an instance statement (conditioned)
first; see `nousergon-data-PR1365` and this test's own module for the worked
shape.

KNOWN_GAPS_I7265 (CLOSED): this same scan, run once across the whole repo
while building this guard, found the identical unconditioned pattern live in
seven more roles not in I7254's scope (`alert-drain-dispatcher`,
`arctic-migration-dispatcher`, `canary-replay-dispatcher`,
`ci-watch-dispatcher`, `scheduled-groom-dispatcher`,
`thinktank-spot-dispatcher`, `substrate-health-gate`). Those, plus
`substrate-health-gate`'s originally-unscoped `ssm:GetCommandInvocation`
resource, were fixed by `alpha-engine-config-I7265`
(`nousergon-data` `fix/i7265-dispatcher-iam-scope`): each dispatcher's
`ssm:SendCommand` grant split into an unconditioned document statement plus
an instance statement conditioned on that dispatcher's own launch `Name` tag
(traced through `nousergon_lib.spot_dispatch.launch_with_fallback`, which
tags atomically via `RunInstances` `TagSpecifications` before
`send_async_command` ever issues `ssm:SendCommand`); `ec2:CreateTags` split
the same way for the three roles that still bundled it
(`arctic-migration-dispatcher`, `scheduled-groom-dispatcher`,
`thinktank-spot-dispatcher`). `substrate-health-gate` is the one exception to
"condition on the dispatcher's own tag": it never launches a box, it probes
whichever `$.ec2_instance_id` the Saturday weekly SF hands it immediately
before `MorningEnrich` (`infrastructure/step_function.json`
`SubstrateHealthGate` state) — always the box
`weekly-freshness-spot-dispatcher` just launched — so its already-correct
`SsmDiskProbeWeeklyFreshnessSpot` statement (conditioned on
`alpha-engine-weekly-freshness-spot`) was kept and the redundant unconditioned
`SsmDiskProbe` statement's `ssm:SendCommand`/instance grant was dropped
rather than re-conditioned. `KNOWN_GAPS_I7265` is left as an empty set
(not deleted) so a future regression in this exact class has a place to
register.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

LAMBDAS_DIR = Path(__file__).resolve().parents[1] / "infrastructure" / "lambdas"

# Actions that must never be grantable against an instance a role does not own.
INSTANCE_SCOPED_ACTIONS = {"ec2:CreateTags", "ec2:TerminateInstances", "ssm:SendCommand"}

# Pre-existing violations of this exact class, measured 2026-08-13 while
# building this guard as part of alpha-engine-config-I7254. Out of scope for
# that issue (different roles; each needs its own rehearsal) and tracked
# separately as alpha-engine-config-I7265. alpha-engine-config-I7265 fixed
# all ten (nousergon-data fix/i7265-dispatcher-iam-scope) — the set is now
# empty rather than deleted so a future regression has a place to land.
KNOWN_GAPS_I7265: set[tuple[str, str]] = set()

# Dispatchers already known to split the document grant from the instance
# grant correctly (I7254 + PR1365, then I7265) — pinned against the split
# regressing.
DOCUMENT_SPLIT_DISPATCHERS = (
    "weekly-freshness-spot-dispatcher",
    "data-spot-dispatcher",
    "sf-watch-spot-dispatcher",
    "preflight-sweep-dispatcher",
    "alert-drain-dispatcher",
    "arctic-migration-dispatcher",
    "canary-replay-dispatcher",
    "ci-watch-dispatcher",
    "scheduled-groom-dispatcher",
    "thinktank-spot-dispatcher",
    "substrate-health-gate",
)


def _discover_policies() -> list[Path]:
    return sorted(LAMBDAS_DIR.glob("*/iam-policy.json"))


def _statements(policy: Path) -> list[dict]:
    return json.loads(policy.read_text())["Statement"]


def _actions(stmt: dict) -> list[str]:
    action = stmt["Action"]
    return [action] if isinstance(action, str) else list(action)


def _resources(stmt: dict) -> list[str]:
    resource = stmt["Resource"]
    return [resource] if isinstance(resource, str) else list(resource)


def _targets_any_instance(stmt: dict) -> bool:
    # A bare "*" Resource reaches every instance in the account exactly as
    # surely as an explicit instance/* ARN does — three of I7254's four roles
    # shipped precisely this shape, so a check for ":instance/" alone would
    # have missed all three.
    return any(r == "*" or ":instance/" in r for r in _resources(stmt))


def _cases() -> list:
    cases = []
    for policy in _discover_policies():
        lambda_name = policy.parent.name
        for action in sorted(INSTANCE_SCOPED_ACTIONS):
            case_id = f"{lambda_name}::{action}"
            marks = []
            if (lambda_name, action) in KNOWN_GAPS_I7265:
                marks.append(
                    pytest.mark.xfail(
                        reason=(
                            f"pre-existing, tracked in alpha-engine-config-I7265: {case_id}"
                        ),
                        strict=True,
                    )
                )
            cases.append(pytest.param(policy, lambda_name, action, id=case_id, marks=marks))
    return cases


@pytest.mark.parametrize("policy,lambda_name,action", _cases())
def test_instance_scoped_action_is_never_unconditioned(
    policy: Path, lambda_name: str, action: str
) -> None:
    """No grant of an instance-scoped action may reach `instance/*` (or `*`) unconditioned."""
    for stmt in _statements(policy):
        if action not in _actions(stmt) or not _targets_any_instance(stmt):
            continue
        assert stmt.get("Condition"), (
            f"{lambda_name}: {stmt.get('Sid')!r} grants {action} reaching every "
            "instance in the account (Resource '*' or an instance/* ARN) with no "
            "Condition. This is arbitrary shell (ssm:SendCommand), a self-grantable "
            "discriminator tag that defeats a sibling terminate condition "
            "(ec2:CreateTags), or an unconditioned kill switch (ec2:TerminateInstances) "
            "reaching any SSM-managed instance, including the trading box."
        )


@pytest.mark.parametrize("dispatcher", DOCUMENT_SPLIT_DISPATCHERS)
def test_sendcommand_document_grant_stays_unconditioned_and_separate(dispatcher: str) -> None:
    """Regression guard for I7254's split gotcha on the roles that already have it right.

    A Condition applies to every Resource in its statement. Folding the
    `AWS-RunShellScript` document ARN into the tag-conditioned instance
    statement denies the document (it carries no such tag) and breaks every
    send — a failure that only appears at runtime, never in a policy-shape
    review.
    """
    policy = LAMBDAS_DIR / dispatcher / "iam-policy.json"
    doc_grants = [
        s
        for s in _statements(policy)
        if "ssm:SendCommand" in _actions(s) and any(":document/" in r for r in _resources(s))
    ]
    assert len(doc_grants) == 1, (
        f"{dispatcher}: expected exactly one statement granting SendCommand on the document"
    )
    stmt = doc_grants[0]
    assert not _targets_any_instance(stmt), (
        f"{dispatcher}: {stmt.get('Sid')!r} must not mix the document ARN with instance "
        "ARNs — they need different conditions, so they need different statements."
    )
    assert "Condition" not in stmt, (
        f"{dispatcher}: the document grant must stay unconditioned; an instance tag "
        "condition here denies the document itself."
    )
