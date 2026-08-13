"""The preflight-sweep dispatcher's role cannot reach a box it does not own.

`TerminateSweepBoxOnSendCommandFailure` is conditioned on
`ec2:ResourceTag/pipeline_role == preflight-sweep`, which reads as a tight
grant. It is only tight if the role cannot WRITE that tag onto an arbitrary
instance. Shipped as of #1364, `ec2:CreateTags` carried no condition at all on
`instance/*`, so the composition was:

    create_tags(<any instance>, pipeline_role=preflight-sweep)
    terminate_instances(<that instance>)   # condition now satisfied

which put every instance in the account inside this Lambda's blast radius,
including the trading box. The terminate condition was never the boundary it
appeared to be; the CreateTags grant was.

`sf-watch-spot-dispatcher` already solved this in the same repo
(`CreateTagsAtLaunchOnly` + `CreateDiscriminatorTagsOnOwnBoxesOnly`), so this
is a mirror of an established pattern rather than a new one.

Same reasoning for `ssm:SendCommand`: an unconditioned grant on `instance/*`
is arbitrary shell on any managed instance. Note the two SendCommand
statements are deliberately SEPARATE — an IAM `Condition` applies to every
`Resource` in its statement, so tagging-conditioning the statement that also
carries the `AWS-RunShellScript` document ARN would deny the document (which
has no such tag) and break every send.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

POLICY = (
    Path(__file__).resolve().parents[1]
    / "infrastructure"
    / "lambdas"
    / "preflight-sweep-dispatcher"
    / "iam-policy.json"
)

# The box this dispatcher borrows from alpha-engine-weekly-freshness-spot-dispatcher.
# Sourced from that Lambda's `tag_name=` argument; if it is ever renamed, these
# conditions stop matching and the sweep fails loudly rather than widening.
BOX_NAME = "alpha-engine-weekly-freshness-spot"

# Actions that must never be grantable against an instance this role does not own.
INSTANCE_SCOPED_ACTIONS = {"ec2:CreateTags", "ec2:TerminateInstances", "ssm:SendCommand"}


def _statements() -> list[dict]:
    return json.loads(POLICY.read_text())["Statement"]


def _actions(stmt: dict) -> list[str]:
    action = stmt["Action"]
    return [action] if isinstance(action, str) else list(action)


def _resources(stmt: dict) -> list[str]:
    resource = stmt["Resource"]
    return [resource] if isinstance(resource, str) else list(resource)


def _targets_any_instance(stmt: dict) -> bool:
    return any(":instance/" in r for r in _resources(stmt))


@pytest.mark.parametrize("action", sorted(INSTANCE_SCOPED_ACTIONS))
def test_instance_scoped_action_is_never_unconditioned(action: str) -> None:
    """No grant of these actions may reach `instance/*` without a condition."""
    for stmt in _statements():
        if action not in _actions(stmt) or not _targets_any_instance(stmt):
            continue
        assert stmt.get("Condition"), (
            f"{stmt.get('Sid')!r} grants {action} on an instance ARN with no "
            "Condition. An unconditioned instance grant here reaches every "
            "instance in the account, including the trading box."
        )


def test_createtags_is_pinned_to_the_sweep_box() -> None:
    """CreateTags is the grant that makes the terminate condition real."""
    grants = [
        s
        for s in _statements()
        if "ec2:CreateTags" in _actions(s) and _targets_any_instance(s)
    ]
    assert grants, "expected a CreateTags grant — the dispatcher re-tags its box"
    for stmt in grants:
        equals = stmt.get("Condition", {}).get("StringEquals", {})
        assert equals.get("ec2:ResourceTag/Name") == BOX_NAME, (
            f"{stmt.get('Sid')!r} must pin CreateTags to instances already "
            f"tagged Name={BOX_NAME}. Without it the role can self-grant the "
            "`pipeline_role=preflight-sweep` tag that "
            "TerminateSweepBoxOnSendCommandFailure keys on, and that terminate "
            "condition stops being a boundary."
        )


def test_sendcommand_to_instances_is_pinned_to_the_sweep_box() -> None:
    grants = [
        s
        for s in _statements()
        if "ssm:SendCommand" in _actions(s) and _targets_any_instance(s)
    ]
    assert grants, "expected a SendCommand grant against instances"
    for stmt in grants:
        equals = stmt.get("Condition", {}).get("StringEquals", {})
        assert equals.get("ssm:resourceTag/Name") == BOX_NAME, (
            f"{stmt.get('Sid')!r} must pin SendCommand to Name={BOX_NAME}. "
            "Unconditioned, this is arbitrary shell on any managed instance."
        )


def test_sendcommand_document_grant_stays_unconditioned_and_separate() -> None:
    """The document ARN must not inherit the instance tag condition.

    A Condition applies to every Resource in its statement. Folding the
    document into the tag-conditioned statement denies `AWS-RunShellScript`
    (it carries no such tag) and breaks every send — a failure that only
    appears at runtime, never in a policy-shape review.
    """
    doc_grants = [
        s
        for s in _statements()
        if "ssm:SendCommand" in _actions(s)
        and any(":document/" in r for r in _resources(s))
    ]
    assert len(doc_grants) == 1, (
        "expected exactly one statement granting SendCommand on the document"
    )
    stmt = doc_grants[0]
    assert not _targets_any_instance(stmt), (
        f"{stmt.get('Sid')!r} must not mix the document ARN with instance ARNs "
        "— they need different conditions, so they need different statements."
    )
    assert "Condition" not in stmt, (
        "the document grant must stay unconditioned; an instance tag condition "
        "here denies the document itself."
    )


def test_terminate_stays_conditioned() -> None:
    """Guards the other half of the pair this test module exists for."""
    grants = [s for s in _statements() if "ec2:TerminateInstances" in _actions(s)]
    assert grants, "expected a TerminateInstances grant"
    for stmt in grants:
        equals = stmt.get("Condition", {}).get("StringEquals", {})
        assert equals.get("ec2:ResourceTag/pipeline_role") == "preflight-sweep", (
            f"{stmt.get('Sid')!r} lost its pipeline_role condition"
        )
