"""Exactly one workflow in this repo assumes `github-actions-cfn-sf-stack-deploy`.

`alpha-engine-config-I10112`, stage 4 of `I10109` (ruling `I9788` option b).

That role carries `cloudformation:CreateStack` on `alpha-engine-orchestration`,
`states:Create/Update/DeleteStateMachine` on every orchestration pipeline, and
`iam:PassRole` on `alpha-engine-*` to `states`/`scheduler`/`events`/`lambda`.
The whole point of splitting it out of `github-actions-lambda-deploy` was to
take those three grants from *74 workflows in 21 repos* down to one workflow —
so the count of consumers **is** the deliverable, and it lives in this repo,
because this is where a second consumer would be added.

The role's trust policy pins `job_workflow_ref` to `deploy-infrastructure.yml`,
so a second consumer here does not silently succeed — it fails at
`sts:AssumeRoleWithWebIdentity` on a merged `main`, with nothing red until then.
This test moves that failure to review time.

The reverse direction matters too: `deploy-infrastructure.yml` must actually
stop using the shared role. A flip that adds the new `role-to-assume` line
without removing the old one is not a narrowing at all, and both lines are valid
YAML in a `with:` block only until the duplicate-key check runs — which is not
part of `actionlint`'s default rule set for composite `with` mappings.

`deploy-scheduled-groom-dispatcher.yml` deliberately stays on the shared role;
see the module-level constant below for why, and `I10112` for the ruling shape.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"

ACCOUNT = "711398986525"
NEW_ROLE = "github-actions-cfn-sf-stack-deploy"
SHARED_ROLE = "github-actions-lambda-deploy"

#: The single workflow this role exists for.
INTENDED_CONSUMER = "deploy-infrastructure.yml"

#: Named in `I10112`'s stage-4 row as a second consumer, and deliberately NOT
#: flipped in that stage. Its two AWS steps both run
#: `infrastructure/lambdas/scheduled-groom-dispatcher/deploy.sh`, one process
#: whose flagless path spans stage 4 (`stepfunctions create/update/describe-
#: state-machine`), stage 9 (`lambda create-function`, `update-function-code`,
#: `update-function-configuration`, `get-function`) and stage 7 (log groups),
#: and whose `--reconcile-schedules` path is stage 8 (`scheduler create/update/
#: delete/get/list-schedule`). A GitHub Actions step takes exactly one set of
#: credentials, so "split the job so only the CFN/SF steps assume the new role"
#: would mean splitting `deploy.sh` itself — out of scope here, and it would
#: decouple the schedule reconcile from the code deploy it must accompany
#: (the `nousergon-data#1179` failure: a green deploy that created no schedule).
DEFERRED_CONSUMER = "deploy-scheduled-groom-dispatcher.yml"

_ROLE_TO_ASSUME = re.compile(
    r"^\s*role-to-assume:\s*(?P<arn>arn:aws:iam::\d+:role/[A-Za-z0-9+=,.@_-]+)\s*$",
    re.MULTILINE,
)


def _workflows() -> list[Path]:
    return sorted(
        p
        for p in WORKFLOW_DIR.iterdir()
        if p.is_file() and p.suffix in {".yml", ".yaml"}
    )


def _assumed_roles(path: Path) -> list[str]:
    """Role NAMES named in `role-to-assume:` lines. Executable references only —
    a role named in a comment is documentation, not a consumer."""
    return [
        m.group("arn").rsplit("/", 1)[1]
        for m in _ROLE_TO_ASSUME.finditer(path.read_text())
    ]


def test_exactly_one_workflow_assumes_the_new_role():
    consumers = sorted(p.name for p in _workflows() if NEW_ROLE in _assumed_roles(p))
    assert consumers == [INTENDED_CONSUMER], (
        f"{NEW_ROLE} is scoped to one workflow by its trust policy's "
        f"job_workflow_ref condition; found {consumers}. A second consumer must "
        f"land together with the trust-policy edit in nous-ergon-ops, or it "
        f"fails at AssumeRoleWithWebIdentity only after merge."
    )


def test_the_intended_consumer_no_longer_assumes_the_shared_role():
    roles = _assumed_roles(WORKFLOW_DIR / INTENDED_CONSUMER)
    assert roles == [NEW_ROLE], (
        f"{INTENDED_CONSUMER} must assume exactly {NEW_ROLE}; found {roles}. "
        f"Leaving the {SHARED_ROLE} line in place is not a narrowing."
    )


def test_the_deferred_consumer_still_assumes_the_shared_role():
    """Not an aspiration — a guard. Flipping it before stages 7 and 8 land takes
    away log-group and EventBridge/Scheduler authority the same job still uses,
    and the first symptom is a half-applied production deploy."""
    roles = _assumed_roles(WORKFLOW_DIR / DEFERRED_CONSUMER)
    assert roles == [SHARED_ROLE], (
        f"{DEFERRED_CONSUMER} still needs the stage-7 (log groups) and stage-8 "
        f"(EventBridge/Scheduler) statements that stay on {SHARED_ROLE} until "
        f"those stages of alpha-engine-config-I10109 land; found {roles}."
    )


@pytest.mark.parametrize("workflow", [p.name for p in _workflows()])
def test_every_role_to_assume_names_this_account(workflow: str):
    """Cheap backstop on the regex above: if a `role-to-assume` line ever stops
    matching, the consumer count silently reads zero and every test here passes
    for the wrong reason."""
    text = (WORKFLOW_DIR / workflow).read_text()
    declared = text.count("role-to-assume:")
    matched = len(_ROLE_TO_ASSUME.findall(text))
    assert declared == matched, (
        f"{workflow}: {declared} role-to-assume line(s), {matched} parsed. "
        f"An unparsed line makes this module's consumer count meaningless."
    )
    for role_arn_name in _assumed_roles(WORKFLOW_DIR / workflow):
        assert role_arn_name, workflow
    assert f"arn:aws:iam::{ACCOUNT}:role/" in text or declared == 0
