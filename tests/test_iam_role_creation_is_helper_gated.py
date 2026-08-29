"""Role creation must go through the shared, presence-gated helper.

WHY (alpha-engine-config-I9114, generalising alpha-engine-config-I9045)
-----------------------------------------------------------------------
`_shared/apply_iam_policy.sh` was fixed on 2026-08-28 so that a DENIED
`iam:GetRole` can never be read as "the role does not exist" — `AccessDenied`
and `NoSuchEntity` had been the same observation, which is how four workflows
came to call `iam:CreateRole` on every run for a week with a grant they do not
hold.

That fix protected exactly the call sites that used the helper.
`thinktank-spot-dispatcher/deploy.sh` had its own copy of the same three lines
and was untouched by it. THAT is the finding this file exists for: a class fix
at the correct layer is worth nothing to a call site that reimplemented the
layer, and the only way to know how many such call sites exist is to enumerate
them and refuse to let the number grow.

The sweep, measured 2026-08-28: 45 shell scripts in this repo still reach
`aws iam create-role` from their own code rather than through
`apply_iam_policy`. Every one of them guards it with the same
`if ! aws iam get-role ... >/dev/null 2>&1` probe, i.e. every one carries the
I9045 misclassification. They are DORMANT, not safe: each sits inside an
operator-only `--bootstrap` block that the CI identity never enters, which is a
property of today's flag layout and not a guarantee. Migrating 45 scripts would
collide with four in-flight PRs, so it is tracked separately; what lands here is
the detection, because detection blindness outranks the defects it hides.

`_REGISTER` is a debt register, not an allowlist. It may only shrink.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent

# The ONE sanctioned implementation. `apply_iam_policy` reaches create-role only
# through `probe_role_presence` (which distinguishes denied from absent) and only
# when its `may_create_role` argument is "true" — the operator path. The
# code-only auto-apply path passes "false" and cannot reach it at all.
_SANCTIONED = {
    "infrastructure/lambdas/_shared/apply_iam_policy.sh",
}

# Scripts that still hand-roll it. Tracked as alpha-engine-config-I9207.
# Delete an entry the moment its script migrates onto `apply_iam_policy` — the
# staleness assertion below fails until you do, because an exemption that
# outlives its debt starts hiding a regression on the same script.
_REGISTER = {
    "infrastructure/codebuild-runners/deploy_codebuild_runner.sh",
    "infrastructure/lambdas/_shared/ensure_overseer_scheduler_role.sh",
    "infrastructure/lambdas/alert-drain-dispatcher/deploy.sh",
    "infrastructure/lambdas/alert-drain-liveness-probe/deploy.sh",
    "infrastructure/lambdas/alpha-engine-alerts-forwarder/deploy.sh",
    "infrastructure/lambdas/arctic-migration-dispatcher/deploy.sh",
    "infrastructure/lambdas/backstop-telegram-notifier/deploy.sh",
    "infrastructure/lambdas/canary-replay-dispatcher/deploy.sh",
    "infrastructure/lambdas/canary-replay-liveness-probe/deploy.sh",
    "infrastructure/lambdas/changelog-cloudwatch-mirror/deploy.sh",
    "infrastructure/lambdas/changelog-incident-mirror/deploy.sh",
    "infrastructure/lambdas/ci-watch-dispatcher/deploy.sh",
    "infrastructure/lambdas/ci-watch-liveness-probe/deploy.sh",
    "infrastructure/lambdas/crypto-balances/deploy.sh",
    "infrastructure/lambdas/data-spot-dispatcher/deploy.sh",
    "infrastructure/lambdas/eod-backstop/deploy.sh",
    "infrastructure/lambdas/eod-precondition-probe/deploy.sh",
    "infrastructure/lambdas/eod-snapshot-existence-check/deploy.sh",
    "infrastructure/lambdas/eod-success-friday-shell-trigger/deploy.sh",
    "infrastructure/lambdas/expense-collector/deploy.sh",
    "infrastructure/lambdas/freshness-monitor/deploy.sh",
    "infrastructure/lambdas/friday-shell-run-report/deploy.sh",
    "infrastructure/lambdas/groom-inject-mock/deploy.sh",
    "infrastructure/lambdas/overseer-backstop-responder/deploy.sh",
    "infrastructure/lambdas/overseer-dispatcher/deploy.sh",
    "infrastructure/lambdas/overseer-liveness-probe/deploy.sh",
    "infrastructure/lambdas/pipeline-watchdog/deploy.sh",
    "infrastructure/lambdas/preflight-sweep-dispatcher/deploy.sh",
    "infrastructure/lambdas/preopen-deploy-readiness-probe/deploy.sh",
    "infrastructure/lambdas/saturday-integrity-sentinel/deploy.sh",
    "infrastructure/lambdas/saturday-sf-watch-dispatcher/deploy.sh",
    "infrastructure/lambdas/scheduled-groom-dispatcher/deploy.sh",
    "infrastructure/lambdas/sf-telegram-notifier/deploy.sh",
    "infrastructure/lambdas/sf-watch-reclaim-sweep-handler/deploy.sh",
    "infrastructure/lambdas/sf-watch-spot-dispatcher/deploy.sh",
    "infrastructure/lambdas/spot-interruption-recorder/deploy.sh",
    "infrastructure/lambdas/spot-orphan-reaper/deploy.sh",
    "infrastructure/lambdas/ssm-liveness-poller/deploy.sh",
    "infrastructure/lambdas/ssm-reachability-probe/deploy.sh",
    "infrastructure/lambdas/substrate-health-gate/deploy.sh",
    "infrastructure/lambdas/sweep-artifact-monitor/deploy.sh",
    "infrastructure/lambdas/weekly-freshness-spot-dispatcher/deploy.sh",
    "infrastructure/lambdas/weekly-preflight/deploy.sh",
    "infrastructure/run_weekly_offcycle.sh",
    "infrastructure/setup_overseer_intake.sh",
}


def _code_only(body: str) -> str:
    """Shell source with FULL-LINE `#` comments removed.

    Load-bearing, not cosmetic. Several of these scripts explain the IAM
    boundary in prose containing the very command names scanned for, and this
    fleet has shipped the false positive three times in two days: a scan matched
    `ne-admin` inside a YAML comment, a test harvested a prose
    `systemctl enable --now` as an installer, and a drift guard matched
    `python3 -m pytest` inside the comment explaining the correct fix. Each time
    the pressure was to delete the rationale rather than fix the scan.

    Whole lines only. A `#` mid-line is left alone: stripping from the first `#`
    anywhere would eat a legitimate `"$URL#frag"` and is a false NEGATIVE risk
    this repo's sibling guard deliberately accepts, but here the assertion is
    about presence, so cutting less is the safer direction.
    """
    return "\n".join(
        line for line in body.splitlines() if not line.lstrip().startswith("#")
    )


def _shell_scripts() -> list[str]:
    out = subprocess.run(  # noqa: S603
        ["git", "ls-files", "*.sh"],  # noqa: S607
        cwd=_REPO_ROOT, capture_output=True, text=True, check=True,
    )
    paths = [line for line in out.stdout.split() if line]
    assert paths, "no shell scripts discovered — the glob or cwd is wrong"
    return paths


def _hand_rolled_creators() -> set[str]:
    """Every tracked shell script reaching `aws iam create-role` in CODE."""
    return {
        rel
        for rel in _shell_scripts()
        if "aws iam create-role" in _code_only((_REPO_ROOT / rel).read_text())
    } - _SANCTIONED


def test_no_new_script_hand_rolls_iam_role_creation() -> None:
    """The ratchet. A NEW script reaching `aws iam create-role` on its own is
    the defect this file exists to stop — it arrives carrying the I9045 probe
    because it is copied from a neighbour that has it."""
    new = sorted(_hand_rolled_creators() - _REGISTER)
    assert not new, (
        "these scripts reach `aws iam create-role` without going through "
        "_shared/apply_iam_policy.sh:\n  " + "\n  ".join(new)
        + "\n\nUse:\n"
        '  source "${SCRIPT_DIR}/../_shared/apply_iam_policy.sh"\n'
        '  apply_iam_policy "${ROLE_NAME}" "${POLICY_NAME}" '
        '"${SCRIPT_DIR}/iam-policy.json" "${TRUST_POLICY}"\n'
        "\nA hand-rolled `if ! aws iam get-role ... >/dev/null 2>&1` probe makes "
        "AccessDenied and NoSuchEntity the same observation, so a DENIED read is "
        "acted on as proof of absence. That is alpha-engine-config-I9045: four "
        "workflows red on every run for a week. `apply_iam_policy` reaches "
        "create-role only via probe_role_presence, which classifies a denial as "
        "`unknown` and creates nothing.\n"
        "Adding a name to _REGISTER instead is not the fix — the register may "
        "only shrink."
    )


def test_the_register_does_not_outlive_its_debt() -> None:
    """A register entry whose script has since migrated must be deleted. Left in
    place it is a standing exemption for a script that no longer needs one, and
    it would silently absorb a regression that re-introduced the hand-rolled
    probe there."""
    stale = sorted(_REGISTER - _hand_rolled_creators())
    assert not stale, (
        "these scripts no longer hand-roll `aws iam create-role`, so their "
        "entries in _REGISTER are stale. Delete each line "
        f"(tests/{Path(__file__).name}):\n  " + "\n  ".join(stale)
    )


def test_the_register_only_shrinks() -> None:
    """A hard ceiling, so growth cannot be laundered through an edit to the set
    literal in the same commit that adds an offender. Measured 2026-08-28: 45,
    down from 46 — thinktank-spot-dispatcher migrated in
    alpha-engine-config-I9114. Lower this number when you remove entries; never
    raise it."""
    assert len(_REGISTER) <= 45, (
        "the hand-rolled-IAM debt register grew. Migrate the script onto "
        "_shared/apply_iam_policy.sh instead (alpha-engine-config-I9207)."
    )


def test_the_shared_helper_is_the_only_sanctioned_creator() -> None:
    """The sanctioned set is not a second register. It names one file, and that
    file must actually still be the presence-gated implementation — otherwise
    this whole guard is asserting against a helper that has itself regressed."""
    assert _SANCTIONED == {"infrastructure/lambdas/_shared/apply_iam_policy.sh"}
    helper = _code_only(
        (_REPO_ROOT / "infrastructure/lambdas/_shared/apply_iam_policy.sh").read_text()
    )
    assert "probe_role_presence" in helper, (
        "apply_iam_policy.sh no longer defines the presence probe, so nothing "
        "distinguishes a denied read from an absent role (alpha-engine-config-I9045)."
    )
    assert "may_create_role" in helper, (
        "apply_iam_policy.sh no longer gates role creation, so the code-only "
        "auto-apply path can reach iam:CreateRole again."
    )
