"""Preflight guard: ``deploy-infrastructure.sh`` MUST validate every Step
Function definition BEFORE it applies any of them (config#1897).

Root cause this pins: on 2026-07-07 a malformed intrinsic (an unbalanced
``commands.$`` in the daily SF, #676) passed the in-repo unit guard
(``TestIntrinsicsWellFormed``, which only paren-balances) and was rejected by
AWS at ``UpdateStateMachine`` time — POST-merge, on ``main``. Because the
deploy script updates state machines one at a time, the weekly SF had already
been updated when the daily SF was rejected, leaving the fleet stamped at mixed
SHAs (#677).

The structural fix (this test guards it): a validate-ALL preflight that calls
``aws stepfunctions validate-state-machine-definition`` — the SAME validation
AWS runs at deploy time, catching the broad malformed-intrinsic class the unit
guard can't — for every stamped definition, BEFORE the first S3 upload or
``update-state-machine``/``create-state-machine`` call, aborting all-or-nothing
if any fails. A resource-less IAM action, so the GHA deploy role must grant it.

This test fails loudly the moment the preflight is removed, moved after an
apply, stops covering a definition, or the IAM grant is dropped.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_INFRA = _REPO_ROOT / "infrastructure"
_DEPLOY = _INFRA / "deploy-infrastructure.sh"
# The stamped SF definition variables the deploy script builds + applies. Every
# one of these must be fed to the validate preflight. Includes the
# config#1897/I2544/I2545 advisory + modelzoo child pipelines added to main
# after this preflight was first written — a subset here repeats exactly the
# 2026-07-07 gap class this test guards against.
_STAMPED_VARS = (
    "$SAT_STAMPED",
    "$DAILY_STAMPED",
    "$EOD_STAMPED",
    "$GROOM_STAMPED",
    # alpha-engine-config-I2890 (2026-07-17): $ADVISORY_STAMPED and
    # $MODELZOO_STAMPED were retired with the I2544/I2545 child SFs — the
    # splits were reversed and the weekly SF carries the full inline pattern
    # in step_function.json again.
)


def _script_text() -> str:
    assert _DEPLOY.is_file(), f"missing {_DEPLOY}"
    return _DEPLOY.read_text()


def _first_index(text: str, pattern: str) -> int:
    """Char offset of the first regex match, or -1 if absent."""
    m = re.search(pattern, text)
    return m.start() if m else -1


def test_preflight_calls_validate_state_machine_definition() -> None:
    text = _script_text()
    assert "validate-state-machine-definition" in text, (
        "deploy-infrastructure.sh must run "
        "`aws stepfunctions validate-state-machine-definition` as a preflight "
        "(config#1897) — no invocation found."
    )


def test_every_stamped_definition_is_validated() -> None:
    """The preflight must cover ALL SIX SF definitions, not a subset — the
    2026-07-07 gap was exactly a subset (unbalanced intrinsic slipped through)."""
    text = _script_text()
    # Restrict to the validate helper call sites so we assert the preflight
    # itself covers each definition (not merely that the var appears anywhere).
    validate_calls = "\n".join(
        line for line in text.splitlines() if "validate_sf_definition" in line
    )
    for var in _STAMPED_VARS:
        assert var in validate_calls, (
            f"stamped definition {var} is not passed to the validate preflight — "
            "every definition the script deploys must be validated (config#1897)."
        )


def test_validation_runs_before_any_apply() -> None:
    """All-or-nothing: the validate preflight must precede the first S3 upload
    AND the first update/create-state-machine call, so a bad definition aborts
    the deploy while nothing has been applied yet (no mixed-SHA fleet)."""
    text = _script_text()
    validate_at = _first_index(text, r"validate-state-machine-definition")
    upload_at = _first_index(text, r"aws s3 cp .*s3://\$BUCKET/infrastructure/")
    update_at = _first_index(text, r"aws stepfunctions (update|create)-state-machine")

    assert validate_at != -1
    assert upload_at != -1, "expected an S3 upload of the SF definitions"
    assert update_at != -1, "expected an update/create-state-machine apply"
    assert validate_at < upload_at, (
        "validate preflight must run BEFORE uploading definitions to S3 "
        "(config#1897)."
    )
    assert validate_at < update_at, (
        "validate preflight must run BEFORE any update/create-state-machine "
        "call, or a bad definition partially applies (config#1897)."
    )


def test_abort_keys_on_result_field_not_diagnostics() -> None:
    """AWS documents that diagnostic codes/wording may change; the pass/fail
    decision must key on the `result` field (OK|FAIL) only."""
    text = _script_text()
    assert "result" in text, (
        "preflight must read the `result` field from "
        "validate-state-machine-definition output (config#1897)."
    )
    # A hard failure path must exist (the script aborts on FAIL).
    assert re.search(r"VALIDATION_FAILED=true", text), (
        "preflight must set a failure flag and abort when a definition is "
        "invalid (config#1897)."
    )
    assert re.search(r"exit 1", text)


# test_gha_deploy_role_grants_validate_action was ported to nous-ergon-ops
# (infrastructure/iam/github-actions-lambda-deploy.json now lives there).
# The invariant (states:ValidateStateMachineDefinition is granted to the
# GHA deploy role) is enforced in nous-ergon-ops/tests/ — the IAM files
# this test read have been removed from this repo (infra/drop-iam-moved-to-ops).


# --- the PRE-merge half of the same preflight ----------------------------
#
# config#1897 put the AWS validator in the deploy script. That is post-merge by
# construction, and its all-or-nothing abort — correct in itself, since it keeps
# the fleet off mixed SHAs — means one bad definition blocks the deploy of ALL of
# them. Measured 2026-08-19/20 (alpha-engine-config-I7798): an illegal ASL escape
# in the WEEKLY definition blocked three consecutive deploys, froze the live SF
# stamp one SHA behind main, and halted the next morning's
# ne-preopen-trading-pipeline at DeployDriftGate — a full unmanaged trading
# session, from quoting in a different pipeline's stage.
#
# The fix is not a second validator: it is the SAME code path, reachable before
# the merge. `--validate-only` stamps and validates exactly as the deploy does,
# then exits BEFORE the first mutation, so the PR is graded by the artifact the
# deploy would apply. These tests fail if that mode is removed, if it stops
# covering a definition, if it gains a mutation, or if CI stops calling it.

_WORKFLOW = _REPO_ROOT / ".github" / "workflows" / "sf-definition-validate.yml"

_MUTATING_CALLS = (
    "aws s3 cp",
    "update-state-machine",
    "create-state-machine",
    "deploy --stack-name",
    "create-stack",
    "update-stack",
)


def test_validate_only_mode_exists() -> None:
    text = _script_text()
    assert "--validate-only" in text, (
        "deploy-infrastructure.sh must offer a --validate-only mode so the "
        "SAME validator can run pre-merge (alpha-engine-config-I7798)."
    )
    assert "VALIDATE_ONLY=true" in text


def test_validate_only_exits_after_the_preflight_and_before_any_mutation() -> None:
    """--validate-only must reach the validate-ALL preflight and stop there.

    If its exit sat BEFORE the preflight it would validate nothing; if it sat
    AFTER a mutation it would write to production from a pull request."""
    text = _script_text()
    validate_at = _first_index(text, r"validate_sf_definition\s+\"\$SAT_STAMPED\"")
    assert validate_at != -1, "validate preflight call site not found"

    exit_at = text.find("Validate-only complete")
    assert exit_at != -1, (
        "--validate-only must announce and take its own exit after the "
        "preflight (alpha-engine-config-I7798)."
    )
    assert validate_at < exit_at, (
        "--validate-only exits before the validate-ALL preflight — it would "
        "validate nothing."
    )

    # Comments in this script quote the very API calls being searched for, so
    # the scan runs over executable lines only.
    code = "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("#")
    )
    code_exit_at = code.find("Validate-only complete")
    assert code_exit_at != -1
    for call in _MUTATING_CALLS:
        at = _first_index(code, re.escape(call))
        if at == -1 or at > code_exit_at:
            continue
        # A mutating call reachable before the validate-only exit must sit
        # inside a branch the flag skips.
        guard_at = code.find("if $VALIDATE_ONLY; then")
        assert guard_at != -1 and guard_at < at, (
            f"mutating call {call!r} is reachable under --validate-only — a "
            f"pull-request check must not write to production."
        )


def test_ci_calls_validate_only_on_pull_request() -> None:
    assert _WORKFLOW.is_file(), f"missing {_WORKFLOW}"
    wf = _WORKFLOW.read_text()
    assert "pull_request:" in wf, (
        "the SF-definition check must run on pull_request — running it only on "
        "main reproduces the post-merge-only gap it exists to close."
    )
    assert "--validate-only" in wf, (
        "the workflow must invoke deploy-infrastructure.sh --validate-only, not "
        "reimplement the validator."
    )
    assert "ne-github-sf-validate" in wf, (
        "the workflow must assume the minimal-privilege validate role, never "
        "the deploy role (alpha-engine-config-I7798)."
    )
    assert "github-actions-lambda-deploy" not in wf
