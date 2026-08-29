"""The code-only deploy path must never attempt IAM role CREATION, and its
tolerance wrapper must survive `run()`'s `exit`.

THE DEFECT THIS PINS (measured 2026-08-28)
------------------------------------------
Four workflows in this repo failed 100% of their runs on `main` for a week —
`deploy-alert-drain-dispatcher`, `deploy-overseer-dispatcher`,
`deploy-alert-drain-liveness-probe`, `deploy-overseer-liveness-probe`. Every
one shipped its Lambda CODE successfully and then died at exit 254 on a line
reading `Creating IAM role: alpha-engine-alert-drain-dispatcher-role` — a role
that has existed since 2026-07-22. The AWS error text appeared NOWHERE in the
job log.

Two independent defects, both closed here:

1. `apply_iam_policy` probed the role with
   `aws iam get-role ... >/dev/null 2>&1`, which makes AccessDenied and
   NoSuchEntity the same observation. `github-actions-lambda-deploy` holds
   `iam:GetRole` on exactly two roles and on no lambda execution role — by
   design (infrastructure/iam/README.md, single-writer rule). So the probe was
   DENIED, read as "absent", and the script went on to call `iam:CreateRole`,
   a grant the CI identity does not and must not hold.

2. `apply_iam_policy_on_deploy` wrapped that call in `... || rc=$?` to classify
   an AccessDenied as the one tolerated failure. But `run()` in
   `_shared/deploy_run.sh` calls `exit`, not `return` (alpha-engine-config-
   I8033), and `exit` inside a function terminates the SHELL — `||` guards a
   RETURN and there is nothing to guard. The classifier was unreachable from
   the day run() started exiting (2026-08-21, the exact commit at which all
   four workflows went red), and the captured stderr was never replayed, which
   is why the log carried no AWS error at all.

   This is the same class alpha-engine-config-I8125 fixed for `run ... || true`
   call sites with `run_tolerating`. It fixed the call-site shape and missed
   the function-level wrapper.

WHY THESE TESTS EXECUTE THE SHELL
---------------------------------
"A loop proven only by reading it is a loop nobody has run." Every case below
sources the real `_shared/apply_iam_policy.sh` and `_shared/deploy_run.sh` and
runs them against a fake `aws` on PATH that reproduces the exact stderr AWS
emits. Asserting on the file's TEXT would have passed against the broken
version: the broken version also contained the word `AccessDenied`.
"""

from __future__ import annotations

import subprocess
import textwrap
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SHARED = _REPO_ROOT / "infrastructure" / "lambdas" / "_shared"

# The literal stderr `aws iam` emits for each condition. Reproduced verbatim so
# a change in how the classifier greps is caught here rather than in CI.
_DENIED = (
    "An error occurred (AccessDenied) when calling the {op} operation: "
    "User: arn:aws:sts::711398986525:assumed-role/github-actions-lambda-deploy/"
    "GitHubActions is not authorized to perform: iam:{op} on resource: "
    "arn:aws:iam::711398986525:role/{role}"
)
_NO_SUCH_ENTITY = (
    "An error occurred (NoSuchEntity) when calling the GetRole operation: "
    "The role with name {role} cannot be found."
)

ROLE = "alpha-engine-alert-drain-dispatcher-role"


def _fake_aws(tmp_path: Path, *, behaviours: dict[str, tuple[int, str]]) -> Path:
    """A fake `aws` binary. `behaviours` maps an "iam get-role"-style command
    prefix to (exit_code, stderr_text). Every invocation is appended to
    `calls.log`, which is what the "never attempted" assertions read."""
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    log = tmp_path / "calls.log"
    cases = "\n".join(
        f'  "{prefix}") echo {stderr!r} >&2; exit {rc} ;;'
        for prefix, (rc, stderr) in behaviours.items()
    )
    fake = bindir / "aws"
    fake.write_text(
        textwrap.dedent(
            f"""\
            #!/usr/bin/env bash
            echo "$*" >> {log}
            case "$1 $2" in
            {cases}
              *) exit 0 ;;
            esac
            """
        )
    )
    fake.chmod(0o755)
    return bindir


def _harness(tmp_path: Path, body: str, bindir: Path) -> subprocess.CompletedProcess:
    policy = tmp_path / "iam-policy.json"
    policy.write_text('{"Version":"2012-10-17","Statement":[]}')
    script = tmp_path / "harness.sh"
    script.write_text(
        textwrap.dedent(
            f"""\
            set -uo pipefail
            DRY_RUN=false
            source "{_SHARED}/deploy_run.sh"
            source "{_SHARED}/apply_iam_policy.sh"
            POLICY_FILE="{policy}"
            TRUST='{{"Version":"2012-10-17"}}'
            {body}
            """
        )
    )
    # A minimal, explicit environment: the fake `aws` must win over any real
    # one, and nothing from the developer's shell may leak into the harness.
    shell_env = {"PATH": f"{bindir}:/usr/bin:/bin", "HOME": str(tmp_path)}
    return subprocess.run(  # noqa: S603
        ["/bin/bash", str(script)],  # noqa: S607
        capture_output=True,
        text=True,
        env=shell_env,
        timeout=120,
        check=False,
    )


def _calls(tmp_path: Path) -> str:
    log = tmp_path / "calls.log"
    return log.read_text() if log.exists() else ""


@pytest.fixture
def denied_everywhere(tmp_path: Path) -> Path:
    """The live CI identity: no iam:GetRole, no iam:CreateRole, no
    iam:PutRolePolicy on a lambda execution role."""
    return _fake_aws(
        tmp_path,
        behaviours={
            "iam get-role": (254, _DENIED.format(op="GetRole", role=ROLE)),
            "iam create-role": (254, _DENIED.format(op="CreateRole", role=ROLE)),
            "iam put-role-policy": (
                254, _DENIED.format(op="PutRolePolicy", role=ROLE)),
            "iam get-role-policy": (
                254, _DENIED.format(op="GetRolePolicy", role=ROLE)),
        },
    )


def test_the_code_only_path_survives_an_identity_with_no_iam_writes(
    tmp_path: Path, denied_everywhere: Path
) -> None:
    """THE REGRESSION. Exit 254 here is a deploy that shipped code and then
    reported failure — indistinguishable from a broken change."""
    r = _harness(
        tmp_path,
        f'apply_iam_policy_on_deploy "{ROLE}" pol "$POLICY_FILE" "$TRUST"; '
        'echo "RC=$?"',
        denied_everywhere,
    )
    assert "RC=0" in r.stdout, (
        f"the code-only deploy path aborted.\nstdout:\n{r.stdout}\n"
        f"stderr:\n{r.stderr}"
    )
    assert r.returncode == 0


def test_a_denied_get_role_is_never_read_as_an_absent_role(
    tmp_path: Path, denied_everywhere: Path
) -> None:
    """The half that made the log unreadable: `Creating IAM role: <name>` was
    printed for a role that had existed for five weeks."""
    r = _harness(
        tmp_path,
        f'apply_iam_policy_on_deploy "{ROLE}" pol "$POLICY_FILE" "$TRUST"',
        denied_everywhere,
    )
    assert "Creating IAM role" not in (r.stdout + r.stderr)
    assert "create-role" not in _calls(tmp_path), (
        "the code-only path attempted iam:CreateRole — a grant the CI identity "
        "does not and must not hold"
    )


def test_the_tolerated_access_denied_is_reported_not_swallowed(
    tmp_path: Path, denied_everywhere: Path
) -> None:
    """Tolerated is not the same as silent: the operator must still be told the
    policy did not land and what to run."""
    r = _harness(
        tmp_path,
        f'apply_iam_policy_on_deploy "{ROLE}" pol "$POLICY_FILE" "$TRUST"',
        denied_everywhere,
    )
    assert "IAM auto-apply skipped" in r.stderr
    assert "--apply-iam" in r.stderr


def test_the_aws_error_text_reaches_the_log(
    tmp_path: Path, denied_everywhere: Path
) -> None:
    """The captured stderr was lost entirely when run()'s `exit` killed the
    shell before the replay. A tolerated failure whose cause never printed is
    how a wrong tolerance survives."""
    r = _harness(
        tmp_path,
        f'apply_iam_policy_on_deploy "{ROLE}" pol "$POLICY_FILE" "$TRUST"',
        denied_everywhere,
    )
    assert "is not authorized to perform" in r.stderr


def test_a_role_that_is_genuinely_absent_is_not_created_from_the_deploy_path(
    tmp_path: Path,
) -> None:
    """NoSuchEntity is the one observation that DOES prove absence — and the
    answer is still not to create it here. Role creation is a bootstrap act
    reserved to an operator by the single-writer rule."""
    bindir = _fake_aws(
        tmp_path,
        behaviours={"iam get-role": (254, _NO_SUCH_ENTITY.format(role=ROLE))},
    )
    r = _harness(
        tmp_path,
        f'apply_iam_policy_on_deploy "{ROLE}" pol "$POLICY_FILE" "$TRUST"; '
        'echo "RC=$?"',
        bindir,
    )
    assert "RC=0" in r.stdout, r.stdout + r.stderr
    assert "create-role" not in _calls(tmp_path)
    assert "IAM role ABSENT" in r.stderr


def test_the_operator_bootstrap_path_still_creates_an_absent_role(
    tmp_path: Path,
) -> None:
    """The fix must not disarm --bootstrap. `apply_iam_policy` called directly
    (may_create_role defaults true) still creates on a proven NoSuchEntity."""
    bindir = _fake_aws(
        tmp_path,
        behaviours={"iam get-role": (254, _NO_SUCH_ENTITY.format(role=ROLE))},
    )
    r = _harness(
        tmp_path,
        # sleep is stubbed away: the real function waits 10s for IAM propagation.
        "sleep() { :; }\n"
        f'apply_iam_policy "{ROLE}" pol "$POLICY_FILE" "$TRUST"; echo "RC=$?"',
        bindir,
    )
    assert "RC=0" in r.stdout, r.stdout + r.stderr
    assert "create-role" in _calls(tmp_path), (
        "the operator bootstrap path stopped creating roles — the fix "
        "over-reached"
    )


def test_a_failure_that_is_not_a_permission_boundary_still_aborts_the_deploy(
    tmp_path: Path,
) -> None:
    """The tolerance is exactly one cause. A broken applier — a throttle, a
    malformed document, a missing binary — must still be fatal, or this becomes
    the `|| true` that alpha-engine-config-I7338 removed."""
    bindir = _fake_aws(
        tmp_path,
        behaviours={
            "iam get-role": (0, ""),
            "iam get-role-policy": (254, "boom"),
            "iam put-role-policy": (
                254,
                "An error occurred (MalformedPolicyDocument) when calling the "
                "PutRolePolicy operation: Syntax errors in policy.",
            ),
        },
    )
    r = _harness(
        tmp_path,
        f'apply_iam_policy_on_deploy "{ROLE}" pol "$POLICY_FILE" "$TRUST"; '
        'echo "RC=$?"',
        bindir,
    )
    assert "RC=0" not in r.stdout, (
        "a MalformedPolicyDocument was tolerated — the applier is broken and "
        "the deploy reported success"
    )
    assert "IAM auto-apply FAILED" in r.stderr
    assert "MalformedPolicyDocument" in r.stderr


def test_an_identity_that_can_write_iam_still_applies_the_policy(
    tmp_path: Path,
) -> None:
    """ne-admin / the operator path: nothing about the fix may stop a
    privileged caller from actually landing the policy."""
    bindir = _fake_aws(tmp_path, behaviours={"iam get-role": (0, "")})
    r = _harness(
        tmp_path,
        f'apply_iam_policy_on_deploy "{ROLE}" pol "$POLICY_FILE" "$TRUST"; '
        'echo "RC=$?"',
        bindir,
    )
    assert "RC=0" in r.stdout, r.stdout + r.stderr
    assert "put-role-policy" in _calls(tmp_path)
    assert "IAM role exists" in r.stdout


def test_the_verdict_survives_the_subshell(tmp_path: Path) -> None:
    """APPLY_IAM_POLICY_VERDICT is set inside the subshell the fix introduced.
    A variable that silently stopped escaping would make every caller reading
    it report `unknown` forever — the exact `absence of signal` class. The
    internal round-trip marker must not leak into the deploy log either.

    The absent-role case is used because its verdict (`role-absent`) is
    distinctive: `unknown` is a legitimate value the comparison can produce on
    its own, so asserting against it would pass on a broken round-trip."""
    bindir = _fake_aws(
        tmp_path,
        behaviours={"iam get-role": (254, _NO_SUCH_ENTITY.format(role=ROLE))},
    )
    r = _harness(
        tmp_path,
        f'apply_iam_policy_on_deploy "{ROLE}" pol "$POLICY_FILE" "$TRUST"; '
        'echo "VERDICT=${APPLY_IAM_POLICY_VERDICT}"',
        bindir,
    )
    assert "VERDICT=role-absent" in r.stdout, r.stdout + r.stderr
    assert "APPLY_IAM_POLICY_VERDICT=" not in r.stdout, (
        "the internal round-trip marker leaked into the deploy log"
    )


def test_run_exiting_inside_a_tolerance_wrapper_is_catchable(
    tmp_path: Path,
) -> None:
    """The generalised class, stated directly. `run()` calls `exit`; any future
    tolerance wrapper that guards it with `||` alone is unreachable. This
    asserts both halves — that run() still exits (I8033 has not regressed) and
    that the subshell idiom the fix uses actually catches it — so the next
    author has an executable statement of the rule, not a comment."""
    bindir = _fake_aws(tmp_path, behaviours={"iam get-role": (254, "denied")})
    unguarded = _harness(
        tmp_path,
        'rc=0; run aws iam get-role --role-name x || rc=$?; echo "RC=$rc"',
        bindir,
    )
    assert "RC=" not in unguarded.stdout, (
        "run() no longer exits — alpha-engine-config-I8033 has regressed, and "
        "a failing deploy command may again be non-fatal"
    )
    guarded = _harness(
        tmp_path,
        'rc=0; ( run aws iam get-role --role-name x ) || rc=$?; echo "RC=$rc"',
        bindir,
    )
    assert "RC=254" in guarded.stdout, guarded.stdout + guarded.stderr
