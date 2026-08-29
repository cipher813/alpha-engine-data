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

2. `check_iam_policy_on_deploy` wrapped that call in `... || rc=$?` to classify
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

WHAT THE SECOND ROUND ADDED (run 33229043798, 2026-08-29)
---------------------------------------------------------
The fix above got `deploy-overseer-dispatcher` past `CreateRole` and no further.
Two more defects of the same class were live behind it:

3. The code-only path still called `aws iam put-role-policy` and classified the
   AccessDenied as expected. Least privilege (identity-access-policy.md §4) is
   not "hold no permission and keep making the call": every merge wrote a
   CloudTrail AccessDenied on iam:PutRolePolicy from the one identity that must
   never hold it. `check_iam_policy_on_deploy` is now
   `check_iam_policy_on_deploy` and mutates no IAM at all —
   `test_no_iam_mutating_verb_is_reachable_on_the_code_only_path` is the guard,
   and it fails on any verb, not only the two seen so far.

4. `get-role-policy` carried the SAME AccessDenied/NoSuchEntity conflation that
   PR1569 fixed one screen above it on `get-role`, so a denied read printed
   "no inline policy ... on the role yet — first apply" about a policy live
   since 2026-07-22. See probe_role_policy_state.

WHY THESE TESTS EXECUTE THE SHELL
---------------------------------
"A loop proven only by reading it is a loop nobody has run." Every case below
sources the real `_shared/apply_iam_policy.sh` and `_shared/deploy_run.sh` and
runs them against a fake `aws` on PATH that reproduces the exact stderr AWS
emits. Asserting on the file's TEXT would have passed against the broken
version: the broken version also contained the word `AccessDenied`.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest
import yaml

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


def _fake_aws(
    tmp_path: Path,
    *,
    behaviours: dict[str, tuple[int, str]],
    stdouts: dict[str, str] | None = None,
) -> Path:
    """A fake `aws` binary. `behaviours` maps an "iam get-role"-style command
    prefix to (exit_code, stderr_text); `stdouts` maps the same prefixes to the
    text the command writes to STDOUT on success. Every invocation is appended
    to `calls.log`, which is what the "never attempted" assertions read."""
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    log = tmp_path / "calls.log"
    arms = []
    for prefix, (rc, stderr) in behaviours.items():
        arms.append(f'  "{prefix}") echo {stderr!r} >&2; exit {rc} ;;')
    for n, (prefix, out) in enumerate(sorted((stdouts or {}).items())):
        if prefix in behaviours:
            continue
        out_file = tmp_path / f"stdout_{n}.txt"
        out_file.write_text(out)
        arms.append(f'  "{prefix}") cat "{out_file}"; exit 0 ;;')
    cases = "\n".join(arms)
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


# Every IAM verb that MUTATES. The guard below fails on any of them, not only
# the two this defect happened to reach: `spot-interruption-recorder` once hit
# an identical wall and patched its own copy, leaving three siblings exposed.
_IAM_MUTATING_VERBS = (
    "create-role",
    "put-role-policy",
    "attach-role-policy",
    "update-assume-role-policy",
    "delete-role-policy",
    "detach-role-policy",
    "tag-role",
    "untag-role",
    "put-user-policy",
    "create-policy",
    "attach-user-policy",
    "update-role",
    "create-instance-profile",
    "add-role-to-instance-profile",
    "put-role-permissions-boundary",
)


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
        f'check_iam_policy_on_deploy "{ROLE}" pol "$POLICY_FILE"; '
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
        f'check_iam_policy_on_deploy "{ROLE}" pol "$POLICY_FILE"',
        denied_everywhere,
    )
    assert "Creating IAM role" not in (r.stdout + r.stderr)
    assert "create-role" not in _calls(tmp_path), (
        "the code-only path attempted iam:CreateRole — a grant the CI identity "
        "does not and must not hold"
    )


def test_no_iam_mutating_verb_is_reachable_on_the_code_only_path(
    tmp_path: Path, denied_everywhere: Path
) -> None:
    """THE CLASS GUARD, not the instance guard.

    PR1569 stopped `create-role` on this path and left `put-role-policy`
    reaching AWS on every merge — a CloudTrail AccessDenied per deploy from the
    one identity the single-writer rule says must never hold that grant. Naming
    only the verbs already seen is how the class survives a fix; this asserts
    against the whole set."""
    r = _harness(
        tmp_path,
        f'check_iam_policy_on_deploy "{ROLE}" pol "$POLICY_FILE"; echo "RC=$?"',
        denied_everywhere,
    )
    assert "RC=0" in r.stdout, r.stdout + r.stderr
    calls = _calls(tmp_path)
    reached = [v for v in _IAM_MUTATING_VERBS if v in calls]
    assert not reached, (
        f"the code-only deploy path issued IAM-mutating call(s) {reached}. "
        f"Least privilege is not 'hold no permission and keep making the "
        f"call' (identity-access-policy.md §4).\ncalls:\n{calls}"
    )


def test_a_denied_policy_read_is_never_reported_as_an_absent_policy(
    tmp_path: Path, denied_everywhere: Path
) -> None:
    """The second instance of the I9045 conflation, one screen below the first.

    `get-role-policy` is denied to the CI identity exactly as `get-role` is, and
    the old code read that denial as `absent`, printing

        no inline policy alpha-engine-overseer-dispatcher-policy on the role
        yet — first apply.

    verbatim in run 33229043798 — about a policy live since 2026-07-22."""
    r = _harness(
        tmp_path,
        f'check_iam_policy_on_deploy "{ROLE}" pol "$POLICY_FILE"; '
        'echo "VERDICT=${IAM_POLICY_CHECK_VERDICT}"',
        denied_everywhere,
    )
    out = r.stdout + r.stderr
    assert "VERDICT=unknown" in r.stdout, out
    assert "no inline policy" not in out, (
        "a denied read was reported as an absent policy"
    )
    assert "cannot verify" in r.stderr


def test_a_denied_identity_is_told_what_it_could_not_check(
    tmp_path: Path, denied_everywhere: Path
) -> None:
    """Unverifiable is a state that must be SAID. `no data` is never rendered
    as green (principle 7), and the line has to name the standing detector so
    the reader knows the gap is covered elsewhere."""
    r = _harness(
        tmp_path,
        f'check_iam_policy_on_deploy "{ROLE}" pol "$POLICY_FILE"',
        denied_everywhere,
    )
    assert "check-drift.py" in r.stderr
    assert ROLE in r.stderr


def test_drift_between_live_and_disk_names_the_operator_command(
    tmp_path: Path,
) -> None:
    """A privileged caller running flagless deploy.sh no longer gets a silent
    apply — it gets the drift and the exact command. That trade is the point of
    the change, so it is pinned: a DIFFERS verdict that named no command would
    be a detector nobody can act on."""
    bindir = _fake_aws(
        tmp_path,
        behaviours={"iam get-role": (0, "")},
        stdouts={
            "iam get-role-policy": (
                '{"Version":"2012-10-17","Statement":['
                '{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}'
            )
        },
    )
    r = _harness(
        tmp_path,
        f'check_iam_policy_on_deploy "{ROLE}" pol "$POLICY_FILE"; '
        'echo "VERDICT=${IAM_POLICY_CHECK_VERDICT}"',
        bindir,
    )
    assert "VERDICT=different" in r.stdout, r.stdout + r.stderr
    assert "IAM DRIFT" in r.stderr
    assert "--apply-iam" in r.stderr
    assert "put-role-policy" not in _calls(tmp_path), (
        "a privileged identity still wrote IAM from the code-only path"
    )


def test_a_matching_policy_reports_no_drift(tmp_path: Path) -> None:
    """The quiet case must be quiet AND positive: `same` is a measurement, and
    reporting nothing at all would make a working check indistinguishable from
    a check that never ran."""
    bindir = _fake_aws(
        tmp_path,
        behaviours={"iam get-role": (0, "")},
        stdouts={"iam get-role-policy": '{"Statement":[],"Version":"2012-10-17"}'},
    )
    r = _harness(
        tmp_path,
        f'check_iam_policy_on_deploy "{ROLE}" pol "$POLICY_FILE"; '
        'echo "VERDICT=${IAM_POLICY_CHECK_VERDICT}"',
        bindir,
    )
    assert "VERDICT=same" in r.stdout, r.stdout + r.stderr
    assert "no drift" in r.stdout
    assert "IAM DRIFT" not in r.stderr


def test_an_unreadable_policy_file_is_fatal(tmp_path: Path) -> None:
    """A broken checkout is not a drift verdict. The checker replaced a call
    that could abort the deploy; if every one of its own failure modes now
    returns 0, the step has become unfalsifiable."""
    bindir = _fake_aws(tmp_path, behaviours={"iam get-role": (0, "")})
    r = _harness(
        tmp_path,
        f'check_iam_policy_on_deploy "{ROLE}" pol /nonexistent/iam-policy.json; '
        'echo "RC=$?"',
        bindir,
    )
    assert "RC=1" in r.stdout, r.stdout + r.stderr
    assert "not readable" in r.stderr


def test_a_role_that_is_genuinely_absent_names_the_bootstrap_command(
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
        f'check_iam_policy_on_deploy "{ROLE}" pol "$POLICY_FILE"; '
        'echo "RC=$?" "VERDICT=${IAM_POLICY_CHECK_VERDICT}"',
        bindir,
    )
    assert "RC=0" in r.stdout, r.stdout + r.stderr
    assert "VERDICT=role-absent" in r.stdout
    assert "create-role" not in _calls(tmp_path)
    assert "--bootstrap" in r.stderr


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


def test_the_operator_apply_path_still_writes_the_policy(tmp_path: Path) -> None:
    """The half that proves the fix did not simply delete IAM handling.
    `apply_iam_policy` — reached only via --bootstrap and --apply-iam, where an
    operator has stated the intent with a flag — must still call
    put-role-policy."""
    bindir = _fake_aws(
        tmp_path,
        behaviours={"iam get-role": (0, "")},
        stdouts={"iam get-role-policy": '{"Statement":[],"Version":"2012-10-17"}'},
    )
    r = _harness(
        tmp_path,
        f'apply_iam_policy "{ROLE}" pol "$POLICY_FILE" "$TRUST"; echo "RC=$?"',
        bindir,
    )
    assert "RC=0" in r.stdout, r.stdout + r.stderr
    assert "put-role-policy" in _calls(tmp_path), (
        "the operator apply path stopped writing IAM — the fix over-reached"
    )


def test_the_operator_apply_path_does_not_claim_absent_on_a_denied_read(
    tmp_path: Path, denied_everywhere: Path
) -> None:
    """The same conflation, on the path that still writes. Claiming `first
    apply` about a policy the caller merely could not read is the misleading
    half of I7444 all over again."""
    r = _harness(
        tmp_path,
        f'apply_iam_policy "{ROLE}" pol "$POLICY_FILE" "$TRUST"',
        denied_everywhere,
    )
    assert "no inline policy" not in (r.stdout + r.stderr)


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


# ---------------------------------------------------------------------------
# THE OTHER HALF OF THE SAME DEFECT: the alert_classes publish step.
#
# `run()` calls `exit`, so `run ... || echo "non-fatal"` is unreachable — the
# I8125 class. The IAM wrapper above was one instance; this was the LAST
# remaining `run ... ||` site in any deploy.sh in this repo, and it is what
# actually reddened run 33229043798 (exit 255):
#
#   WARN alert_classes extraction failed (non-fatal): No module named 'yaml'
#   aws: [ERROR]: The user-provided path /tmp/tmp.HAx0uXw8RV/alert_classes.json
#        does not exist.
#   ERROR: command failed (exit 255): aws s3 cp ...
#
# INDEPENDENT of the IAM defect above, not a cascade from it: the IAM step
# returned 0 in that run. A producer swallowed its own failure and the consumer
# two lines later exploded on the file that was therefore never written.
#
# The step is EXTRACTED FROM THE REAL deploy.sh rather than restated here — a
# copy would keep passing after the script changed underneath it.
# ---------------------------------------------------------------------------

_OVERSEER_DEPLOY = (
    _REPO_ROOT / "infrastructure" / "lambdas" / "overseer-dispatcher" / "deploy.sh"
)
_REAL_YAML = Path(yaml.__file__).parent


def _publish_step() -> str:
    """The alert_classes publish step, lifted verbatim from deploy.sh."""
    text = _OVERSEER_DEPLOY.read_text()
    start = text.index('echo "Publishing alert_classes to S3..."')
    end = text.index('\necho "Done."', start)
    step = text[start:end]
    assert "run_tolerating" in step, (
        "the publish step no longer uses run_tolerating — if it went back to "
        "`run ... || echo`, that tolerance is unreachable (run() exits)"
    )
    return step


def _publish_harness(
    tmp_path: Path, bindir: Path, *, with_yaml: bool
) -> subprocess.CompletedProcess:
    """Run the real publish step with a controllable `python3` and `aws`.

    The `python3` shim runs the real interpreter with `-S`, so site-packages is
    out of reach and `import yaml` succeeds only via the PYTHONPATH the step
    sets — which is the whole mechanism under test. Without that, a runner that
    happens to ship PyYAML would make the failing case unreproducible."""
    pkg = tmp_path / "pkg"
    pkg.mkdir()
    if with_yaml:
        # Exactly what step 1 does: this lambda's requirements.txt (pyyaml>=6.0)
        # installed into ${PKG}.
        shutil.copytree(_REAL_YAML, pkg / "yaml")

    (bindir / "python3").write_text(
        f'#!/usr/bin/env bash\nexec "{sys.executable}" -S "$@"\n'
    )
    (bindir / "python3").chmod(0o755)

    script = tmp_path / "publish.sh"
    script.write_text(
        textwrap.dedent(
            f"""\
            set -euo pipefail
            DRY_RUN=false
            source "{_SHARED}/deploy_run.sh"
            PKG="{pkg}"
            SCRIPT_DIR="{_OVERSEER_DEPLOY.parent}"
            """
        )
        + _publish_step()
    )
    return subprocess.run(  # noqa: S603
        ["/bin/bash", str(script)],  # noqa: S607
        capture_output=True,
        text=True,
        env={"PATH": f"{bindir}:/usr/bin:/bin", "HOME": str(tmp_path)},
        timeout=120,
        check=False,
    )


def test_the_publish_step_reads_the_pyyaml_it_is_about_to_ship(
    tmp_path: Path,
) -> None:
    """The fix: PYTHONPATH=${PKG}. PyYAML is already in this lambda's
    requirements.txt and was installed into ${PKG} by step 1, so the deploy
    parses playbooks.yaml with the same pyyaml it bundles — no dependency added
    to the runner, nothing to keep in sync."""
    bindir = _fake_aws(tmp_path, behaviours={})
    r = _publish_harness(tmp_path, bindir, with_yaml=True)
    assert r.returncode == 0, r.stdout + r.stderr
    assert "Extracted" in r.stdout, r.stdout + r.stderr
    assert "s3 cp" in _calls(tmp_path), (
        "the projection was never uploaded"
    )


def test_a_missing_yaml_is_fatal_at_the_producer_not_at_the_consumer(
    tmp_path: Path,
) -> None:
    """THE REGRESSION, stated at the layer that broke.

    This repo is a PRODUCER (AGENTS.md): a writer that swallows and returns
    partial output is a silent corruption of every consumer. The old code
    printed `WARN ... (non-fatal)`, exited 0 without writing the file, and let
    `aws s3 cp` fail on the absence — so the log named a missing PATH where the
    cause was a missing MODULE, and the step that actually failed reported
    success."""
    bindir = _fake_aws(tmp_path, behaviours={})
    r = _publish_harness(tmp_path, bindir, with_yaml=False)
    assert r.returncode != 0, (
        "an extraction failure was tolerated — the producer swallowed"
    )
    assert "yaml" in (r.stdout + r.stderr), r.stdout + r.stderr
    assert "s3 cp" not in _calls(tmp_path), (
        "the upload was attempted after the extraction failed: that is the "
        "cascade that made the log name the wrong cause"
    )


def test_the_denied_upload_is_tolerated_and_reachably_so(tmp_path: Path) -> None:
    """`github-actions-lambda-deploy` holds no s3:PutObject on
    alpha-engine-research/overseer/ (measured 2026-08-29), so this ONE cause is
    expected from CI. The point of the change is that the tolerance now
    EXECUTES: `run ... || echo` never did, because run() exits."""
    bindir = _fake_aws(
        tmp_path,
        behaviours={
            "s3 cp": (
                1,
                "An error occurred (AccessDenied) when calling the PutObject "
                "operation: Access Denied",
            )
        },
    )
    r = _publish_harness(tmp_path, bindir, with_yaml=True)
    assert r.returncode == 0, (
        "a denied upload aborted the deploy — the tolerance is unreachable "
        "again\n" + r.stdout + r.stderr
    )
    assert "tolerated" in r.stdout, r.stdout + r.stderr


def test_any_other_upload_failure_still_fails_the_deploy(tmp_path: Path) -> None:
    """The tolerance is exactly one named cause. `|| echo ...non-fatal` was
    always too broad — it would have swallowed a wrong bucket, a malformed key
    and an expired token identically."""
    bindir = _fake_aws(
        tmp_path,
        behaviours={
            "s3 cp": (
                1,
                "An error occurred (NoSuchBucket) when calling the PutObject "
                "operation: The specified bucket does not exist",
            )
        },
    )
    r = _publish_harness(tmp_path, bindir, with_yaml=True)
    assert r.returncode != 0, (
        "a NoSuchBucket was tolerated — the publish became unfalsifiable\n"
        + r.stdout
        + r.stderr
    )
    assert "NoSuchBucket" in r.stderr
