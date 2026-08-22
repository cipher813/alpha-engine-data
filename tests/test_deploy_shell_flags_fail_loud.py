"""Every infrastructure/lambdas/*/deploy.sh must carry `set -euo pipefail`.

alpha-engine-config-I8033. Bootstrapping alpha-engine-preopen-deploy-
readiness-probe on 2026-08-21, deploy.sh's final step — the code upload —
failed with a mid-flight connection close, printed an `aws: [ERROR]` line,
and the SCRIPT EXITED 0. The outcome was benign only because create-function
had already uploaded identical bytes; on the ordinary (non-bootstrap) path
the same failure ships nothing and reports success.

Root cause was two-layered:

  1. The `run()` helper every deploy.sh used did not propagate a failing
     command's exit status to the script (fixed by moving to the shared,
     status-propagating `run()` in `_shared/deploy_run.sh`).
  2. `set -euo pipefail` is necessary but NOT sufficient — measured the same
     day, the AWS CLI itself can print an error and still return exit 0 for
     a mid-stream connection failure on a large `--zip-file` upload. The
     durable fix is `_shared/deploy_run.sh::verify_code_deployed`, which
     reads back the function's live CodeSha256 and compares it to the
     artifact just built, independent of what the CLI claims.

This test is the narrow, mechanical half of the fix: `set -euo pipefail`
closes the FIRST layer (an ordinary nonzero exit — from `python3`, `zip`,
`aws iam ...`, a bad substitution, etc. — must still abort the script) and
must never silently regress. It does not and cannot verify the second layer
(that requires reading live AWS state); that is
`test_deploy_shell_functions_are_defined.py`'s job for reachability of
`verify_code_deployed` itself, plus the read-back logic's own docstring.

WHY A DERIVED SCOPE AND NOT A HARDCODED LIST. 43 deploy.sh scripts exist
today; this repo grows a lambda most weeks (data-spot-dispatcher was itself
added because a prior migration shipped source + IAM + SF wiring with NO
deploy.sh — config-I1767). A hardcoded list of "the 43 known scripts" leaves
script #44 uncovered on the day it lands, silently. The universe here is
every `infrastructure/lambdas/*/deploy.sh` git-tracked file, discovered by
glob against `git ls-files` — a new deploy.sh is in-scope automatically.

WHY NOT SHELLCHECK. shellcheck flags unquoted variables and dozens of other
shapes, but does not have a check that FAILS a script for the *absence* of
`set -e`/`set -u`/`set -o pipefail` — SC2154-adjacent advice is informational
(and not enabled by `--severity=error`), never a hard failure. This test is
the derived, fail-loud, no-`set -e`-absent-is-fine substitute.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent

# Accepts any of the standard fail-loud combinations, in either flag or
# longopt spelling, and in any order/grouping bash allows:
#   set -euo pipefail / set -eu -o pipefail / set -e; set -u; set -o pipefail
# `pipefail` MUST be present: a deploy.sh piping a failing command into a
# `--query`/`--output text`/`grep` (several here do) needs it to fail loud on
# the pipeline's real failure rather than reporting the last stage's status.
#
# A whole `set ...` invocation (everything up to end-of-line, `;`, or `#`),
# tokenized by whitespace below rather than parsed token-by-token in the
# regex — bash's `-o` takes its argument as the NEXT WORD regardless of
# whether `o` was combined into an earlier short-flag cluster (`-euo
# pipefail`) or given standalone (`-eu -o pipefail`), which a single regex
# alternation cannot express without re-deriving a mini shell tokenizer.
_SET_LINE_RE = re.compile(r"^[ \t]*set[ \t]+([^\n;#]+)", re.MULTILINE)


def _lambda_deploy_scripts() -> list[Path]:
    """Every git-tracked infrastructure/lambdas/*/deploy.sh, discovered from
    the tree rather than hardcoded — see module docstring."""
    out = subprocess.run(
        ["git", "-C", str(_REPO_ROOT), "ls-files", "--", "infrastructure/lambdas/*/deploy.sh"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    files = sorted({_REPO_ROOT / p for p in out})
    assert files, (
        "derived-test precondition failed: found ZERO tracked "
        "infrastructure/lambdas/*/deploy.sh files. This guard measures "
        "nothing in that state."
    )
    # Precondition: this repo has historically had 40+ such scripts
    # (alpha-engine-config-I8033 enumerated 43 on 2026-08-21). A count this
    # low means the glob itself is broken, not that the fleet shrank.
    assert len(files) >= 20, (
        f"derived-test precondition failed: only {len(files)} deploy.sh "
        "files found under infrastructure/lambdas/ — expected 40+. The glob "
        "is almost certainly broken rather than the fleet having shrunk; "
        "fix _lambda_deploy_scripts() before trusting this guard."
    )
    return files


def _has_all_flags(text: str) -> tuple[bool, bool, bool]:
    """(-e present, -u present, pipefail present) anywhere the script sets
    shell options, unioned across every `set ...` invocation (some scripts
    split `set -eu` and `set -o pipefail` onto separate lines)."""
    has_e = has_u = has_pipefail = False
    for m in _SET_LINE_RE.finditer(text):
        tokens = m.group(1).split()
        expect_o_arg = False
        for tok in tokens:
            if expect_o_arg:
                if tok == "pipefail":
                    has_pipefail = True
                expect_o_arg = False
                continue
            if tok == "-o":
                expect_o_arg = True
                continue
            if tok == "pipefail":
                # bare `pipefail` with no preceding `-o` is not an option
                # argument to `set` — ignore (defensive; not a real shape).
                continue
            if not tok.startswith("-") or tok.startswith("--"):
                continue
            # A short-flag cluster, e.g. `-euo`. `o` inside it means the
            # NEXT WORD is that flag's argument, exactly as for standalone
            # `-o`.
            if "e" in tok:
                has_e = True
            if "u" in tok:
                has_u = True
            if "o" in tok:
                expect_o_arg = True
    return has_e, has_u, has_pipefail


def test_every_lambda_deploy_script_sets_euo_pipefail() -> None:
    violations: list[str] = []
    for script in _lambda_deploy_scripts():
        text = script.read_text()
        has_e, has_u, has_pipefail = _has_all_flags(text)
        missing = [
            name
            for name, present in (("-e", has_e), ("-u", has_u), ("pipefail", has_pipefail))
            if not present
        ]
        if missing:
            violations.append(f"{script.relative_to(_REPO_ROOT)}: missing {', '.join(missing)}")

    assert not violations, (
        f"{len(violations)} deploy.sh script(s) do not set the full "
        "`set -euo pipefail` contract (alpha-engine-config-I8033 class — a "
        "script without `set -e`/pipefail can exit 0 after a failing "
        "command, including a failed Lambda code upload):\n  "
        + "\n  ".join(sorted(violations))
    )


def test_every_lambda_deploy_script_sources_deploy_run() -> None:
    """The status-propagating `run()` + `verify_code_deployed` live in
    `_shared/deploy_run.sh` (alpha-engine-config-I8033). Every deploy.sh must
    source it — `set -e` alone still trusts whatever exit code the AWS CLI
    reports, which was measured to be wrong for a failed 25MB code upload."""
    violations: list[str] = []
    for script in _lambda_deploy_scripts():
        text = script.read_text()
        if "_shared/deploy_run.sh" not in text:
            violations.append(str(script.relative_to(_REPO_ROOT)))

    assert not violations, (
        f"{len(violations)} deploy.sh script(s) do not source "
        "_shared/deploy_run.sh, so they lack both the status-propagating "
        "run() and verify_code_deployed() (alpha-engine-config-I8033):\n  "
        + "\n  ".join(sorted(violations))
    )


# ---------------------------------------------------------------------------
# `run ... || true` is a silent lie now (alpha-engine-config-I8125).
#
# run() calls `exit`, not `return`. `exit` inside a function terminates the
# SHELL, and `cmd || true` cannot catch it — `||` guards a non-zero RETURN and
# there is no return to guard. So the moment run() started exiting, every
# pre-existing `run ... || true` became fatal rather than tolerant, silently
# and everywhere at once.
#
# Measured 2026-08-21: 24 sites across 20 of 43 deploy.sh scripts, all
# `aws lambda add-permission` — which returns ResourceConflictException
# whenever its statement-id already exists, i.e. on EVERY deploy after the
# first. Twenty Lambdas became undeployable in one merge, and
# deploy-overseer-backstop-responder failed five consecutive times before the
# log was read.
#
# The pattern reads as tolerant and behaves as fatal, which is the worst
# combination a reviewer can be shown. It must never come back.
# ---------------------------------------------------------------------------

import re  # noqa: E402


def _logical_lines(text: str) -> list[str]:
    """Join backslash continuations so a wrapped command is one line."""
    return re.sub(r"\\\n\s*", " ", text).splitlines()


def test_no_run_invocation_is_guarded_by_or_true() -> None:
    """`run ... || true` cannot do what it says. Use run_tolerating."""
    offenders = []
    for path in _lambda_deploy_scripts():
        for line in _logical_lines(path.read_text()):
            stripped = line.strip()
            if stripped.startswith("run ") and "|| true" in stripped:
                offenders.append(f"{path.parent.name}: {stripped[:90]}")
    assert not offenders, (
        "`run ... || true` reads as tolerant and is fatal — run() exits, and "
        "`||` cannot catch an exit. Use `run_tolerating \"<ErrorName>\" ...`, "
        "which names the ONE failure that is benign and still fails loud on "
        "every other:\n  " + "\n  ".join(offenders)
    )


def test_run_tolerating_exists_and_names_its_expected_error() -> None:
    """The helper must require an expected-error argument.

    A `run_tolerating` with an optional pattern would decay straight back
    into `|| true` at the first call site that omitted it.
    """
    src = (_REPO_ROOT / "infrastructure/lambdas/_shared/deploy_run.sh").read_text()
    assert "run_tolerating()" in src
    assert "run_tolerating: expected-error substring required" in src, (
        "the expected-error argument must be mandatory, or the helper "
        "degrades to the unconditional swallow it replaces"
    )
    assert 'exit "${status}"' in src, "an unexpected failure must still exit"


def test_every_add_permission_tolerates_only_the_conflict() -> None:
    """`aws lambda add-permission` is idempotent-by-conflict.

    It legitimately fails with ResourceConflictException when the statement
    already exists — the steady state on every deploy after the first — and
    must NOT be tolerated for AccessDenied or a malformed ARN, which is
    exactly what the old `|| true` swallowed alongside it.
    """
    for path in _lambda_deploy_scripts():
        lines = _logical_lines(path.read_text())
        for idx, line in enumerate(lines):
            if "aws lambda add-permission" not in line:
                continue
            # Either the shared wrapper, or a call site that captures the
            # output itself and checks it — changelog-cloudwatch-mirror reads
            # PERM_OUT to decide a propagation sleep, so it keeps its own
            # capture. What is NOT acceptable either way is an unconditional
            # swallow: the conflict is the ONE benign failure here.
            # Comment lines are excluded: a comment EXPLAINING why `|| true`
            # was removed must not read as `|| true` still being there.
            window_lines = [
                ln for ln in lines[max(0, idx - 1):idx + 15]
                if not ln.strip().startswith("#")
            ]
            window = " ".join(window_lines)
            assert "ResourceConflictException" in window, (
                f"{path.parent.name}: add-permission tolerates exactly one "
                f"failure — ResourceConflictException, the steady state on "
                f"every deploy after the first. Wrap it in run_tolerating, or "
                f"capture and check the output. Got: {line.strip()[:90]}"
            )
            assert "|| true" not in window, (
                f"{path.parent.name}: `|| true` on add-permission swallows "
                f"AccessDenied and a malformed ARN alongside the conflict"
            )


def test_no_tolerated_call_discards_its_own_stderr() -> None:
    """`2>/dev/null` on a tolerated call is what made the outage unreadable.

    run_tolerating captures stderr so the tolerated case can be RECOGNISED
    and reported; discarding it at the call site defeats that and leaves a
    failing deploy with no message, which is how five consecutive failures
    went unexplained.
    """
    offenders = []
    for path in _lambda_deploy_scripts():
        for line in _logical_lines(path.read_text()):
            if "run_tolerating" in line and "2>/dev/null" in line:
                offenders.append(f"{path.parent.name}: {line.strip()[:90]}")
    assert not offenders, "\n  ".join(["tolerated calls must not hide stderr:"] + offenders)
