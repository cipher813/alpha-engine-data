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
