"""Every shell script that CALLS a repo-defined shell function must define it
or (transitively) source a file that does.

alpha-engine-config-I7338. `infrastructure/lambdas/alert-drain-liveness-probe/
deploy.sh` called `apply_iam_policy` at its "#4472 auto-apply IAM on merge"
site and never sourced `_shared/apply_iam_policy.sh`. Measured 2026-08-14
running as `ne-admin`, an identity that DOES hold iam:PutRolePolicy:

    .../alert-drain-liveness-probe/deploy.sh: line 188: apply_iam_policy: command not found
    WARN: IAM auto-apply failed (expected in CI — role lacks iam:PutRolePolicy)

The `||` beneath the call asserted a cause that could not have been true, so
the failure read as benign and the auto-apply feature never executed on that
lambda — the content-drift half of alpha-engine-config-I6299.

WHY A DERIVED TEST AND NOT A LIST. 34 deploy.sh scripts call
`apply_iam_policy`; 33 sourced it. A test enumerating those 34 leaves the 35th
script with the identical hole, and this repo grows a lambda most weeks. So
both sides are derived from the tree:

  * the UNIVERSE of function names is every `name() { ... }` defined in any
    tracked shell file under infrastructure/ — nothing is hardcoded;
  * the CALL SITES are every command-position occurrence of one of those
    names in any tracked shell file.

A new helper in `_shared/` and a new deploy.sh calling it are both covered on
the day they land, with no edit here.

WHY SHELLCHECK DOES NOT COVER THIS. shellcheck resolves `source` only when the
path is a literal it can follow, and every deploy.sh here reaches _shared/
through `${SCRIPT_DIR}` or a `$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)`
expansion. With the path unresolvable, SC1091 downgrades to an *info* about
the source line and shellcheck then treats the callee as an external command —
it emits nothing at all for the call. Verified against the pre-fix revision:
`shellcheck --severity=warning` on alert-drain-liveness-probe/deploy.sh
reports zero findings for `apply_iam_policy`.

FAIL-LOUD (fleet rule). This test never skips and never silently narrows. If a
`source` line cannot be resolved to a file on disk it is a FAILURE, not an
ignored line — an unresolvable source is exactly how a guard like this decays
into one that passes while measuring nothing.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
_INFRA = _REPO_ROOT / "infrastructure"

# `name() {`, `name () {`, `function name {`, `function name() {`.
_DEF_RE = re.compile(
    r"^[ \t]*(?:function[ \t]+)?([A-Za-z_][A-Za-z0-9_]*)[ \t]*\([ \t]*\)[ \t]*\{"
    r"|^[ \t]*function[ \t]+([A-Za-z_][A-Za-z0-9_]*)[ \t]*\{",
    re.MULTILINE,
)

# `source X` / `. X`, capturing the (possibly quoted, possibly interpolated)
# path argument. Rest-of-line rather than `\S+`: the dominant idiom here is
# `source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../_shared/x.sh"`,
# which contains spaces, and a `\S+` capture truncates it to `"$(cd`.
_SOURCE_RE = re.compile(r"^[ \t]*(?:source|\.)[ \t]+(.+?)[ \t]*$", re.MULTILINE)

# A shell comment, so a call inside a comment is not counted as a call site.
_COMMENT_RE = re.compile(r"(?<!\$)#.*$", re.MULTILINE)

# Heredoc bodies are data, not code in THIS script's scope. Stripped so a
# generated remote script that calls a function it defines itself is not
# attributed to the outer file.
_HEREDOC_RE = re.compile(
    r"<<-?[ \t]*'?\"?([A-Za-z_][A-Za-z0-9_]*)'?\"?.*?\n.*?^[ \t]*\1[ \t]*$",
    re.MULTILINE | re.DOTALL,
)


def _tracked_shell_files() -> list[Path]:
    """Every git-tracked *.sh under infrastructure/.

    Uses git rather than glob so an untracked scratch script in a worktree
    cannot fail the suite, and a tracked one can never be missed.
    """
    out = subprocess.run(
        ["git", "-C", str(_REPO_ROOT), "ls-files", "--", "infrastructure/**/*.sh", "infrastructure/*.sh"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split()
    files = sorted({_REPO_ROOT / p for p in out})
    assert files, (
        "derived-test precondition failed: found ZERO tracked shell files under "
        "infrastructure/. This guard measures nothing in that state."
    )
    return files


def _blank(match: re.Match[str]) -> str:
    """Replace a match with blanks, preserving byte offsets and line count.

    Offset-preserving so a reported violation's line number refers to the
    ORIGINAL file. A stripping `sub("")` shifts every subsequent line and the
    guard then points an engineer at the wrong code — which is how a correct
    finding gets dismissed as noise.
    """
    return re.sub(r"[^\n]", " ", match.group(0))


def _code(text: str) -> str:
    """Script text with heredoc bodies and comments blanked out."""
    return _COMMENT_RE.sub(_blank, _HEREDOC_RE.sub(_blank, text))


def _defined_in(text: str) -> set[str]:
    return {m.group(1) or m.group(2) for m in _DEF_RE.finditer(text)}


def _resolve_source(raw: str, script: Path) -> Path:
    """Resolve a `source` argument to a path on disk.

    Handles the four idioms present in this repo:
        "${SCRIPT_DIR}/../_shared/pause.sh"
        "$SCRIPT_DIR/_spot_common.sh"
        "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../_shared/pause.sh"
        "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lambdas/_shared/pause.sh"

    All four denote a path relative to the SCRIPT'S OWN directory, so the
    leading expansion is replaced by that directory. An argument that still
    contains a `$` after substitution is unresolvable and the caller fails.
    """
    arg = raw.strip().strip("\"'")
    here = script.parent

    # `$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)` and `${SCRIPT_DIR}` /
    # `$SCRIPT_DIR` all mean "the directory holding this script".
    arg = re.sub(r'^\$\(cd\s+"\$\(dirname\s+"\$\{BASH_SOURCE\[0\]\}"\)"\s*&&\s*pwd\)', str(here), arg)
    arg = re.sub(r"^\$\{SCRIPT_DIR\}|^\$SCRIPT_DIR", str(here), arg)

    return Path(arg)


def _sourced_closure(script: Path, _seen: set[Path] | None = None) -> tuple[set[Path], list[str]]:
    """Files transitively sourced by `script`, plus any unresolvable args."""
    seen = _seen if _seen is not None else set()
    unresolved: list[str] = []
    if script in seen or not script.is_file():
        return seen, unresolved
    seen.add(script)

    for raw in _SOURCE_RE.findall(_code(script.read_text())):
        target = _resolve_source(raw, script)
        if "$" in str(target) or not target.is_file():
            unresolved.append(f"{script.relative_to(_REPO_ROOT)}: source {raw}")
            continue
        child_seen, child_unresolved = _sourced_closure(target.resolve(), seen)
        seen |= child_seen
        unresolved.extend(child_unresolved)

    return seen, unresolved


def test_every_called_shell_function_is_reachable() -> None:
    files = _tracked_shell_files()
    texts = {f: f.read_text() for f in files}
    code = {f: _code(t) for f, t in texts.items()}

    # The universe: every function name defined anywhere in the shell tree.
    universe: set[str] = set()
    for f in files:
        universe |= _defined_in(code[f])
    assert "apply_iam_policy" in universe, (
        "derived-test precondition failed: apply_iam_policy is not in the "
        "discovered function universe, so this guard is not measuring the "
        "class it exists for."
    )

    # A call is a command-position occurrence: start of line, or after a
    # separator (`;` `|` `&` `(` `{`), or after `then`/`do`/`else`. Bash
    # keywords only — an English word in a quoted message is NOT a call site
    # (`|| echo "...did not run for X..."` is prose, not an invocation of a
    # function named `run`).
    call_res = {
        name: re.compile(
            r"(?:^|(?<=[;|&(){])|(?<=\bthen\b)|(?<=\bdo\b)|(?<=\belse\b))"
            r"[ \t]*" + re.escape(name) + r"(?=[ \t]|$)",
            re.MULTILINE,
        )
        for name in universe
    }
    # ...and never inside a quoted string, which is where the prose lives.
    _dquote_re = re.compile(r'"(?:\\.|[^"\\])*"', re.DOTALL)
    _squote_re = re.compile(r"'[^']*'", re.DOTALL)

    def _call_sites(body: str) -> dict[str, int]:
        """{function name -> 1-based line of its first command-position call}."""
        scan = _squote_re.sub(_blank, _dquote_re.sub(_blank, body))
        out: dict[str, int] = {}
        for name in universe:
            m = call_res[name].search(scan)
            if m is not None:
                out[name] = scan[: m.start()].count("\n") + 1
        return out

    # Reachability is a property of an ENTRY POINT — a script that is executed,
    # not sourced — because that is what bash actually flattens at runtime. A
    # file under _shared/ legitimately calls `run()`, which its consumers
    # define; checking it standalone would report a false violation and
    # checking every file standalone is not the runtime rule. So: resolve each
    # entry point's full source closure, then require every call made ANYWHERE
    # in that closure to be defined SOMEWHERE in it. A library sourced by
    # nobody is its own entry point and is still checked.
    closures: dict[Path, set[Path]] = {}
    unresolved_all: list[str] = []
    sourced_by_someone: set[Path] = set()

    for f in files:
        closure, unresolved = _sourced_closure(f)
        closures[f] = closure
        unresolved_all.extend(unresolved)
        sourced_by_someone |= closure - {f}

    entry_points = [f for f in files if f not in sourced_by_someone]
    assert entry_points, (
        "derived-test precondition failed: every shell file is sourced by "
        "another, so there is no entry point to check."
    )

    violations: list[str] = []
    for entry in entry_points:
        members = sorted(closures[entry])
        reachable: set[str] = set()
        for m in members:
            reachable |= _defined_in(code.get(m) or _code(m.read_text()))

        for m in members:
            for name, line in _call_sites(code.get(m) or _code(m.read_text())).items():
                if name in reachable:
                    continue
                where = f"{m.relative_to(_REPO_ROOT)}:~{line}"
                via = "" if m == entry else f" (reached from {entry.relative_to(_REPO_ROOT)})"
                violations.append(
                    f"{where}: calls `{name}` but neither it nor anything it "
                    f"sources defines it{via}"
                )

    assert not unresolved_all, (
        "unresolvable `source` argument(s) — this guard cannot see past them, "
        "and a source it cannot follow is indistinguishable from a missing "
        "one. Add the idiom to _resolve_source():\n  "
        + "\n  ".join(sorted(unresolved_all))
    )
    assert not violations, (
        f"{len(violations)} shell call site(s) reach a repo-defined function that is "
        "not reachable from them — the alpha-engine-config-I7338 class "
        "(`command not found` at runtime):\n  " + "\n  ".join(sorted(violations))
    )
