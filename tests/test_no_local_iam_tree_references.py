"""This repo must not READ files from `infrastructure/iam/` — it no longer owns IAM.

alpha-engine-config-I5271 · nous-ergon-ops `policies/infrastructure-ownership-policy.md` §35.

WHY. IAM roles, policies and trust documents were consolidated into
`nous-ergon-ops/infrastructure/iam/` on 2026-07-27. Commit `506be30`
("[P2/high] infra: remove the IAM tree — now owned by nous-ergon-ops", #1075)
deleted the tree from this repo, but `infrastructure/deploy-infrastructure.sh`
still read one file out of it:

    EB_TRUST_FILE="$SCRIPT_DIR/iam/${EB_ROLE_NAME}.trust.json"
    aws iam update-assume-role-policy --policy-document "file://$EB_TRUST_FILE"

That PR passed CI — nothing asserted the script's file references resolve — and
`Deploy Infrastructure` then failed 9 consecutive times from 15:34 UTC on
2026-07-28 with:

    ParamValidation: Unable to load paramfile
      .../infrastructure/iam/alpha-engine-eventbridge-sfn-role.trust.json
    [Errno 2] No such file or directory

Roughly six hours in which no orchestration/CFN change actually deployed, while
every other check on those merges reported green. Each failure dispatched a
ci-watch repair agent; all of them died at boot on an unrelated bug that landed
one minute after this one (alpha-engine-config-PR5261), so nothing said a word.

THE INVARIANT. A missing-file check would be too narrow: restoring the file
would satisfy it while still violating the ownership policy. The real invariant
is the ownership boundary itself — **this repo reads no file under an
`iam/` tree**. That is stable under someone re-adding the directory, and it is
what the policy actually says.

Prose is exempt: comments here still cite `iam/github-actions-lambda-deploy.json`
as the historical rationale for a grant, and citing a path is not depending on
one. Only executable references count.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
INFRA = REPO / "infrastructure"

# Shell scripts that run in deploy/CI. Bootstraps under infrastructure/ are
# included: they run on boxes with a clone of this repo and would hit the same
# missing-path failure.
SHELL_SCRIPTS = sorted(INFRA.rglob("*.sh"))

# An executable reference to a path inside an `iam/` directory. Matches the
# `$SCRIPT_DIR/iam/x.json`, `infrastructure/iam/x.json` and `./iam/x.json`
# forms; deliberately anchored on the `iam/` path segment followed by a
# filename, so a bare mention of the word "iam" (e.g. `aws iam get-role`) is
# not a hit.
_IAM_PATH_RE = re.compile(r"[\w${}./\"-]*\biam/[\w.${}-]+\.(?:json|yaml|yml)")

# Reading the OPS repo's iam tree through an explicit external-checkout variable
# is the sanctioned post-consolidation pattern, not a violation — e.g.
# `infrastructure/lambdas/sf-watch-market-hours-toggler/deploy.sh` reads
# "${IAM_REPO}/infrastructure/iam/sf-watch-executor-role-policy.json". What this
# test forbids is a path resolving INSIDE this repo ($SCRIPT_DIR/iam/…,
# infrastructure/iam/…), not any mention of an iam tree anywhere.
_EXTERNAL_REPO_MARKERS = ("IAM_REPO",)


def _is_external_checkout(path_ref: str) -> bool:
    return any(marker in path_ref for marker in _EXTERNAL_REPO_MARKERS)


def _strip_comment(line: str) -> str:
    """Drop a `#` comment, honouring quotes so a `#` inside a string survives."""
    out, quote = [], None
    for i, ch in enumerate(line):
        if quote:
            if ch == quote and line[i - 1 : i] != "\\":
                quote = None
            out.append(ch)
            continue
        if ch in ("'", '"'):
            quote = ch
            out.append(ch)
            continue
        if ch == "#":
            if i and line[i - 1] in "${":
                out.append(ch)
                continue
            break
        out.append(ch)
    return "".join(out)


def test_scripts_are_discovered():
    """Guard against the suite passing vacuously on an empty glob."""
    assert SHELL_SCRIPTS, f"no *.sh found under {INFRA}"


def test_local_iam_tree_does_not_exist():
    """The tree is owned by nous-ergon-ops; re-adding it here re-forks it."""
    assert not (INFRA / "iam").exists(), (
        "infrastructure/iam/ is back in this repo. IAM was consolidated into "
        "nous-ergon-ops/infrastructure/iam/ on 2026-07-27 "
        "(infrastructure-ownership-policy.md §35). Re-adding it recreates the "
        "per-repo mirrored trees that consolidation removed."
    )


@pytest.mark.parametrize("script", SHELL_SCRIPTS, ids=lambda p: p.name)
def test_no_executable_reference_to_an_iam_file(script: Path):
    offenders = []
    for lineno, raw in enumerate(script.read_text().splitlines(), start=1):
        code = _strip_comment(raw)
        if not code.strip():
            continue
        for hit in _IAM_PATH_RE.findall(code):
            if _is_external_checkout(hit):
                continue
            offenders.append((lineno, hit, raw.strip()))

    assert not offenders, (
        f"{script.relative_to(REPO)} reads {len(offenders)} file(s) from an iam/ tree "
        f"this repo no longer owns — the exact shape that broke Deploy Infrastructure "
        f"for ~6h on 2026-07-28 (alpha-engine-config-I5271). IAM lives in "
        f"nous-ergon-ops/infrastructure/iam/; verify state with a read-only AWS call "
        f"and fail loud pointing at that repo's apply.sh, rather than reading or "
        f"writing the document from here. Offenders: "
        + "; ".join(f"line {ln}: {hit}" for ln, hit, _ in offenders)
    )


def test_detector_catches_the_i5271_regression():
    """Pin the detector against the exact line that broke, so a later refactor
    of the regex cannot quietly neuter it."""
    broken = 'EB_TRUST_FILE="$SCRIPT_DIR/iam/${EB_ROLE_NAME}.trust.json"'
    assert _IAM_PATH_RE.findall(_strip_comment(broken))

    # A comment citing the path is prose, not a dependency.
    assert not _IAM_PATH_RE.findall(
        _strip_comment("# see infrastructure/iam/github-actions-lambda-deploy.json).")
    )
    # An ordinary IAM API call is not a file reference.
    assert not _IAM_PATH_RE.findall(
        _strip_comment('aws iam get-role --role-name "$EB_ROLE_NAME" --output json')
    )
