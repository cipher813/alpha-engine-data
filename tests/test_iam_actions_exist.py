"""No IAM document in this repo may grant an action name that does not exist.

alpha-engine-config-I7571. ``iam:PutRolePolicy`` accepts an arbitrary string in
``Action`` without validating it against the service's authorization reference.
A policy naming a non-existent action therefore APPLIES CLEANLY and grants
nothing — the call it was written for 403s at runtime, months later, on a path
whose failure handler usually reports "absent" rather than raising.

Measured instance that generated this test: ``s3:HeadObject``. S3's HeadObject
API is authorized by ``s3:GetObject``; there is no ``s3:HeadObject`` action at
all. Five documents in this repo named it. Two of them (the sf-telegram-notifier
EOD completion-marker attestation and the spot-orphan-reaper's watch-completion
checks) named it as their ONLY grant, so both probes had 403'd on every
invocation since they shipped:

    completion marker HEAD failed for
    s3://alpha-engine-research/_sf_completion/ne-postclose-trading-pipeline/2026-08-14.json
    (non-404: An error occurred (403) when calling the HeadObject operation: Forbidden)
    — reporting absent rather than raising

The denylist is deliberately a denylist and not a full IAM action inventory:
the authoritative list is a large AWS-published document that would need
vendoring and refreshing, and a stale copy of it would manufacture false
failures on genuinely new actions. A denylist of names that LOOK like actions,
are commonly written, and provably are not, fails only on the mistake it
names — and grows by one line each time another is found.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

# action-name -> what to write instead. Each entry must cite why it is wrong.
NONEXISTENT_ACTIONS: dict[str, str] = {
    # HEAD Object is authorized by s3:GetObject (AWS S3 service authorization
    # reference: HeadObject is not listed as an action; the API is covered by
    # GetObject). Confirmed live 2026-08-14 by a 403 on a marker that existed.
    "s3:HeadObject": "s3:GetObject",
    # HEAD Bucket is authorized by s3:ListBucket, for the same reason.
    "s3:HeadBucket": "s3:ListBucket",
    # The Lambda SDK method is "invoke"; the IAM action is InvokeFunction.
    "lambda:Invoke": "lambda:InvokeFunction",
}


def _iam_documents() -> list[Path]:
    """Every JSON file in the repo that parses as an IAM policy document."""
    out: list[Path] = []
    for path in REPO_ROOT.rglob("*.json"):
        # Relative to REPO_ROOT, never the absolute path: this repo is itself
        # checked out under a `.worktrees/` directory during agent sessions,
        # and an absolute-parts filter silently excluded EVERY document there
        # (caught by test_repo_has_iam_documents_to_check).
        rel_parts = path.relative_to(REPO_ROOT).parts
        if any(part in {".git", "node_modules", ".worktrees"} for part in rel_parts):
            continue
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except (ValueError, OSError, UnicodeDecodeError):
            continue
        if isinstance(doc, dict) and "Statement" in doc and "Version" in doc:
            out.append(path)
    return out


def _actions(statement: dict) -> list[str]:
    for key in ("Action", "NotAction"):
        raw = statement.get(key)
        if isinstance(raw, str):
            return [raw]
        if isinstance(raw, list):
            return [a for a in raw if isinstance(a, str)]
    return []


def test_repo_has_iam_documents_to_check():
    """Guard the guard: an empty corpus would make every assertion below pass."""
    assert _iam_documents(), "no IAM policy documents discovered — the walk is broken"


@pytest.mark.parametrize("doc_path", _iam_documents(), ids=lambda p: str(p.relative_to(REPO_ROOT)))
def test_no_nonexistent_iam_actions(doc_path: Path):
    doc = json.loads(doc_path.read_text(encoding="utf-8"))
    statements = doc["Statement"]
    if isinstance(statements, dict):
        statements = [statements]
    offenders: list[str] = []
    for statement in statements:
        if not isinstance(statement, dict):
            continue
        for action in _actions(statement):
            replacement = NONEXISTENT_ACTIONS.get(action)
            if replacement:
                offenders.append(
                    f"Sid={statement.get('Sid', '<unnamed>')}: "
                    f"{action!r} is not a real IAM action — use {replacement!r}"
                )
    assert not offenders, (
        f"{doc_path.relative_to(REPO_ROOT)} grants action name(s) that do not exist. "
        "put-role-policy accepts them silently and they grant nothing:\n  "
        + "\n  ".join(offenders)
    )
