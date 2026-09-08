"""A Lambda that writes a `_stage_coverage` verdict must be able to READ the registry.

`alpha-engine-config-I10174`. `alpha-engine-weekly-freshness-spot-dispatcher`
holds `s3:PutObject` on `_stage_coverage/*` and had no grant to read
`_freshness_monitor/ARTIFACT_REGISTRY.yaml`, which its verdict is derived from.
So `RelaunchWeeklyFreshnessSpot` rendered **UNMEASURED** on every weekly cycle
— not absent, not covered, permanently uncountable.

Measured live 2026-09-08 with `aws iam simulate-principal-policy`:
`implicitDeny` on `s3:GetObject` AND `s3:ListBucket` for that key. The object
exists (`head-object`, 386993 bytes), so this was a genuine identity-policy
denial and not the `alpha-engine-config` gotcha where S3 answers `GetObject`
on a MISSING key with `AccessDenied` unless the caller also holds `ListBucket`.

That gotcha is why both actions are asserted here rather than just the read: a
`GetObject`-only grant makes a missing object indistinguishable from a denied
one, and this Lambda's whole job is to report which.

DERIVED, never hand-listed — `AGENTS.md` rule 4. The population is every
Lambda in this repo whose policy writes under `_stage_coverage/`, so a fourth
verdict-writer added tomorrow is held to the same rule without anyone
remembering to add it here.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

LAMBDAS_DIR = Path(__file__).resolve().parents[1] / "infrastructure" / "lambdas"

BUCKET = "alpha-engine-research"
REGISTRY_KEY = "_freshness_monitor/ARTIFACT_REGISTRY.yaml"
REGISTRY_ARN = f"arn:aws:s3:::{BUCKET}/{REGISTRY_KEY}"
BUCKET_ARN = f"arn:aws:s3:::{BUCKET}"


def _statements(policy: dict) -> list[dict]:
    return policy.get("Statement") or []


def _actions(stmt: dict) -> list[str]:
    a = stmt.get("Action")
    return [a] if isinstance(a, str) else list(a or [])


def _resources(stmt: dict) -> list[str]:
    r = stmt.get("Resource")
    return [r] if isinstance(r, str) else list(r or [])


def _covers_object(resource: str, arn: str) -> bool:
    """Does an IAM resource string cover `arn`? Prefix wildcards only."""
    if resource == arn:
        return True
    if resource.endswith("*") and arn.startswith(resource[:-1]):
        return True
    return False


def _stage_coverage_writers() -> list[tuple[str, dict]]:
    out = []
    for policy_path in sorted(LAMBDAS_DIR.glob("*/iam-policy.json")):
        policy = json.loads(policy_path.read_text(encoding="utf-8"))
        for stmt in _statements(policy):
            if stmt.get("Effect") != "Allow":
                continue
            if not any(a in ("s3:PutObject", "s3:*") for a in _actions(stmt)):
                continue
            if any("_stage_coverage" in r for r in _resources(stmt)):
                out.append((policy_path.parent.name, policy))
                break
    return out


WRITERS = _stage_coverage_writers()


def test_the_population_is_not_empty():
    """A derived population that derives nothing makes every assertion vacuous."""
    assert WRITERS, (
        "no Lambda in infrastructure/lambdas/ was found writing under "
        "_stage_coverage/ — either the population moved or this guard is now "
        "measuring nothing"
    )


@pytest.mark.parametrize("name,policy", WRITERS, ids=lambda v: v if isinstance(v, str) else "")
def test_a_stage_coverage_writer_can_read_the_artifact_registry(name, policy):
    reads = [
        s for s in _statements(policy)
        if s.get("Effect") == "Allow"
        and any(a in ("s3:GetObject", "s3:*") for a in _actions(s))
        and any(_covers_object(r, REGISTRY_ARN) for r in _resources(s))
    ]
    assert reads, (
        f"{name} writes a _stage_coverage verdict but holds no s3:GetObject "
        f"covering {REGISTRY_ARN}. Its verdict is derived from that registry, "
        f"so without the grant the stage renders UNMEASURED forever — which "
        f"principles.md 2.7 forbids being read as healthy "
        f"(alpha-engine-config-I10174)."
    )


@pytest.mark.parametrize("name,policy", WRITERS, ids=lambda v: v if isinstance(v, str) else "")
def test_the_read_is_paired_with_a_listbucket_on_the_prefix(name, policy):
    """Without ListBucket, a MISSING object is indistinguishable from a DENIED one.

    S3 answers `GetObject` on a key that does not exist with `AccessDenied`
    unless the caller also holds `ListBucket` — measured 2026-09-04. A
    verdict-writer whose job is to report whether an artifact is present must
    be able to tell those two apart.
    """
    lists = [
        s for s in _statements(policy)
        if s.get("Effect") == "Allow"
        and any(a in ("s3:ListBucket", "s3:*") for a in _actions(s))
        and BUCKET_ARN in _resources(s)
    ]
    assert lists, (
        f"{name} can read the registry object but holds no s3:ListBucket on "
        f"{BUCKET_ARN}, so a missing artifact and a denied one return the same "
        f"error and its verdict cannot distinguish them."
    )


def test_the_dispatchers_listbucket_is_prefix_scoped():
    """The grant this issue added is narrow, and stays narrow.

    A bare `ListBucket` on the bucket would enumerate every prefix in
    alpha-engine-research. The dispatcher needs exactly one.
    """
    policy = json.loads(
        (LAMBDAS_DIR / "weekly-freshness-spot-dispatcher" / "iam-policy.json")
        .read_text(encoding="utf-8")
    )
    lists = [
        s for s in _statements(policy)
        if s.get("Effect") == "Allow" and "s3:ListBucket" in _actions(s)
    ]
    assert lists, "the dispatcher's ListBucket grant is gone"
    for stmt in lists:
        prefixes = (
            (stmt.get("Condition") or {}).get("StringLike", {}).get("s3:prefix")
        )
        assert prefixes, (
            "the dispatcher's s3:ListBucket carries no s3:prefix condition — "
            "that grants a listing of the whole bucket"
        )
        assert all(p.startswith("_freshness_monitor/") for p in prefixes), (
            f"the dispatcher's ListBucket prefixes are {prefixes}; it reads one "
            f"registry and needs no other prefix"
        )
