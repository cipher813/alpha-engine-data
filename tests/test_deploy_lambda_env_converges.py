"""The deploy must strip denied environment variables, and must do it where
the promotion can carry the change to traffic.

alpha-engine-config-I7925. `alpha-engine-data-collector` was one of eleven
fleet-wide Lambdas carrying a `GITHUB_TOKEN` set by hand and refreshed by
nothing — the environment is live-only state that no repo, IaC file or
script here ever wrote. The environment carried a STALE COPY of the
credential — set from an older SSM parameter version and never
re-derived on deploy — that GitHub rejected while the SSM parameter's
own value remained valid the whole time (alpha-engine-config-I7968
tracks the mis-attribution). On 2026-08-21 a
first-party dependency picked it up out of site-packages, sent it to
GitHub, got a 401, and halted the preopen trading pipeline 3.4 seconds
after start (alpha-engine-config-I7924). `alpha-engine-predictor-inference`
(crucible-predictor) was the one of eleven that broke and was fixed first
(crucible-predictor-PR535). This mirrors that same deploy-side convergence
here.

`infrastructure/deploy.sh` now converges the environment against a deny-list
in the same slot the existing memory/timeout convergence (guarded by
tests/test_deploy_lambda_config_converges.py) already occupies: after
`update-function-code`, before `publish-version`. These tests pin the two
properties that make it real, because both fail SILENTLY: a removal placed
after `publish-version` never reaches the published version the `live`
alias serves, and a removal that also promotes the alias would race the
deploy's own promotion.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_DEPLOY = Path(__file__).resolve().parents[1] / "infrastructure" / "deploy.sh"
_CODE = _DEPLOY.read_text(encoding="utf-8")


def test_deploy_script_exists() -> None:
    assert _DEPLOY.is_file(), f"{_DEPLOY} is missing"


def test_github_token_is_on_the_deny_list() -> None:
    """The credential that caused I7924 must be named, not merely implied."""
    assert "LAMBDA_ENV_DENIED_KEYS=(" in _CODE, (
        "the deploy no longer declares a denied-key set — a variable set by "
        "hand now outlives every deploy again (alpha-engine-config-I7925)"
    )
    declaration = _CODE.split("LAMBDA_ENV_DENIED_KEYS=(", 1)[1].split(")", 1)[0]
    assert "GITHUB_TOKEN" in declaration


def test_removal_uses_the_shared_cli_not_a_bare_aws_call() -> None:
    """`aws lambda update-function-configuration --environment` REPLACES the
    whole variable map, deleting every operator-set flag codified nowhere
    (FMP_API_KEY, FINNHUB_API_KEY, EDGAR_IDENTITY, …). The read-modify-write
    chokepoint is `krepis.aws remove-lambda-env`."""
    assert "krepis.aws remove-lambda-env" in _CODE


def test_removal_runs_before_the_version_is_published() -> None:
    """A removal after `publish-version` mutates $LATEST only. The published
    version — and therefore the `live` alias the Saturday SF invokes — would
    keep the variable, and the deploy would report success having changed
    nothing that serves traffic."""
    remove_at = _CODE.index("remove-lambda-env")
    publish_at = _CODE.index("aws lambda publish-version")
    assert remove_at < publish_at, (
        "the environment convergence must precede publish-version, or the "
        "published version keeps the denied variable (L4497)"
    )


def test_removal_defers_promotion_to_the_deploy() -> None:
    """The deploy publishes a version and moves the `live` alias itself
    immediately after. A removal that also promoted would publish a second
    version mid-deploy and race the alias move."""
    step = _CODE.split("krepis.aws remove-lambda-env", 1)[1].split("\n\n", 1)[0]
    assert "--defer-publish" in step
    assert "--promote-alias" not in step


def test_removal_is_idempotent_across_deploys() -> None:
    """Every deploy after the first finds the key already gone; without
    --missing-ok the CLI refuses and `set -euo pipefail` aborts the deploy."""
    step = _CODE.split("krepis.aws remove-lambda-env", 1)[1].split("\n\n", 1)[0]
    assert "--missing-ok" in step


def test_krepis_pin_can_supply_the_subcommand() -> None:
    """`remove-lambda-env` ships in krepis 0.59.23. An older pin makes the
    deploy step exit 2 on an unknown subcommand."""
    req = Path(__file__).resolve().parents[1] / "requirements.txt"
    line = next(
        ln
        for ln in req.read_text(encoding="utf-8").splitlines()
        if ln.startswith("krepis==")
    )
    version = line.split("==", 1)[1].split()[0].strip()
    parts = tuple(int(p) for p in version.split("."))
    assert parts >= (0, 59, 23), (
        f"requirements.txt pins krepis {version}; remove-lambda-env needs >= 0.59.23"
    )


def test_krepis_pin_does_not_need_the_deploy_role_to_list_aliases() -> None:
    """krepis 0.59.23's `remove_lambda_environment_keys` enumerated Lambda
    aliases unconditionally, including under `--defer-publish` — which the
    call site above passes. This repo's deploy role does not hold
    `lambda:ListAliases`. The failure lands after the image is pushed and
    $LATEST is updated, and before `publish-version` and the alias move: a
    PARTIAL deploy, with the `live` alias serving a stale image while main
    has moved on — the SHA drift the preopen `DeployDriftGate` halts on
    (alpha-engine-config-I8030, mirroring crucible-predictor's fix for
    I7925/deploy run 32509752554).

    krepis 0.59.24 skips the enumeration under `defer_publish` (krepis#176).
    An older pin reintroduces the partial deploy, so the floor is pinned
    here rather than left to memory.
    """
    req = Path(__file__).resolve().parents[1] / "requirements.txt"
    line = next(
        ln
        for ln in req.read_text(encoding="utf-8").splitlines()
        if ln.startswith("krepis==")
    )
    version = line.split("==", 1)[1].split()[0].strip()
    parts = tuple(int(p) for p in version.split("."))
    assert parts >= (0, 59, 24), (
        f"requirements.txt pins krepis {version}; --defer-publish needs >= "
        f"0.59.24 or the deploy fails on lambda:ListAliases and leaves a "
        f"PARTIAL deploy (alpha-engine-config-I8030)"
    )
