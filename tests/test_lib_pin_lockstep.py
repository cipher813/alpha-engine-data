"""Pin every lib-install surface to the same nousergon-lib version.

(The dist was renamed ``alpha-engine-lib`` → ``nousergon-lib`` at v0.60.0;
the historical incidents below predate the rename and reference the old
``nousergon_lib`` import name accordingly — kept verbatim as the
drift-class record.)

The Dockerfile strips nousergon-lib from ``requirements.txt`` before
``pip install`` (see the ``grep -vE ...nousergon-lib`` line in the
Dockerfile RUN block) and instead installs the lib via a hardcoded
``pip install "nousergon-lib@vX.Y.Z"`` line ABOVE that grep. So
bumping ``requirements.txt`` alone does NOT propagate to the Lambda
image — the Dockerfile's hardcoded pin wins. The slim
``requirements-daily-news.txt`` (standalone daily-news collector on the
dashboard box) carries its own copy of the pin and its header demands
lockstep with ``requirements.txt`` — so it is guarded here too.
``.github/workflows/deploy-infrastructure.yml`` also carries its own
hardcoded ``pip install`` pin for its drift-check alerting step
(``nousergon_lib.alerts``) and is guarded here for the same reason
(alpha-engine-config#2999: this file drifted a full version behind
``requirements.txt`` undetected until this test covered it).

Some Lambdas have deliberate exemptions documented in their requirements.txt
comments. These must move in lockstep within their exemption group (e.g., all
spot-dispatch Lambdas stay together) and MUST NOT silently drift from their
documented version without a named contract reason.

This drift class has bitten production multiple times:

  - 2026-05-06 (research): requirements.txt bumped @v0.4.0 → @v0.5.1
    but Dockerfile kept v0.3.0; Research Lambda canary failed with
    ``ModuleNotFoundError: nousergon_lib.agent_schemas``.
  - 2026-05-12 (predictor): requirements.txt → v0.12.0 but
    requirements-lambda.txt stayed v0.9.1; predictor canary failed
    with ``ModuleNotFoundError: nousergon_lib.secrets``.
  - 2026-05-12 (data, this repo): requirements.txt → v0.12.0 in PR
    #221 but Dockerfile kept v0.3.0 (a 9-version-old pin); data
    Lambda canary failed at 17:22 UTC with the same
    ``nousergon_lib.secrets`` ModuleNotFoundError.

This test re-greps all three files on every CI run so a future single-file
bump fails here, not in a canary.
"""

from __future__ import annotations

import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent

_REQUIREMENTS_PIN_RE = re.compile(
    r"nousergon-lib\[[^\]]*\]\s*@\s*git\+https://github\.com/nousergon/nousergon-lib@(v[0-9]+\.[0-9]+\.[0-9]+)"
)
_DOCKERFILE_PIN_RE = re.compile(
    r'"nousergon-lib\[[^\]]*\]\s*@\s*git\+https://github\.com/nousergon/nousergon-lib@(v[0-9]+\.[0-9]+\.[0-9]+)"'
)
_LAMBDA_PIN_RE = re.compile(
    r"nousergon-lib(?:\[[^\]]*\])?\s*@\s*git\+https://github\.com/nousergon/nousergon-lib@(v[0-9]+\.[0-9]+\.[0-9]+)"
)

# Lambda exemptions: deliberate pins outside the root lockstep guard,
# documented in each Lambda's requirements.txt header comment.
# Key: lambda directory name, Value: (pin version, contract reason)
_LAMBDA_PIN_EXEMPTIONS = {
    "arctic-migration-dispatcher": (
        "v0.124.5",
        "nousergon_lib.spot_dispatch chokepoint (alpha-engine-config-I3242: same "
        "spot-launch/concurrency-lock primitives as sf-watch-spot-dispatcher / "
        "ci-watch-dispatcher / canary-replay-dispatcher / alert-drain-dispatcher — "
        "same exemption group, stays in lockstep with them, not with root)",
    ),
    "canary-replay-dispatcher": (
        "v0.124.5",
        "nousergon_lib.spot_dispatch chokepoint (alpha-engine-config#2246: same SpotProbeError "
        "handling as ci-watch-dispatcher; bumped for config#2698 SpotQuotaExceededError "
        "on-demand fallback, first available at v0.124.1)",
    ),
    "alert-drain-dispatcher": (
        "v0.124.5",
        "nousergon_lib.spot_dispatch chokepoint (alpha-engine-config-I2824: same "
        "extra_tags atomic-launch-tagging floor as ci-watch-dispatcher, config#2292; bumped "
        "for config#2698 SpotQuotaExceededError on-demand fallback, first available at v0.124.1)",
    ),
    "alert-drain-liveness-probe": (
        "v0.83.0",
        "flow-doctor forum-topic routing (config#1742) — mirrors "
        "sf-watch-reclaim-sweep-handler's reclaim-checker exactly (config#3173)",
    ),
    "ci-watch-dispatcher": (
        "v0.124.5",
        "nousergon_lib.spot_dispatch chokepoint (config#2267: SpotProbeError handling; "
        "bumped for extra_tags atomic-launch-tagging, config#2292; bumped for config#2698 "
        "SpotQuotaExceededError on-demand fallback, first available at v0.124.1)",
    ),
    "ci-watch-liveness-probe": (
        "v0.83.0",
        "flow-doctor forum-topic routing (config#1742) — mirrors "
        "sf-watch-reclaim-sweep-handler's reclaim-checker exactly (config#3173)",
    ),
    "data-spot-dispatcher": (
        "v0.124.5",
        "ec2_spot launch chokepoint (config#1767); bumped for config#2698 "
        "SpotQuotaExceededError availability (krepis>=0.14.0, first shipped in "
        "krepis#28) — index.py's own _launch_instance now handles it directly "
        "(this Lambda calls nousergon_lib.ec2_spot.launch() directly rather than "
        "through spot_dispatch.launch_with_fallback)",
    ),
    "eod-backstop": (
        "v0.83.0",
        "trading_calendar coherence with sibling Lambdas",
    ),
    "eod-success-friday-shell-trigger": (
        "v0.83.0",
        "date helpers coherence with sf-telegram-notifier",
    ),
    "expense-collector": (
        "v0.83.0",
        "flow-doctor forum-topic routing (config#1742); OPS_HEALTH-only "
        "single-topic consumer, same exemption group as "
        "saturday-integrity-sentinel/sf-watch-reclaim-sweep-handler/pipeline-watchdog "
        "(alpha-engine-config#2843)",
    ),
    "freshness-monitor": (
        "v0.85.0",
        "flow-doctor event_driven + liveness_via (config#1747/1718/1726)",
    ),
    "friday-shell-run-report": (
        "v0.83.0",
        "trading_calendar coherence with eod-success-friday-shell-trigger",
    ),
    "overseer-liveness-probe": (
        "v0.83.0",
        "flow-doctor forum-topic routing (config#1742) — unified registry-driven "
        "watch-plane liveness probe; mirrors the sf-watch/groom probes it replaces "
        "(alpha-engine-config-I2831; groom probe deleted, sf-watch slimmed to its "
        "reclaim/sweep action paths)",
    ),
    "pipeline-watchdog": (
        "v0.83.0",
        "flow-doctor forum-topic routing (config#1742)",
    ),
    "saturday-integrity-sentinel": (
        "v0.83.0",
        "flow-doctor forum-topic routing (config#1742)",
    ),
    "saturday-sf-watch-dispatcher": (
        "v0.83.0",
        "flow-doctor forum-topic routing (config#1742)",
    ),
# scheduled-groom-dispatcher: EXEMPTION REMOVED 2026-07-27. Every reason it ever
# carried was a FLOOR ("bumped to vX for feature Y"), but exemptions are enforced
# as EQUALITY below — so the entry silently froze the Lambda at the version of its
# last feature need and turned a minimum into a ceiling. nousergon-lib v0.124.16
# moved TIER_MODELS["high"] to deepseek-v4-pro; the exemption held the Lambda at
# v0.124.15, so every live complexity:high groom kept dispatching claude-sonnet-5
# — violating groom-sweep-policy §5 and §7 — while root sat at v0.124.19 and CI
# stayed green. It now tracks root, with the floor pinned explicitly below.
# A floor is not an exemption: root >= floor already satisfies it.
    "sf-telegram-notifier": (
        "v0.83.0",
        "flow-doctor forum-topic routing (config#1742)",
    ),
    "sf-watch-reclaim-sweep-handler": (
        "v0.83.0",
        "flow-doctor forum-topic routing (config#1742)",
    ),
    "sf-watch-spot-dispatcher": (
        "v0.124.5",
        "nousergon_lib.spot_dispatch chokepoint (config#2267: SpotProbeError handling; "
        "bumped for extra_tags atomic-launch-tagging, config#2292; bumped for config#2698 "
        "SpotQuotaExceededError on-demand fallback, first available at v0.124.1)",
    ),
    "spot-orphan-reaper": (
        "v0.97.0",
        "telegram alert shape for CI-watch (config#2106)",
    ),
}


def _read_pin(filename: str, regex: re.Pattern[str]) -> str:
    text = (_REPO_ROOT / filename).read_text()
    match = regex.search(text)
    assert match is not None, (
        f"could not find nousergon-lib pin in {filename}"
    )
    return match.group(1)


def test_requirements_and_dockerfile_pins_match():
    req_pin = _read_pin("requirements.txt", _REQUIREMENTS_PIN_RE)
    docker_pin = _read_pin("Dockerfile", _DOCKERFILE_PIN_RE)
    daily_news_pin = _read_pin("requirements-daily-news.txt", _REQUIREMENTS_PIN_RE)
    deploy_infra_pin = _read_pin(
        ".github/workflows/deploy-infrastructure.yml", _LAMBDA_PIN_RE
    )
    assert req_pin == docker_pin == daily_news_pin == deploy_infra_pin, (
        f"nousergon-lib pin drift: requirements.txt={req_pin!r}, "
        f"Dockerfile={docker_pin!r}, requirements-daily-news.txt={daily_news_pin!r}, "
        f".github/workflows/deploy-infrastructure.yml={deploy_infra_pin!r}. "
        f"All four must move in lockstep — the Dockerfile strips lib from "
        f"requirements.txt before pip install, so requirements-only bumps "
        f"don't propagate to the Lambda image, the slim daily-news file "
        f"carries an independent copy of the pin, and the deploy-infrastructure "
        f"workflow's drift-check step installs its own copy directly."
    )


def test_lambda_pins_match_or_are_explicitly_exempted():
    root_pin = _read_pin("requirements.txt", _REQUIREMENTS_PIN_RE)
    lambdas_dir = _REPO_ROOT / "infrastructure" / "lambdas"

    for req_file in sorted(lambdas_dir.glob("*/requirements.txt")):
        lambda_name = req_file.parent.name
        text = req_file.read_text()
        match = _LAMBDA_PIN_RE.search(text)

        if match is None:
            continue

        lambda_pin = match.group(1)

        if lambda_name in _LAMBDA_PIN_EXEMPTIONS:
            exempted_pin, reason = _LAMBDA_PIN_EXEMPTIONS[lambda_name]
            assert (
                lambda_pin == exempted_pin
            ), f"{lambda_name}: pin {lambda_pin!r} does not match exempted pin {exempted_pin!r} (reason: {reason})"
        else:
            assert (
                lambda_pin == root_pin
            ), f"{lambda_name}: pin {lambda_pin!r} must match root pin {root_pin!r}, or be added to _LAMBDA_PIN_EXEMPTIONS with a contract reason"


# --------------------------------------------------------------------------- #
# Tier→model conformance floor (groom-sweep-policy §2.3 / §5).
#
# The groom dispatcher's launch decisions come from the *pinned lib*, not from
# the Lambda's own code — `nousergon_lib.groom_eligibility.TIER_MODELS` is the
# single owner of the tier→model assignment. So the policy's tier table is only
# true in production if the pinned lib is new enough to contain it.
# --------------------------------------------------------------------------- #

#: First nousergon-lib release where TIER_MODELS["high"] == "deepseek-v4-pro"
#: (nousergon-lib#252). Below this, live high-tier grooms dispatch claude-sonnet-5,
#: violating groom-sweep-policy §5 (tier table) and §7 (no Claude for groom traffic).
_TIER_MODEL_FLOOR = (0, 124, 16)


def _version_tuple(pin: str) -> tuple[int, int, int]:
    return tuple(int(part) for part in pin.lstrip("v").split("."))


def test_groom_dispatcher_pin_can_express_the_policy_tier_table():
    """The dispatcher must bundle a lib new enough to know high == deepseek-v4-pro.

    This is the check groom-sweep-policy §2.3 demands for the §5 tier table: it
    fails if the pin regresses below the release that carries the assignment,
    however that regression happens (manual edit, revived exemption, bad merge).
    """
    pin = _read_pin(
        "infrastructure/lambdas/scheduled-groom-dispatcher/requirements.txt",
        _LAMBDA_PIN_RE,
    )
    assert _version_tuple(pin) >= _TIER_MODEL_FLOOR, (
        f"scheduled-groom-dispatcher pins nousergon-lib {pin}, which predates "
        f"TIER_MODELS['high'] = 'deepseek-v4-pro'. Live complexity:high grooms "
        f"would dispatch claude-sonnet-5, violating groom-sweep-policy §5 and §7."
    )


def test_groom_dispatcher_is_not_exempted_from_the_root_pin():
    """Regression guard for the removed exemption.

    Re-adding `scheduled-groom-dispatcher` to `_LAMBDA_PIN_EXEMPTIONS` would
    reinstate equality-pinning and let the lib go stale silently again. If it ever
    genuinely needs a CEILING (a real incompatibility with root, not a floor),
    that is a deliberate change that must also update this test and say why.
    """
    assert "scheduled-groom-dispatcher" not in _LAMBDA_PIN_EXEMPTIONS, (
        "the groom dispatcher must track the root pin — its historical exemption "
        "reasons were all floors, and equality-pinning them froze the lib"
    )
