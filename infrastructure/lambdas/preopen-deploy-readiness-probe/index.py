"""alpha-engine-preopen-deploy-readiness-probe — pre-preopen deploy-readiness
probe with runway (alpha-engine-config-I7800 deliverable #2).

**Why this Lambda exists.** 2026-08-19: `Deploy Infrastructure` failed 3x on
`nousergon-data@main`. Each failure fired the ordinary `notify-main-failure`
Telegram alert; nothing connected those failures to the fact that they had
ALREADY DECIDED the outcome of the next morning's 05:15 PT preopen — the live
SF stamp was frozen behind `main`, so the next `DeployDriftGate` invocation
would halt trading. It did (2026-08-20). Detected-and-paged (the CI alert) is
partial coverage (`principles.md` §2.3): nothing closed the loop by (a) trying
to self-heal before the deadline and (b) escalating with runway to spare if
self-heal didn't work.

**Mechanism.** Scheduled 04:30 America/Los_Angeles MON-FRI (Scheduler-native
timezone, DST-correct) — 45 minutes before the 05:15 PT preopen trigger.

  1. Not a NYSE trading day (`nousergon_lib.trading_calendar.is_trading_day`)
     -> no-op. No preopen is expected, so no readiness check is meaningful.
  2. Trading day -> invoke `alpha-engine-predictor-inference:live`
     `action=check_deploy_drift` (the SAME probe `DeployDriftCheck` runs
     inside the preopen SF itself — one implementation, not a second one
     free to drift; see `alpha-engine-predictor/inference/deploy_drift.py`).
  3. `sf_drift=false` -> clean. Write the verdict, no page, no dispatch.
  3a. `sf_drift` ABSENT (or `sf_drift_state == "unmeasured"`) -> the probe
     could not measure definition freshness at all. NOT clean: this is the
     exact input the preopen `DeployDriftGate` fail-closed branch halts on
     45 minutes later, so it pages immediately, with runway, and does NOT
     self-heal — a `workflow_dispatch` on an unmeasured verdict acts on
     evidence nobody has (alpha-engine-config-I8142). Reading absence as
     clean is the same defect one hop downstream from the one I8142 fixed
     in the probe itself.
  4. `sf_drift=true` -> self-heal: fire a `workflow_dispatch` on
     `nousergon-data`'s `deploy-infrastructure.yml` (it is idempotent by
     design — re-running it is always safe, see that workflow's own header
     comment), using the fine-grained PAT already provisioned at
     `GITHUB_PAT_SSM_PARAM` for exactly this kind of cross-repo GitHub call
     (mirrors `saturday-sf-watch-dispatcher._get_github_pat`, same SSM
     parameter — one PAT, not a second one to provision and rotate).
     Poll the dispatched run to a terminal conclusion (bounded — see
     `_DISPATCH_POLL_*`), then re-probe `check_deploy_drift`.
     - Still `sf_drift=true` after self-heal (or the dispatch/poll itself
       failed) -> page `alpha-engine-alerts` (severity=critical) with the
       failing validation diagnostics and the literal sentence that the
       05:15 PT preopen will halt.
     - Self-heal cleared the drift -> log the recovery; no page (a probe
       that pages after fixing the problem trains the operator to ignore
       pages).

**Console emission (§2.7 — a silent probe is not an observation).** Every
invocation — clean, self-healed, or escalated — writes a verdict JSON to
`s3://{BUCKET}/deploy_readiness/{date}.json`, the same "one small artifact per
run" shape the fleet's other scheduled probes use for their console surface
(mirrors `eod-precondition-probe`'s sentinel-read pattern in reverse: this
Lambda is the WRITER of its own observation record).

**Fail-loud posture.** The initial `check_deploy_drift` invoke and the S3
verdict write RAISE on any unexpected error (CloudWatch Errors metric is the
backstop). The self-heal dispatch/poll and the page are best-effort/secondary:
a GitHub API hiccup on the self-heal leg must not crash the probe before it
gets to page — the page IS the fallback for exactly that case.

Deliberately NOT a second implementation of drift detection or of market-hours
calendar logic — both are read from the SAME sources their live consumers use
(the predictor Lambda's own probe; `nousergon_lib.trading_calendar`), per the
sf-pipeline-policy §3 "one answer, one owner" convention this fleet already
follows for `weekday_sf_rerun.py`'s market-hours check (alpha-engine-config-
I7807).
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
from datetime import date, datetime, timezone
from urllib.error import HTTPError

import boto3
from nousergon_lib import alerts
from nousergon_lib.trading_calendar import is_trading_day

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
ACCOUNT_ID = os.environ.get("ACCOUNT_ID", "711398986525")
BUCKET = os.environ.get("PROBE_BUCKET", "alpha-engine-research")
VERDICT_PREFIX = os.environ.get("PROBE_VERDICT_PREFIX", "deploy_readiness")

DRIFT_PROBE_FUNCTION = os.environ.get(
    "DRIFT_PROBE_FUNCTION", "alpha-engine-predictor-inference:live"
)

SNS_TOPIC_ARN = os.environ.get(
    "ALPHA_ENGINE_ALERTS_SNS_TOPIC_ARN",
    f"arn:aws:sns:{REGION}:{ACCOUNT_ID}:alpha-engine-alerts",
)

# Same fine-grained PAT SSM parameter saturday-sf-watch-dispatcher already
# reads (see that Lambda's `_get_github_pat` / `GITHUB_PAT_SSM_PARAM`) —
# "scoped to the SF-path repos", which nousergon-data (this repo, the
# workflow_dispatch target) already is. ONE PAT, not a second one to
# provision and rotate. If its scope turns out NOT to cover
# `actions:write` on nousergon-data, the dispatch call fails 403 and the
# probe falls through to paging exactly as if self-heal had not fixed the
# drift — never silently.
GITHUB_PAT_SSM_PARAM = os.environ.get(
    "GITHUB_PAT_SSM_PARAM", "/alpha-engine/saturday_sf_watch/github_pat"
)
DISPATCH_REPO = os.environ.get("DISPATCH_REPO", "nousergon/nousergon-data")
DISPATCH_WORKFLOW = os.environ.get("DISPATCH_WORKFLOW", "deploy-infrastructure.yml")
DISPATCH_REF = os.environ.get("DISPATCH_REF", "main")

_HTTP_TIMEOUT_SEC = 15
# Bounded poll for the self-heal dispatch to finish: 20 attempts x 20s = ~6.7
# minutes, comfortably inside this Lambda's 10-minute configured timeout
# (see deploy.sh) with margin for the drift-probe invokes on either side.
# deploy-infrastructure.sh normally completes in well under 2 minutes on a
# no-op CF update (module docstring, deploy-infrastructure.yml header); this
# budget covers a genuine CFN update-stack wait too.
_DISPATCH_POLL_ATTEMPTS = int(os.environ.get("DISPATCH_POLL_ATTEMPTS", "20"))
_DISPATCH_POLL_INTERVAL_SEC = int(os.environ.get("DISPATCH_POLL_INTERVAL_SEC", "20"))

_ET_PREOPEN_NOTE = (
    "the 05:15 America/Los_Angeles ne-preopen-trading-pipeline preopen will "
    "halt at DeployDriftGate unless this is resolved before then"
)


def _get_github_pat() -> str:
    """Read the fine-grained PAT (SecureString) from SSM. Never logged."""
    ssm = boto3.client("ssm", region_name=REGION)
    resp = ssm.get_parameter(Name=GITHUB_PAT_SSM_PARAM, WithDecryption=True)
    return resp["Parameter"]["Value"]


def _invoke_check_deploy_drift(lam_client) -> dict:
    """Call the SAME `action=check_deploy_drift` handler DeployDriftCheck
    invokes inside the preopen SF (alpha-engine-predictor Lambda). Raises on
    any invoke-level failure — this is the primary probe, fail-loud."""
    resp = lam_client.invoke(
        FunctionName=DRIFT_PROBE_FUNCTION,
        Payload=json.dumps({"action": "check_deploy_drift"}).encode(),
    )
    payload = json.loads(resp["Payload"].read())
    if resp.get("FunctionError"):
        raise RuntimeError(f"check_deploy_drift invoke returned FunctionError: {payload}")
    return payload


def _dispatch_deploy_infrastructure() -> tuple[bool, str]:
    """Fire workflow_dispatch on nousergon-data's deploy-infrastructure.yml.

    Returns (dispatched, detail). Best-effort — any failure here (bad PAT
    scope, GitHub outage) is caught and reported, never raised, so the probe
    can still fall through to paging.
    """
    try:
        pat = _get_github_pat()
        payload = json.dumps({"ref": DISPATCH_REF}).encode()
        req = urllib.request.Request(
            f"https://api.github.com/repos/{DISPATCH_REPO}/actions/workflows/"
            f"{DISPATCH_WORKFLOW}/dispatches",
            data=payload,
            method="POST",
            headers={
                "Authorization": f"Bearer {pat}",
                "Accept": "application/vnd.github+json",
                "Content-Type": "application/json",
                "User-Agent": "preopen-deploy-readiness-probe",
            },
        )
        with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT_SEC) as resp:
            status = resp.status
        return True, f"workflow_dispatch accepted (http={status})"
    except HTTPError as exc:
        body = exc.read().decode(errors="replace") if exc.fp else ""
        return False, f"workflow_dispatch HTTP {exc.code}: {body[:300]}"
    except Exception as exc:  # noqa: BLE001 — best-effort self-heal leg
        return False, f"workflow_dispatch failed: {type(exc).__name__}: {exc}"


def _poll_dispatch_conclusion(dispatched_at: datetime) -> tuple[str, str]:
    """Poll for the workflow_dispatch run's terminal conclusion.

    Returns (conclusion, detail). conclusion is one of "success", "failure",
    "timed_out" (poll budget exhausted before a terminal state), or
    "poll_error" (couldn't even list runs). Never raises — same best-effort
    posture as the dispatch call itself.
    """
    try:
        pat = _get_github_pat()
    except Exception as exc:  # noqa: BLE001
        return "poll_error", f"could not re-read PAT for polling: {exc}"

    for attempt in range(_DISPATCH_POLL_ATTEMPTS):
        time.sleep(_DISPATCH_POLL_INTERVAL_SEC)
        try:
            req = urllib.request.Request(
                f"https://api.github.com/repos/{DISPATCH_REPO}/actions/workflows/"
                f"{DISPATCH_WORKFLOW}/runs?event=workflow_dispatch&branch={DISPATCH_REF}"
                f"&per_page=5",
                headers={
                    "Authorization": f"Bearer {pat}",
                    "Accept": "application/vnd.github+json",
                    "User-Agent": "preopen-deploy-readiness-probe",
                },
            )
            with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT_SEC) as resp:
                body = json.loads(resp.read())
        except Exception as exc:  # noqa: BLE001 — transient poll error, keep trying
            logger.warning("dispatch-poll attempt %d failed: %s", attempt, exc)
            continue

        for run in body.get("workflow_runs", []):
            created_at = datetime.strptime(
                run["created_at"], "%Y-%m-%dT%H:%M:%SZ"
            ).replace(tzinfo=timezone.utc)
            if created_at < dispatched_at:
                continue
            status = run.get("status")
            if status == "completed":
                conclusion = run.get("conclusion") or "unknown"
                return conclusion, f"run {run.get('id')} completed: {conclusion}"
            # Found the run but it's still in progress — keep polling.
            break

    return "timed_out", (
        f"no matching run reached a terminal state within "
        f"{_DISPATCH_POLL_ATTEMPTS * _DISPATCH_POLL_INTERVAL_SEC}s"
    )


#: The three states a drift verdict can be in. `check_deploy_drift` OMITS
#: `sf_drift` when it could not measure it (alpha-engine-config-I7048, I7924,
#: I8142) and, since I8142, also states it positively in `sf_drift_state`. Read
#: the positive field when the predictor Lambda is new enough to emit it, and
#: fall back to key presence otherwise, so this probe is correct across the
#: rollout window rather than only after it.
_DRIFT = "drift"
_NO_DRIFT = "no_drift"
_UNMEASURED = "unmeasured"


def _drift_state(drift: dict) -> str:
    """Three states, never two. `drift.get("sf_drift")` collapses "no" and "no
    answer" into one falsy value — the shape behind every instance of this
    defect class, including the one on the halting gate itself (I8142)."""
    state = drift.get("sf_drift_state")
    if state in (_DRIFT, _NO_DRIFT, _UNMEASURED):
        return state
    if "sf_drift" not in drift:
        return _UNMEASURED
    return _DRIFT if drift["sf_drift"] else _NO_DRIFT


def _page_unmeasured(drift: dict) -> None:
    """The verdict could not be measured. The preopen gate halts on exactly this
    payload (`Not IsPresent($.drift_result.Payload.sf_drift) -> HandleFailure`),
    so the whole point of this probe — runway before the 05:15 PT trigger — is
    to say so now rather than let the session discover it."""
    message = (
        f"Preopen deploy-readiness probe (alpha-engine-config-I8142): the "
        f"deploy-drift verdict is UNMEASURED — the probe could not determine "
        f"whether the live preopen definition matches what the deploy "
        f"published, so THE 05:15 PT PREOPEN WILL HALT at DeployDriftGate "
        f"(its fail-closed branch reads an absent sf_drift). "
        f"{_ET_PREOPEN_NOTE}. sf_drift_reason={drift.get('sf_drift_reason')!r} "
        f"sf_definition_reason={drift.get('sf_definition_reason')!r} "
        f"live_definition_error={drift.get('live_definition_error')!r} "
        f"sf_sha={drift.get('sf_sha')!r} upstream_sha={drift.get('upstream_sha')!r}. "
        f"No self-heal was attempted: a deploy dispatch on an unmeasured "
        f"verdict would act on evidence nobody has. "
        f"`live_definition_unreadable` means the predictor role could not run "
        f"states:DescribeStateMachine on the preopen state machine — check the "
        f"DeployDriftProbe statement of alpha-engine-predictor-inference's role "
        f"(nous-ergon-ops/infrastructure/iam) before the trigger."
    )
    result = alerts.publish(
        message=message,
        severity="critical",
        source="alpha-engine-preopen-deploy-readiness-probe",
        sns=True,
        telegram=False,
        sns_topic_arn=SNS_TOPIC_ARN,
    )
    logger.error(
        "DEPLOY-READINESS-PROBE ALERT: sf_drift UNMEASURED (reason=%s) sns_ok=%s",
        drift.get("sf_drift_reason"), result.sns.ok,
    )


def _page(drift: dict, self_heal_detail: str) -> None:
    message = (
        f"Preopen deploy-readiness probe (alpha-engine-config-I7800): the "
        f"deployed SF stamp is STILL behind main after self-heal — "
        f"{_ET_PREOPEN_NOTE}. sf_drift={drift.get('sf_drift')} "
        f"sf_sha={drift.get('sf_sha')!r} upstream_sha={drift.get('upstream_sha')!r} "
        f"cf_drift={drift.get('cf_drift')} cf_drift_reason={drift.get('cf_drift_reason')!r}. "
        f"Self-heal: {self_heal_detail}. "
        f"Manual recovery: `bash infrastructure/deploy-infrastructure.sh` "
        f"from a clean nousergon-data checkout (AWS_PROFILE=ne-admin), or "
        f"`gh workflow run deploy-infrastructure.yml --repo {DISPATCH_REPO}`."
    )
    result = alerts.publish(
        message=message,
        severity="critical",
        source="alpha-engine-preopen-deploy-readiness-probe",
        sns=True,
        telegram=False,
        sns_topic_arn=SNS_TOPIC_ARN,
    )
    logger.warning(
        "DEPLOY-READINESS-PROBE ALERT: sf_drift=%s sns_ok=%s",
        drift.get("sf_drift"), result.sns.ok,
    )


def _write_verdict(s3client, today: date, verdict: dict) -> None:
    key = f"{VERDICT_PREFIX}/{today.isoformat()}.json"
    s3client.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(verdict, indent=2, default=str).encode(),
        ContentType="application/json",
    )
    logger.info("verdict written to s3://%s/%s", BUCKET, key)


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    today = datetime.now(timezone.utc).date()

    if not is_trading_day(today):
        logger.info("Not a NYSE trading day (%s) — no preopen expected; no-op.", today)
        return {"action": "noop", "reason": "not_a_trading_day", "date": str(today)}

    lam_client = boto3.client("lambda", region_name=REGION)
    s3client = boto3.client("s3", region_name=REGION)

    drift = _invoke_check_deploy_drift(lam_client)
    state = _drift_state(drift)
    verdict: dict = {
        "date": str(today),
        "sf_drift_initial": drift.get("sf_drift"),
        "sf_drift_state_initial": state,
        "cf_drift_initial": drift.get("cf_drift"),
        "drift_initial": drift,
    }

    if state == _UNMEASURED:
        # alpha-engine-config-I8142. `drift.get("sf_drift")` read absence as
        # clean, so an unmeasured verdict — the very input the preopen gate
        # halts on — produced action=noop reason=clean and no page, 45 minutes
        # before the session it had already decided.
        verdict["action"] = "paged"
        verdict["reason"] = "unmeasured"
        _page_unmeasured(drift)
        _write_verdict(s3client, today, verdict)
        return verdict

    if state == _NO_DRIFT:
        verdict["action"] = "noop"
        verdict["reason"] = "clean"
        _write_verdict(s3client, today, verdict)
        logger.info("Deploy stamp clean for %s — no self-heal, no page.", today)
        return verdict

    logger.warning(
        "sf_drift=true for %s — attempting self-heal dispatch of %s on %s.",
        today, DISPATCH_WORKFLOW, DISPATCH_REPO,
    )
    dispatched_at = datetime.now(timezone.utc)
    dispatched, dispatch_detail = _dispatch_deploy_infrastructure()
    verdict["self_heal_dispatched"] = dispatched
    verdict["self_heal_dispatch_detail"] = dispatch_detail

    if dispatched:
        conclusion, poll_detail = _poll_dispatch_conclusion(dispatched_at)
        verdict["self_heal_run_conclusion"] = conclusion
        verdict["self_heal_poll_detail"] = poll_detail
        self_heal_detail = f"{dispatch_detail}; {poll_detail}"
    else:
        self_heal_detail = dispatch_detail

    drift_after = _invoke_check_deploy_drift(lam_client)
    state_after = _drift_state(drift_after)
    verdict["sf_drift_after_self_heal"] = drift_after.get("sf_drift")
    verdict["sf_drift_state_after_self_heal"] = state_after
    verdict["drift_after_self_heal"] = drift_after

    if state_after == _UNMEASURED:
        # I8142, the same collapse on the re-probe: an unmeasured verdict here
        # is not "the self-heal worked", it is "we no longer know", and the
        # preopen gate will still halt on it.
        verdict["action"] = "paged"
        verdict["reason"] = "unmeasured_after_self_heal"
        _page_unmeasured(drift_after)
    elif state_after == _DRIFT:
        verdict["action"] = "paged"
        verdict["reason"] = "still_drifted_after_self_heal"
        _page(drift_after, self_heal_detail)
    else:
        verdict["action"] = "self_healed"
        verdict["reason"] = "drift_cleared_by_dispatch"
        logger.info("Self-heal cleared drift for %s — no page.", today)

    _write_verdict(s3client, today, verdict)
    return verdict
