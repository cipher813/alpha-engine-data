"""alpha-engine-research-eval-judge-spot-dispatcher — launch the dedicated EC2
spot box the weekly SF's ``EvalJudgeProcess`` stage now runs on
(alpha-engine-config-I9329, following -I9309).

WHY (Brian, 2026-08-29, verbatim): *"perhaps if the lambda times out then we
need to put the judge on a spot instance"*, and *"ok lets keep 100% coverage
and move to spot."*

``EvalJudgeProcess`` ran as a Lambda under a 960s state ceiling against a
900s function ceiling. At the measured 45-105s per synchronous judge call it
covered roughly 8-15 of an ~83-artifact corpus, reported ``complete=False``
honestly, and returned SUCCESS. crucible-research-PR766 made coverage a
pass/fail verdict and added ``evals/judge_spot_run.py``, the
substrate-independent entrypoint with no deadline. This Lambda is the
substrate.

## Topology

``EvalJudgeSubmit{Weekly,FirstSaturday}`` stay Lambdas — seconds of S3
listing, no LLM call, no ceiling risk. The four ``EvalJudgePoll*`` states and
the ``alpha-engine-research-eval-judge-poll`` Lambda are DELETED: they existed
to drive an asynchronous provider batch API that is retired
(alpha-engine-config-I9263, Brian: *"at this point we shouldn't be using the
anthropic api at all"*), and a state that can only ever fall straight through
is a reader's trap. ``EvalJudgeProcess`` keeps its NAME — it is keyed on by
``eval_artifact_latest.produced_by``, by ``AggregateCosts.required_producers``
and by the stage-coverage registry — and changes its SUBSTRATE to
``aws-sdk:ssm:sendCommand`` against the box this Lambda launches.

## Mechanism — the weekly-freshness-spot-dispatcher shape, not the thinktank one

Both siblings launch a spot box through ``nousergon_lib.spot_dispatch``. They
differ in who drives the workload, and that difference is the whole reason
this file mirrors one and not the other:

* ``thinktank-spot-dispatcher`` fires ONE async command that runs the entire
  workload and self-terminates. Nothing polls it. That is correct for an
  EventBridge-triggered daily arm.
* ``weekly-freshness-spot-dispatcher`` fires an async BOOTSTRAP command only,
  returns ``instance_id`` + ``command_id``, and the Step Function polls that
  command to Success before any stage sends work to the box.

The judge is the second shape. The Step Function owns the run, because the
run's outcome — a coverage verdict — must reach the SF as a stage status,
and an SSM ``ResponseCode`` the SF reads is the only signal that survives
before anyone opens a log. So this Lambda bootstraps and stops.

Consequence, stated because it inverts the thinktank contract:
``crucible-research/infrastructure/eval_judge_spot_bootstrap.sh`` must NOT
self-terminate. A ``shutdown -h now`` at the end of bootstrap would pull the
box out from under the stage that is about to use it. Orphan prevention is the
two ``krepis.spot_bootstrap`` timers armed BEFORE that script runs (the
dead-man and ``max_runtime_seconds``), plus the fleet ``spot-orphan-reaper``
age cap, and the run command the SF sends ends by detaching a delayed
``shutdown``.

## Router addressing (principles.md §2.8, model-router-policy §3.4a R27a)

Three facts the box cannot derive for itself, so the launcher states them —
identical in intent to the thinktank dispatcher's block, with one substitution:

    where it runs      a stock-AMI spot box, so the dashboard box's local
                       egress proxy at 127.0.0.1:8990 does NOT answer here
                       and the authenticated edge is the only path;
    which URL          the router is addressed by (url, credential) and
                       reaching it may not depend on host, VPC, subnet, SG
                       or private IP;
    which credential   the edge identifies a consumer BY its credential
                       VALUE, and ``krepis.secrets`` resolves SSM BEFORE
                       ``os.environ``, so sharing the secret NAME
                       ``LITELLM_MASTER_KEY`` would collapse this box into
                       the director's identity however the environment is
                       set. ``ROUTER_CONSUMER_RESEARCH`` is already
                       provisioned (verified 2026-08-29:
                       ``/alpha-engine/ROUTER_CONSUMER_RESEARCH`` exists, and
                       ``alpha-engine-executor-role`` grants
                       ``ssm:GetParameter`` on ``/alpha-engine/*``).

Nothing here names a model id, a base URL, a provider or an SDK client. The
registry itself comes from AppConfig, because crucible-research is PUBLIC and
``private-docs/LLM_MODEL_REGISTRY.yaml`` is correctly absent from the clone.

These are exported into ``ENV_FILE`` on the box rather than into the bootstrap
command's own environment, and that is load-bearing: the SF's later
``ssm:sendCommand`` is a SEPARATE shell with a SEPARATE environment. An export
that lived only in the bootstrap process would be gone by the time the judge
runs, and ``judge_exec_context()`` would silently answer "lambda" from a spot
box — the exact class of silent substrate mismatch this stage exists to end.

## Fail-loud

A launch/SSM failure RAISES. The SF's ``DispatchEvalJudgeSpot`` Catch routes to
``MarkEvalJudgeDegraded`` — eval is observability (ROADMAP §1635) and must not
halt the Saturday pipeline — so the degradation is recorded rather than
swallowed. There is no fail-open branch and no ``{"launched": false}``: a
dispatch that was supposed to happen and did not must never be
indistinguishable from a healthy no-op.

## Deployment

Managed OUTSIDE CloudFormation, same as every sibling dispatcher.
``deploy.sh`` flagless is code-only and runs on merge via
``.github/workflows/deploy-eval-judge-spot-dispatcher.yml``. ``--bootstrap``
adds IAM-role and Lambda-function CREATION, which the
``github-actions-lambda-deploy`` OIDC role deliberately cannot do (fleet-wide
after four IAM-clobber incidents in two months). That first bootstrap is
therefore an operator step at a real privilege boundary, and TWO detectors
stay red until it runs: this Lambda's own deploy workflow (its preflight
prints the exact command and fails), and
``infrastructure/step-functions/check-lambda-existence.py`` via
``sf-arn-drift-check.yml`` on every push to main and daily.

The function name is deliberate. ``alpha-engine-step-functions-role``'s
existing ``lambda:InvokeFunction`` grant already carries the resource wildcard
``arn:aws:lambda:us-east-1:711398986525:function:alpha-engine-research-eval-judge*``
(verified live 2026-08-29), so the SF's invoke needs no IAM change at all.
"""

from __future__ import annotations

import datetime
import logging
import os
import uuid

from krepis.spot_bootstrap import SpotBootstrapSpec, render_bootstrap
from nousergon_lib import spot_dispatch
from nousergon_lib.spot_dispatch import SpotLaunchError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")

# Kill-switch. There is deliberately NO fail-open branch on the SF side: this
# raises, and the SF's Catch records a degradation. Flipping it off means "the
# judge does not run this week", said out loud.
DISPATCH_ENABLED = (
    os.environ.get("EVAL_JUDGE_SPOT_DISPATCH_ENABLED", "true").lower() == "true"
)

# ── Spot launch config ──────────────────────────────────────────────────────
# The workload is LLM/network-bound, not CPU-bound (the Lambda it replaces
# peaked well under its 1024MB), so the smallest standard tier is right. The
# multi-type list exists for CAPACITY RESILIENCE, not performance — the number
# of distinct instance_type x subnet pools `launch_with_fallback` can rotate
# through is what decides whether a capacity dip is survivable. Same ten types
# the weekly-freshness launcher widened to on alpha-engine-config-I7133, all
# x86_64 (the AMI is x86_64 AL2023), 2 vCPU, >= 4 GiB.
INSTANCE_TYPES = [
    t.strip()
    for t in os.environ.get(
        "EVAL_JUDGE_SPOT_INSTANCE_TYPES",
        "c5.large,m5.large,c6i.large,c5a.large,m6i.large,"
        "m5a.large,c6a.large,m6a.large,r5.large,r6i.large",
    ).split(",")
    if t.strip()
]
SUBNETS = [
    s.strip()
    for s in os.environ.get(
        "EVAL_JUDGE_SPOT_SUBNETS",
        "subnet-a61ec0fb,subnet-1e58307a,subnet-789d3857,"
        "subnet-c670118d,subnet-7cff7c43,subnet-e07166ec",
    ).split(",")
    if s.strip()
]
AMI_ID = os.environ.get("EVAL_JUDGE_SPOT_AMI_ID", "ami-0c421724a94bba6d6")  # AL2023 x86_64
KEY_NAME = os.environ.get("EVAL_JUDGE_SPOT_KEY_NAME", "alpha-engine-key")
SECURITY_GROUP = os.environ.get("EVAL_JUDGE_SPOT_SECURITY_GROUP", "sg-03cd3c4bd91e610b0")
# The same profile every sibling spot box uses: ssm:GetParameter on
# /alpha-engine/* (the config PAT and ROUTER_CONSUMER_RESEARCH), the AppConfig
# session for the model registry, and read/write on s3://alpha-engine-research.
# VERIFIED live 2026-08-29 — no IAM change is needed for the box.
IAM_PROFILE = os.environ.get("EVAL_JUDGE_SPOT_IAM_PROFILE", "alpha-engine-executor-profile")
# One shallow public clone (crucible-research) + one private config clone
# (prompts) + one venv carrying the research stack. 40GB matches the sibling
# launchers' sizing, and for the same reason: the venv, not the data.
VOLUME_SIZE_GB = int(os.environ.get("EVAL_JUDGE_SPOT_VOLUME_SIZE_GB", "40"))

RESEARCH_REPO = os.environ.get("EVAL_JUDGE_SPOT_RESEARCH_REPO", "nousergon/crucible-research")
RESEARCH_BRANCH = os.environ.get("EVAL_JUDGE_SPOT_RESEARCH_BRANCH", "main")
RESEARCH_CHECKOUT = "/home/ec2-user/crucible-research"

#: The env file the SF's LATER sendCommand sources. See the module docstring:
#: two SSM commands are two shells, so the router addressing has to be written
#: to disk here or it does not exist when the judge runs.
ENV_FILE = "/home/ec2-user/eval-judge.env"

# ── Timing ──────────────────────────────────────────────────────────────────
# Bootstrap only: one shallow public clone, one private clone, a gitleaks
# install and one venv build. 30 min is large headroom over the realistic
# low-single-digit minutes, sized so a cold pip index or a slow dnf mirror is
# never mistaken for an infrastructure fault. It is NOT the judge's budget —
# that lives on the SF's EvalJudgeProcess state, where the run happens.
BOOTSTRAP_TIMEOUT_SECONDS = int(
    os.environ.get("EVAL_JUDGE_SPOT_BOOTSTRAP_TIMEOUT_SECONDS", "1800")
)
SSM_ONLINE_BUDGET_SEC = int(os.environ.get("EVAL_JUDGE_SPOT_SSM_ONLINE_BUDGET_SEC", "300"))
CW_LOG_GROUP = os.environ.get(
    "EVAL_JUDGE_SPOT_CW_LOG_GROUP", "/alpha-engine/eval-judge-spot"
)

# Orphan-prevention backstop ONLY — never fires on a healthy run, and nothing
# on the happy path depends on it. Sized above the sum of every ceiling the box
# can legitimately consume:
#
#   BOOTSTRAP_TIMEOUT_SECONDS      1800   the bootstrap command
#   + EVAL_JUDGE_EXECUTION_TIMEOUT 10800  the judge run (SF executionTimeout)
#   + slack for the SF's 30s poll granularity and the detached shutdown
#   = 12600 + slack
#
# 16200s (4.5h) leaves ~1h. It must stay ABOVE that sum: a watchdog that fires
# mid-run destroys the coverage this whole stage exists to guarantee, and a
# coverage shortfall is a HARD failure by Brian's ruling — so the watchdog
# would convert a healthy run into a stage failure. `handler` refuses to launch
# if the inequality is violated, rather than discovering it as a truncated
# corpus once a week.
#
# The mirror-image risk is real too and is why this is not simply enormous:
# the box bills until something stops it, and the reaper's fleet age cap is
# 6.5h, so an unbounded watchdog just moves the ceiling somewhere less visible.
WATCHDOG_SECONDS = int(os.environ.get("EVAL_JUDGE_SPOT_WATCHDOG_SECONDS", "16200"))
#: Tier one of two (see `krepis.spot_bootstrap`): the dead-man, armed before
#: the script can fail, above `max_runtime_seconds`. A bootstrap that dies
#: before arming its real cap otherwise leaves a box running with no watchdog.
DEADMAN_SECONDS = int(os.environ.get("EVAL_JUDGE_SPOT_DEADMAN_SECONDS", "18000"))
#: The SF-side judge budget this dispatcher's watchdog must exceed. Declared
#: here so the inequality is checkable from the launcher, and asserted against
#: the Step Function definition by tests/test_sf_eval_judge_wiring.py — a
#: constant that only one side knows is not a coupling, it is a coincidence.
EVAL_JUDGE_EXECUTION_TIMEOUT_SECONDS = int(
    os.environ.get("EVAL_JUDGE_EXECUTION_TIMEOUT_SECONDS", "10800")
)

# ── Router addressing (see the module docstring) ────────────────────────────
KREPIS_EXEC_CONTEXT = os.environ.get("EVAL_JUDGE_SPOT_EXEC_CONTEXT", "ec2")
KREPIS_LITELLM_PROXY_URL = os.environ.get(
    "EVAL_JUDGE_SPOT_ROUTER_URL", "https://router.nousergon.ai:8443"
)
KREPIS_ROUTER_CREDENTIAL_SECRET = os.environ.get(
    "EVAL_JUDGE_SPOT_ROUTER_CREDENTIAL_SECRET", "ROUTER_CONSUMER_RESEARCH"
)
KREPIS_APPCONFIG_APPLICATION = os.environ.get(
    # IAM-scoped AppConfig application for the live registry (yq405wh), not the
    # legacy alpha-engine id — same fix as crucible-research#778 on submit.
    "EVAL_JUDGE_SPOT_APPCONFIG_APPLICATION", "yq405wh"
)
KREPIS_APPCONFIG_CONFIG_PROFILE = os.environ.get(
    "EVAL_JUDGE_SPOT_APPCONFIG_CONFIG_PROFILE", "llm-model-registry"
)
KREPIS_APPCONFIG_ENVIRONMENT = os.environ.get(
    "EVAL_JUDGE_SPOT_APPCONFIG_ENVIRONMENT", "production"
)

# ── Cost-sink addressing (config-I7179) ─────────────────────────────────────
# krepis>=0.57.0 resolves the process-default sink from these two variables.
# Lambdas get them from crucible-research/infrastructure/deploy.sh; the spot
# box gets them through ENV_FILE because the judge run is a separate SSM shell.
# Without them, `default_sink_from_env()` returns None, every judge call is
# unpriced, and AggregateCosts names EvalJudgeProcess for a missing
# `evaljudge-sync` producer (measured watch-rerun-2026-08-28-8/9, 2026-08-30).
KREPIS_COST_SINK_BUCKET = os.environ.get(
    "EVAL_JUDGE_SPOT_COST_SINK_BUCKET", "alpha-engine-research"
)
KREPIS_COST_SINK_PREFIX = os.environ.get(
    "EVAL_JUDGE_SPOT_COST_SINK_PREFIX", "decision_artifacts/_cost_raw"
)

INSTANCE_TAG_NAME = "alpha-engine-eval-judge-spot"


def _bootstrap_spec() -> SpotBootstrapSpec:
    """Everything about this box's provisioning that krepis renders.

    ``crucible-research`` is PUBLIC (it is one of the nine public alpha-engine
    repos), so it is cloned as a plain literal URL with no credential in the
    argv or in ``.git/config``. The one PRIVATE clone this box needs —
    ``alpha-engine-config``, for the judge's gitignored prompts — is done by
    ``eval_judge_spot_bootstrap.sh`` ON THE BOX, reading the PAT from SSM via
    the instance profile, so the credential is never known to this Lambda and
    never appears in the SSM document.

    Both timers are rendered, not one: the dead-man answers "the bootstrap
    died before arming its cap", ``max_runtime_seconds`` answers "the workload
    hung", and the ``ec2-spot-watchdog`` unit answers "the SSM agent died and
    nothing can ever reach this box again". For a box driven ENTIRELY over SSM
    the third is the failure mode that strands the whole stage.
    """
    return SpotBootstrapSpec(
        repo_url=f"https://github.com/{RESEARCH_REPO}.git",
        checkout=RESEARCH_CHECKOUT,
        branch=RESEARCH_BRANCH,
        region=REGION,
        max_runtime_seconds=WATCHDOG_SECONDS,
        deadman_seconds=DEADMAN_SECONDS,
        unit_prefix="eval-judge-spot",
        exports={"XDG_CACHE_HOME": "/home/ec2-user/.cache"},
    )


def _bootstrap_command(run_token: str) -> str:
    """The async SSM RunShellScript body: render the shared bootstrap, write
    the router env file, then exec the repo's own setup script.

    Deliberately minimal, and deliberately NOT the workload. The heavy,
    version-controlled setup lives in
    ``crucible-research/infrastructure/eval_judge_spot_bootstrap.sh`` so a
    change to the box's shape is a PR in that repo rather than a Lambda
    redeploy; the RUN itself is issued separately by the Step Function.

    The env file is written by this launcher rather than by the repo script
    because it is launcher-side configuration — which router edge, which
    consumer credential, which AppConfig profile — and the box must not be
    able to answer those questions for itself.
    """
    log = f"/var/log/eval-judge-spot-bootstrap-{run_token}.log"
    s3_log = (
        f"s3://alpha-engine-research/_ssm_logs/eval-judge-spot/bootstrap/"
        f"$(date -u +%Y-%m-%d)/$(hostname)-$(date -u +%H%M%S)-{run_token}.log"
    )
    prelude = f"""set -uo pipefail
mkdir -p "$(dirname {log})"
exec > >(tee -a {log}) 2>&1
fail() {{ trap - EXIT; echo "[eval-judge-spot-bootstrap] FATAL: $1"; aws s3 cp {log} "{s3_log}" --region {REGION} --quiet || true; shutdown -h now; exit 1; }}
trap 'rc=$?; [ "$rc" -eq 0 ] || fail "bootstrap aborted (rc=$rc)"' EXIT
"""
    # The rendered block runs under `set -e` (the renderer's contract), so the
    # EXIT trap above is what converts an abort inside it into a shipped log
    # and a terminated box instead of an idle one.
    tail = f"""set +e
set -uo pipefail
git config --global --add safe.directory '*' || true
cat > {ENV_FILE} <<'EVAL_JUDGE_ENV'
KREPIS_EXEC_CONTEXT={KREPIS_EXEC_CONTEXT}
KREPIS_LITELLM_PROXY_URL={KREPIS_LITELLM_PROXY_URL}
KREPIS_ROUTER_CREDENTIAL_SECRET={KREPIS_ROUTER_CREDENTIAL_SECRET}
KREPIS_APPCONFIG_APPLICATION={KREPIS_APPCONFIG_APPLICATION}
KREPIS_APPCONFIG_CONFIG_PROFILE={KREPIS_APPCONFIG_CONFIG_PROFILE}
KREPIS_APPCONFIG_ENVIRONMENT={KREPIS_APPCONFIG_ENVIRONMENT}
KREPIS_COST_SINK_BUCKET={KREPIS_COST_SINK_BUCKET}
KREPIS_COST_SINK_PREFIX={KREPIS_COST_SINK_PREFIX}
AWS_REGION={REGION}
AWS_DEFAULT_REGION={REGION}
EVAL_JUDGE_ENV
[ -s {ENV_FILE} ] || fail "router env file was not written"
chown ec2-user:ec2-user {ENV_FILE} || fail "env file chown failed"
export EVAL_JUDGE_SPOT_RUN_TOKEN={run_token}
export BOOTSTRAP_LOG={log}
cd {RESEARCH_CHECKOUT} || fail "crucible-research checkout missing at {RESEARCH_CHECKOUT}"
bash infrastructure/eval_judge_spot_bootstrap.sh || fail "eval_judge_spot_bootstrap.sh failed"
trap - EXIT
aws s3 cp {log} "{s3_log}" --region {REGION} --quiet || true
echo "[eval-judge-spot-bootstrap] complete — judge box ready"
"""
    return prelude + "\n" + render_bootstrap(_bootstrap_spec()) + "\n" + tail


def _launch_instance(
    extra_tags: dict[str, str] | None = None, force_on_demand: bool = False
) -> tuple[str, str]:
    """Launch the judge box; spot first, on-demand fallback on capacity/quota
    exhaustion through the shared ``spot_dispatch`` chokepoint.

    Interruptible-by-default is the right posture here
    (cost-management-policy §2.1): the work is idempotent per artifact and the
    SF re-dispatches once on a substrate loss. The on-demand fallback exists so
    a capacity dip costs money rather than a week of eval coverage — which,
    since a coverage shortfall now FAILS the stage, is the more expensive
    outcome.
    """
    return spot_dispatch.launch_with_fallback(
        INSTANCE_TYPES,
        SUBNETS,
        image_id=AMI_ID,
        key_name=KEY_NAME,
        security_group_ids=[SECURITY_GROUP],
        iam_instance_profile=IAM_PROFILE,
        volume_size_gb=VOLUME_SIZE_GB,
        tag_name=INSTANCE_TAG_NAME,
        region=REGION,
        force_on_demand=force_on_demand,
        # Atomic with RunInstances, never a post-launch create_tags: a box
        # reaped inside the tagging window would be unlookupable either way.
        extra_tags=extra_tags,
    )


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """Step Function handler — launch and bootstrap the eval-judge spot box.

    ``event`` carries ``{"execution_id", "run_date", "force_on_demand"}``.
    Returns ``{"instance_id", "market", "command_id", "run_token"}`` — the SF
    threads ``instance_id`` into ``$.eval_judge_instance_id`` and polls
    ``command_id`` to Success before ``EvalJudgeProcess`` sends the run.

    ``force_on_demand`` is set true by the SF's ONE bounded re-dispatch after a
    spot reclaim: re-entering the pool that just reclaimed the box is how a
    recovery spends its single retry on the same failure
    (alpha-engine-config-I7119's reasoning, same shape).
    """
    event = event or {}
    force_on_demand = bool(event.get("force_on_demand", False))

    if not DISPATCH_ENABLED:
        raise RuntimeError(
            "EVAL_JUDGE_SPOT_DISPATCH_ENABLED=false — the eval judge will not "
            "run this cycle. There is no alternative substrate; re-enable the "
            "dispatcher or run `python -m evals.judge_spot_run` by hand."
        )

    if WATCHDOG_SECONDS <= BOOTSTRAP_TIMEOUT_SECONDS + EVAL_JUDGE_EXECUTION_TIMEOUT_SECONDS:
        # Refuse to launch rather than run a box whose watchdog can pre-empt
        # the judge. A watchdog firing mid-run truncates the corpus, and a
        # coverage shortfall is a HARD stage failure — so this misconfiguration
        # would present as a weekly eval failure with no obvious cause.
        raise ValueError(
            f"EVAL_JUDGE_SPOT_WATCHDOG_SECONDS={WATCHDOG_SECONDS} must exceed "
            f"BOOTSTRAP_TIMEOUT_SECONDS={BOOTSTRAP_TIMEOUT_SECONDS} + "
            f"EVAL_JUDGE_EXECUTION_TIMEOUT_SECONDS="
            f"{EVAL_JUDGE_EXECUTION_TIMEOUT_SECONDS}; otherwise the orphan "
            "watchdog guillotines a healthy judge run and the stage fails on "
            "coverage (alpha-engine-config-I9329)"
        )

    # Per-run identity tags (config#5504): attribute this box's EC2 cost to the
    # SF execution and the cycle. Absent for an operator off-cycle invoke.
    extra_tags: dict[str, str] = {}
    for key, tag_name in (
        ("execution_id", "execution-id"),
        ("run_date", "run-date"),
        ("pipeline_role", "pipeline-role"),
    ):
        val = str(event.get(key, "")).strip()
        if val:
            extra_tags[tag_name] = val

    run_token = uuid.uuid4().hex
    # config#5695: the orphan reaper prefers this per-box deadline over the
    # fleet-wide cap, so the box is never reaped before its own watchdog fires.
    deadline = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
        seconds=WATCHDOG_SECONDS
    )
    extra_tags["watchdog-deadline"] = deadline.strftime("%Y-%m-%dT%H:%M:%S+00:00")

    try:
        instance_id, market = _launch_instance(
            extra_tags=extra_tags, force_on_demand=force_on_demand
        )
    except SpotLaunchError:
        logger.error("eval-judge-spot launch failed (spot + on-demand exhausted)")
        raise
    logger.info("launched eval-judge-spot box %s (%s)", instance_id, market)

    # Between launch and the bootstrap command landing there is no watchdog or
    # trap on the box yet — the timers are armed BY the bootstrap this box has
    # not received. Anything failing in here would orphan it.
    try:
        spot_dispatch.wait_ssm_online(
            instance_id, region=REGION, ssm_online_budget_sec=SSM_ONLINE_BUDGET_SEC
        )
        command_id = spot_dispatch.send_async_command(
            instance_id,
            _bootstrap_command(run_token),
            comment=f"eval-judge-spot bootstrap ({run_token}) — alpha-engine-config-I9329",
            region=REGION,
            cw_log_group=CW_LOG_GROUP,
            execution_timeout_seconds=BOOTSTRAP_TIMEOUT_SECONDS,
        )
    except Exception:
        spot_dispatch.terminate_on_failure(
            instance_id, region=REGION, label="eval-judge-spot"
        )
        raise

    logger.info(
        "eval-judge-spot dispatched: instance=%s market=%s command=%s run_token=%s",
        instance_id, market, command_id, run_token,
    )
    return {
        "instance_id": instance_id,
        "market": market,
        "command_id": command_id,
        "run_token": run_token,
    }
