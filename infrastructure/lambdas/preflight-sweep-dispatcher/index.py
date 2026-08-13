"""alpha-engine-preflight-sweep-dispatcher — daily trigger for the all-stage
``--preflight-only`` sweep (alpha-engine-config-I7249).

WHY THIS EXISTS
---------------
Every real execution of ``ne-weekly-freshness-pipeline`` since 2026-08-10 has
failed — sixteen consecutively — and each had a DIFFERENT root cause: a
missing ``predictor.yaml``, ``backtest.py`` rc=1, an SSM ``Undeliverable``
from a spot reclaim, DataPhase1 rc=1, rc=137 OOM, rc=75, and ``No module named
'nousergon_lib'`` raised inside the preflight Lambda itself. Every one of them
happened ON THE SPOT BOX, after boot, at first use — the substrate the
``weekly-preflight`` Lambda structurally cannot observe. On 2026-08-10 alone
the operator burned nine reruns without converging; ``sf-pipeline-policy``
§2.5's target is ONE.

The dry instruments already exist (``--preflight-only`` on every stage
launcher, ``$.preflight_args`` threaded through the definition), but their
only whole-pipeline caller is the Friday shell-run — which drives them through
the SF's FAIL-FAST chain and therefore surfaces one root cause per execution.
This dispatcher drives them as a non-short-circuiting SUITE instead, DAILY,
so a broken substrate precondition is found in minutes for one spot-hour
rather than over days of live runs.

CADENCE
-------
Declared in ONE file: ``infrastructure/preflight_sweep_cadence.json``. That
manifest is the single codified parameter (``sf-pipeline-policy`` §2.6); the
EventBridge schedule in the CloudFormation template and the silence deadman's
period are both derived from it, and this Lambda re-reads it at invoke time so
a cadence of ``off`` reports itself as a declared skip rather than as a dead
component. An operator reading that one file can answer "when does the sweep
run next, and what pages if it doesn't?"

MECHANISM — REUSE, NOT A SECOND PROVISIONING PATH
--------------------------------------------------
This Lambda does NOT launch a box of its own. It INVOKES
``alpha-engine-weekly-freshness-spot-dispatcher``, the Lambda that already
builds the weekly pipeline's shared launcher box, and then sends its own SSM
command to the instance that comes back. Three consequences, all deliberate:

  * The sweep runs on a box built by the SAME bootstrap the real weekly run
    uses — same clone set, same venv, same paths, same ``config.yaml``
    symlink. The bootstrap is itself a measured failure source, so it must be
    part of what is swept, not engineered around.
  * Changes to how that box is provisioned are inherited automatically.
    ``nousergon-data-PR1343`` (launch the shared launcher box ON-DEMAND from
    the start, config-I7120) lands in the sweep with no edit here — which is
    exactly what a second, divergent provisioning path would have cost.
  * There is one launcher-box implementation in the fleet, not two
    (``policy-shared-code``).

PRE-SPEND GUARD (``sf-pipeline-policy`` §2.2)
---------------------------------------------
Before spending a cent the handler asserts that the sweep's own code is
present on the data repo's default branch. Absent means it has not been
deployed yet, so booting a box could only produce a false page. That check is
ALSO what makes this PR deployable by the merge button alone: the EventBridge
rule and the alarms ship via CloudFormation on merge, this Lambda is
operator-bootstrapped ahead of the merge, and the MERGE is what flips the
guard from skip to dispatch. No post-merge command exists to forget.

FAIL-LOUD
---------
A launch or SSM failure RAISES, and — critically — first writes the console's
``unmeasured`` artifact. Silence on the console must not be producible by this
Lambda failing: a sweep that never booted a box still leaves a row saying so.
The two non-raising paths (kill-switch, pre-spend guard) return
``dispatched: false`` with a reason and their own metric: a deliberate skip is
not a failure and must not be reported as one.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import urllib.error
import urllib.request

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

REGION = os.environ.get("AWS_REGION", "us-east-1")
COMPONENT_ID = "ae-preflight-sweep"

# Operator kill-switch. Default ON. There is deliberately no downstream
# fail-open branch: flipping this off is an explicit "no sweep tonight", and
# it reports itself as a declared skip with a reason, never as a healthy run.
DISPATCH_ENABLED = (
    os.environ.get("PREFLIGHT_SWEEP_DISPATCH_ENABLED", "true").lower() == "true"
)

BOX_DISPATCHER = os.environ.get(
    "PREFLIGHT_SWEEP_BOX_DISPATCHER", "alpha-engine-weekly-freshness-spot-dispatcher"
)
DATA_REPO = os.environ.get("PREFLIGHT_SWEEP_DATA_REPO", "nousergon/nousergon-data")
DATA_BRANCH = os.environ.get("PREFLIGHT_SWEEP_DATA_BRANCH", "main")
SWEEP_SCRIPT = "infrastructure/preflight_sweep.py"
SWEEP_ENTRYPOINT = "infrastructure/preflight_sweep.sh"

SNS_TOPIC = os.environ.get(
    "PREFLIGHT_SWEEP_SNS_TOPIC",
    "arn:aws:sns:us-east-1:711398986525:alpha-engine-alerts",
)
BUCKET = os.environ.get("PREFLIGHT_SWEEP_BUCKET", "alpha-engine-research")
REPORT_PREFIX = "_preflight_sweep"
CW_LOG_GROUP = os.environ.get(
    "PREFLIGHT_SWEEP_CW_LOG_GROUP", "/alpha-engine/preflight-sweep"
)

# Whole-sweep ceiling on the box. 16 sweepable stages at concurrency 6, each a
# boot + deps + smoke of a few minutes, is comfortably inside an hour; 5400s
# leaves headroom for a slow dnf/pip mirror without letting a hung run hold
# the box indefinitely.
SWEEP_TIMEOUT_SECONDS = int(os.environ.get("PREFLIGHT_SWEEP_TIMEOUT_SECONDS", "5400"))
# The box's own orphan deadline, stamped as a tag the spot-orphan-reaper reads.
# The box dispatcher sizes ITS watchdog for a 12h SF run; this job is ~1h and
# must not hold a box for the difference.
WATCHDOG_SECONDS = int(os.environ.get("PREFLIGHT_SWEEP_WATCHDOG_SECONDS", "7200"))

GITHUB_RAW = "https://raw.githubusercontent.com/{repo}/{branch}/{path}"
GUARD_TIMEOUT_SECONDS = int(os.environ.get("PREFLIGHT_SWEEP_GUARD_TIMEOUT", "10"))
CADENCE_PATH = "infrastructure/preflight_sweep_cadence.json"


class SweepDispatchError(RuntimeError):
    """Dispatch could not be completed. Raised — never swallowed."""


def _emit(metric: str, value: float = 1.0) -> None:
    """Best-effort metric emission.

    Telemetry emission must not break the observed work (``observability-policy``
    §4) — but it must not be silent either. A failure here logs a stable,
    greppable marker which is itself the signal; the dispatch outcome is
    unaffected because the sweep's run report is the durable record, not this
    datapoint.
    """
    try:
        boto3.client("cloudwatch", region_name=REGION).put_metric_data(
            Namespace="AlphaEngine",
            MetricData=[
                {
                    "MetricName": metric,
                    "Dimensions": [{"Name": "Component", "Value": COMPONENT_ID}],
                    "Value": float(value),
                    "Unit": "Count",
                }
            ],
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "PREFLIGHT_SWEEP_TELEMETRY_DEGRADED could not emit %s: %s: %s",
            metric,
            type(exc).__name__,
            exc,
        )


def _now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def write_unmeasured_report(run_id: str, reason: str, dispatched: bool = False) -> None:
    """Leave the console a row even when no box ever ran.

    The console's freshness view is driven by ``_preflight_sweep/latest.json``.
    If this Lambda could fail without writing one, a dispatch outage would
    render as a stale row whose cause the operator has to go find — or worse,
    as the previous run's verdict aging quietly. Writing the row here makes
    silence-by-dispatch-failure unproducible: the surface says "could not
    measure", by name, with the reason.
    """
    payload = {
        "component_id": COMPONENT_ID,
        "run_id": run_id,
        "started_at": _now().isoformat(),
        "finished_at": _now().isoformat(),
        "outcome": "failed",
        "measured": False,
        "unmeasured_reason": reason,
        "dispatched": dispatched,
        "stages_declared": 0,
        "stages_swept": 0,
        "stages_passed": 0,
        "stages_failed": 0,
        "stages_unmeasured": 0,
        "stages_unsweepable": 0,
        "coverage_findings": [],
        "results": [],
    }
    body = json.dumps(payload, indent=2, sort_keys=True).encode()

    # The console's own row for this component. Written HERE, from the
    # dispatcher, precisely because the box never ran: without it the console
    # would show yesterday's verdict quietly ageing toward STALE, and the
    # operator would have to work out from the age alone that today's dispatch
    # died. The row says so by name instead. cadence_minutes comes from the
    # single declaration rather than a constant here — a second copy of the
    # cadence is the drift the declaration exists to prevent.
    cadence_minutes = None
    _cadence_name, _ = read_declared_cadence()
    try:
        url = GITHUB_RAW.format(repo=DATA_REPO, branch=DATA_BRANCH, path=CADENCE_PATH)
        with urllib.request.urlopen(
            urllib.request.Request(url), timeout=GUARD_TIMEOUT_SECONDS
        ) as resp:
            cadence_minutes = json.loads(resp.read().decode()).get("cadence_minutes")
    except Exception as exc:  # noqa: BLE001
        # Without a cadence the console cannot compute staleness — and says so
        # rather than assuming fresh (console-policy §5.2). Recorded, not
        # swallowed; the row is still written, just without the threshold.
        logger.warning(
            "PREFLIGHT_SWEEP_CADENCE_UNREADABLE could not read %s for the console "
            "row: %s: %s — the row is published without cadence_minutes, so the "
            "console renders it unable-to-compute-staleness rather than fresh",
            CADENCE_PATH,
            type(exc).__name__,
            exc,
        )

    console_row = {
        "check_id": COMPONENT_ID,
        "component_id": COMPONENT_ID,
        "status": "error",
        "ran_at": payload["finished_at"],
        "cadence_minutes": cadence_minutes,
        "declared_cadence": _cadence_name,
        "run_id": run_id,
        "outcome": "failed",
        "measured": False,
        "unmeasured_reason": reason,
        "stages_declared": 0,
        "stages_expected": 0,
        "stages_reported": 0,
        "stages_unobserved": 0,
        "summary": (
            f"All-stage preflight sweep COULD NOT MEASURE: {reason}. No stage's "
            "preconditions were checked today. Scope: PRECONDITIONS only — this "
            "surface never speaks to whether a stage's output is correct."
        ),
        "evidence": f"{REPORT_PREFIX}/{run_id}/report.json",
        "schema_version": 1,
    }

    try:
        s3 = boto3.client("s3", region_name=REGION)
        for key in (f"{REPORT_PREFIX}/{run_id}/report.json", f"{REPORT_PREFIX}/latest.json"):
            s3.put_object(
                Bucket=BUCKET, Key=key, Body=body, ContentType="application/json"
            )
        s3.put_object(
            Bucket=BUCKET,
            Key=f"ops/checks/{COMPONENT_ID}/latest.json",
            Body=json.dumps(console_row, indent=2, sort_keys=True).encode(),
            ContentType="application/json",
        )
    except Exception as exc:  # noqa: BLE001
        # The console row could not be written. This is the one degradation
        # that genuinely reduces observability, so it is logged at ERROR with a
        # greppable marker AND the deadman alarm (which watches for the absence
        # of PreflightSweepRunCompleted) still covers the resulting silence.
        logger.error(
            "PREFLIGHT_SWEEP_CONSOLE_ROW_LOST could not write the unmeasured report "
            "for %s: %s: %s — the deadman alarm on PreflightSweepRunCompleted is the "
            "remaining cover",
            run_id,
            type(exc).__name__,
            exc,
        )
        return
    _emit("PreflightSweepRunCompleted")


def read_declared_cadence(
    repo: str = DATA_REPO, branch: str = DATA_BRANCH, opener=urllib.request.urlopen
) -> tuple[str, str]:
    """The declared cadence, read from the single manifest that owns it.

    Returns ``(cadence, reason)``. An unreadable manifest returns ``"unknown"``
    — never a defaulted ``"daily"``. Guessing the cadence is how a component
    ends up running on a schedule nobody declared.
    """
    url = GITHUB_RAW.format(repo=repo, branch=branch, path=CADENCE_PATH)
    try:
        with opener(urllib.request.Request(url), timeout=GUARD_TIMEOUT_SECONDS) as resp:
            manifest = json.loads(resp.read().decode())
    except Exception as exc:  # noqa: BLE001 — reported, never swallowed
        return "unknown", (
            f"could not read {CADENCE_PATH} from {repo}@{branch} "
            f"({type(exc).__name__}: {exc})"
        )
    cadence = manifest.get("sweep_cadence")
    allowed = manifest.get("allowed_values", ["daily", "off"])
    if cadence not in allowed:
        return "unknown", (
            f"{CADENCE_PATH} declares sweep_cadence={cadence!r}, which is not in "
            f"allowed_values {allowed}"
        )
    return cadence, f"declared cadence: {cadence}"


def sweep_code_is_deployed(
    repo: str = DATA_REPO, branch: str = DATA_BRANCH, opener=urllib.request.urlopen
) -> tuple[bool, str]:
    """Is the sweep's own code on the data repo's default branch yet?

    Returns ``(deployed, reason)``. A network failure returns ``False`` with
    the error named: unable to confirm the code is deployed is not the same as
    confirming it is, and spending a spot hour on the guess is the wrong side
    to err on. The next day retries; nothing is lost but one skipped run, and
    the skip is recorded, metered and visible on the console.
    """
    for path in (SWEEP_SCRIPT, SWEEP_ENTRYPOINT):
        url = GITHUB_RAW.format(repo=repo, branch=branch, path=path)
        request = urllib.request.Request(url, method="HEAD")
        try:
            with opener(request, timeout=GUARD_TIMEOUT_SECONDS) as response:
                status = getattr(response, "status", 200)
                if status != 200:
                    return False, f"{path} is not on {repo}@{branch} (HTTP {status})"
        except urllib.error.HTTPError as exc:
            return False, f"{path} is not on {repo}@{branch} (HTTP {exc.code})"
        except Exception as exc:  # noqa: BLE001 — reported, never swallowed
            return False, (
                f"could not confirm {path} is on {repo}@{branch} "
                f"({type(exc).__name__}: {exc})"
            )
    return True, "sweep code present on the default branch"


def _launch_box(run_id: str) -> dict:
    """Invoke the weekly launcher-box dispatcher and return its payload."""
    client = boto3.client("lambda", region_name=REGION)
    response = client.invoke(
        FunctionName=BOX_DISPATCHER,
        InvocationType="RequestResponse",
        Payload=json.dumps(
            {"pipeline_role": "preflight-sweep", "run_id": run_id}
        ).encode(),
    )
    raw = response["Payload"].read()
    if response.get("FunctionError"):
        raise SweepDispatchError(
            f"{BOX_DISPATCHER} returned {response['FunctionError']}: "
            f"{raw.decode(errors='replace')[:800]}"
        )
    payload = json.loads(raw or b"{}")
    if not payload.get("instance_id"):
        raise SweepDispatchError(
            f"{BOX_DISPATCHER} returned no instance_id: {str(payload)[:400]}"
        )
    return payload


def _retag_watchdog_deadline(instance_id: str) -> None:
    """Re-stamp the box's orphan deadline for THIS job's much shorter life.

    The box dispatcher stamps a deadline sized for a 12h SF execution. Leaving
    it would let a sweep box that lost its SSM command sit for half a day.
    Best-effort: the sweep script's own EXIT trap and the systemd watchdog it
    arms are the primary stop mechanisms, so a failure here is logged with a
    greppable marker rather than raised — it narrows a backstop, it is not one.
    """
    deadline = _now() + datetime.timedelta(seconds=WATCHDOG_SECONDS)
    try:
        boto3.client("ec2", region_name=REGION).create_tags(
            Resources=[instance_id],
            Tags=[
                {"Key": "watchdog-deadline", "Value": deadline.isoformat()},
                {"Key": "pipeline_role", "Value": "preflight-sweep"},
            ],
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "PREFLIGHT_SWEEP_TAG_DEGRADED could not tighten watchdog-deadline on %s: "
            "%s: %s — the box's own EXIT trap and systemd watchdog remain primary",
            instance_id,
            type(exc).__name__,
            exc,
        )


def _sweep_command(run_id: str, bootstrap_command_id: str) -> str:
    return f"""set -uo pipefail
export HOME=/home/ec2-user
export AWS_REGION={REGION}
export AWS_DEFAULT_REGION={REGION}
export PREFLIGHT_SWEEP_RUN_ID={run_id}
export PREFLIGHT_SWEEP_BUCKET={BUCKET}
export PREFLIGHT_SWEEP_SNS_TOPIC={SNS_TOPIC}
export PREFLIGHT_SWEEP_WATCHDOG_SECONDS={WATCHDOG_SECONDS}
export BOOTSTRAP_COMMAND_ID={bootstrap_command_id}
# This command is dispatched async and races the bootstrap's clone. Waiting
# for the entrypoint to appear is expected; the entrypoint then waits on the
# bootstrap COMMAND's terminal status and reports a bootstrap failure as a
# run that could not measure. Neither wait swallows anything.
for _ in $(seq 1 150); do
  [ -f /home/ec2-user/alpha-engine-data/{SWEEP_ENTRYPOINT} ] && break
  sleep 10
done
bash /home/ec2-user/alpha-engine-data/{SWEEP_ENTRYPOINT}
"""


def _send_sweep(instance_id: str, run_id: str, bootstrap_command_id: str) -> str:
    ssm = boto3.client("ssm", region_name=REGION)
    result = ssm.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Comment=f"preflight-sweep {run_id} (config-I7249)"[:100],
        Parameters={
            "commands": [_sweep_command(run_id, bootstrap_command_id)],
            "executionTimeout": [str(SWEEP_TIMEOUT_SECONDS)],
        },
        TimeoutSeconds=600,
        CloudWatchOutputConfig={
            "CloudWatchLogGroupName": CW_LOG_GROUP,
            "CloudWatchOutputEnabled": True,
        },
    )
    return result["Command"]["CommandId"]


def _terminate(instance_id: str, why: str) -> None:
    try:
        boto3.client("ec2", region_name=REGION).terminate_instances(
            InstanceIds=[instance_id]
        )
        logger.info("terminated %s after %s", instance_id, why)
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "PREFLIGHT_SWEEP_ORPHAN_RISK could not terminate %s after %s: %s: %s — "
            "the spot-orphan-reaper and the watchdog-deadline tag are the remaining "
            "backstops",
            instance_id,
            why,
            type(exc).__name__,
            exc,
        )


def handler(event: dict, context) -> dict:  # noqa: ARG001 — Lambda contract
    """EventBridge handler — dispatch today's all-stage preflight sweep.

    Returns ``{"dispatched": bool, "reason": str, ...}``. Raises
    ``SweepDispatchError`` on any failure past the pre-spend guards, after
    writing the console's ``unmeasured`` row.
    """
    event = event or {}
    run_id = event.get("run_id") or _now().strftime("preflight-sweep-%Y%m%dT%H%M%SZ")

    if not DISPATCH_ENABLED:
        reason = "PREFLIGHT_SWEEP_DISPATCH_ENABLED=false (operator kill-switch)"
        logger.warning("preflight sweep skipped: %s", reason)
        _emit("PreflightSweepDispatchSkipped")
        return {"dispatched": False, "declared_skip": True, "reason": reason, "run_id": run_id}

    cadence, cadence_reason = read_declared_cadence()
    if cadence == "off":
        # A declared skip. Distinct from a dead component on every surface:
        # its own metric, its own return shape, and no unmeasured row (there is
        # nothing to measure by declaration, which is not the same as failing
        # to measure something that was expected).
        logger.info("preflight sweep not scheduled today: %s", cadence_reason)
        _emit("PreflightSweepDeclaredOff")
        return {
            "dispatched": False,
            "declared_skip": True,
            "reason": cadence_reason,
            "run_id": run_id,
        }

    deployed, guard_reason = sweep_code_is_deployed()
    if not deployed:
        logger.warning("preflight sweep skipped before any spend: %s", guard_reason)
        _emit("PreflightSweepDispatchSkipped")
        return {
            "dispatched": False,
            "declared_skip": True,
            "reason": guard_reason,
            "run_id": run_id,
        }

    try:
        payload = _launch_box(run_id)
    except Exception as exc:  # noqa: BLE001
        write_unmeasured_report(
            run_id,
            f"launcher box could not be provisioned ({type(exc).__name__}: {exc}) — "
            "no stage preflight was exercised",
        )
        raise SweepDispatchError(f"launcher box dispatch failed: {exc}") from exc

    instance_id = payload["instance_id"]
    bootstrap_command_id = payload.get("command_id", "")
    logger.info(
        "launcher box %s (%s) bootstrap=%s for %s",
        instance_id,
        payload.get("market", "unknown"),
        bootstrap_command_id or "<none>",
        run_id,
    )
    _retag_watchdog_deadline(instance_id)

    try:
        command_id = _send_sweep(instance_id, run_id, bootstrap_command_id)
    except Exception as exc:  # noqa: BLE001
        # The box is up but will never receive its work. Terminating it here is
        # the difference between a failed run and a failed run plus an orphaned
        # instance billing until the watchdog fires.
        _terminate(instance_id, "a failed send_command")
        write_unmeasured_report(
            run_id,
            f"launcher box {instance_id} booted but the sweep command could not be "
            f"delivered ({type(exc).__name__}: {exc}) — no stage preflight was "
            "exercised",
        )
        raise SweepDispatchError(
            f"send_command to {instance_id} failed: {type(exc).__name__}: {exc}"
        ) from exc

    _emit("PreflightSweepDispatched")
    return {
        "dispatched": True,
        "declared_skip": False,
        "reason": guard_reason,
        "cadence": cadence,
        "run_id": run_id,
        "instance_id": instance_id,
        "market": payload.get("market"),
        "bootstrap_command_id": bootstrap_command_id,
        "command_id": command_id,
    }
