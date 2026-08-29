"""
validators/phase_marker_sweep.py — Weekly-SF post-run phase-marker sweep
(config#2322).

Background:

  A non-fatal crucible-backtester phase that errors on a REAL (non-smoke)
  weekly run writes a `.phases/{phase}.json` marker with `status: error` —
  but nothing reads those markers on real runs. The fail-loud
  `registry.phase_errors` check (backtest.py ~L4450) that would catch this
  exists ONLY in the smoke/rehearsal path, and it can't be reused directly
  for a cross-process weekly sweep anyway: `registry.phase_errors` is an
  in-process `PhaseRegistry` attribute populated only for phases run in the
  *current* invocation, not a marker reader.

  Live instance (2026-07-11): the config#1405 research-free backfill phase
  failed in 0.01s on its first live exercise. Weekly SF stayed green, zero
  pages fired, and the failure was discovered only by manual post-run S3
  inspection hours later. Per the config#1684 doctrine (experiment/observe
  producers FAIL-HARD), the recording surface for any deliberate fail-soft
  deviation must be ALARMED — a phase marker nothing reads is not a
  recording surface.

  This sweep reads every `backtest/{run_date}/.phases/*.json` marker
  (schema: crucible-backtester `pipeline_common.py`, `status` in
  `{"ok", "error"}`) after the Backtester/PredictorBacktest/
  PortfolioOptimizerBacktest/Evaluator chain and pages on any `status:
  error` found. Report-not-abort: the pipeline still completes — the
  WeeklySubstrateHealthCheck SF state's existing States.ALL Catch already
  routes any non-zero exit from this command to the non-blocking
  SubstrateHealthCheckDegraded path, same as constituents_drift_check.

Usage:

  python -m validators.phase_marker_sweep --run-date 2026-07-18            # checks + alerts on any status=error marker
  python -m validators.phase_marker_sweep --run-date 2026-07-18 --no-alert  # diagnostic, no SNS/Telegram
  python -m validators.phase_marker_sweep --run-date 2026-07-18 --bucket alpha-engine-research

Exit code 0 if no error markers found (or none exist yet), 1 if any
status=error marker is found (alert-worthy), 2 on a sweep-infra failure
(S3 unreachable etc). SF Catch on WeeklySubstrateHealthCheck turns any
non-zero exit into the degraded completion path.

Composes with [[feedback_no_silent_fails]]: a phase marker is ground
truth for what actually happened during the run; do NOT widen
flow-doctor's notify_on_category instead (heuristic exception
classification would miss code/contract defects like the 2026-07-11
FileNotFoundError, which is exactly the failure mode this sweep exists
to catch deterministically).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Optional

import boto3

logger = logging.getLogger(__name__)

_PHASE_PREFIX_TEMPLATE = "backtest/{run_date}/.phases/"

# Truncate the phase list in the alert message so a worst-case
# all-phases-failed run doesn't blow the SNS subject length.
_ALERT_PREVIEW_LIMIT = 10


def _list_phase_markers(bucket: str, run_date: str) -> tuple[list[dict], list[dict]]:
    """Parse the phase markers directly under backtest/{run_date}/.phases/.

    Returns ``(markers, malformed)`` — the well-formed markers, and one
    ``{"s3_key", "reason"}`` entry per object under the marker prefix that
    could not be read as a marker.

    **``Delimiter="/"`` is load-bearing (alpha-engine-config-I9262).** A
    phase marker declares its own outputs in ``artifact_keys``, and the
    backtester writes those artifacts one level DOWN, under
    ``.phases/{phase}/``. Listing the prefix without a delimiter walks that
    whole subtree and hands this function objects that were never markers.
    Measured 2026-08-29 on ``backtest/2026-08-28/.phases/``: 26 markers at
    the top level and 10 nested ``*.json`` artifacts below it, one of which
    (``simulation_setup/dates.json``) is a JSON **array**. ``json.loads``
    accepts an array, so the old ``except (JSONDecodeError, ValueError)``
    guard did not catch it, and ``marker["_s3_key"] = key`` then raised
    ``TypeError: list indices must be integers or slices, not str``. The
    caller's blanket ``except`` turned that code fault into
    ``stage=s3_list``, the shell scored it as a gating failure, and the
    weekly run terminated ``SubstrateHealthCheckDegraded`` having never
    computed a phase verdict at all.

    A non-dict body is therefore reported as a NAMED malformed marker
    rather than crashing the sweep or being silently dropped: a marker this
    sweep cannot read is exactly the condition it exists to surface, and a
    skip would make it indistinguishable from a healthy phase.
    """
    s3 = boto3.client("s3")
    prefix = _PHASE_PREFIX_TEMPLATE.format(run_date=run_date)
    markers: list[dict] = []
    malformed: list[dict] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for obj in page.get("Contents", []) or []:
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            try:
                marker = json.loads(body)
            except (json.JSONDecodeError, ValueError) as exc:
                logger.error("Unparseable phase marker %s: %s", key, exc)
                malformed.append({"s3_key": key, "reason": f"unparseable: {exc}"})
                continue
            if not isinstance(marker, dict):
                logger.error(
                    "Phase marker %s parsed to %s, not an object — cannot be "
                    "read as a marker", key, type(marker).__name__,
                )
                malformed.append({
                    "s3_key": key,
                    "reason": f"parsed to {type(marker).__name__}, expected object",
                })
                continue
            marker["_s3_key"] = key
            markers.append(marker)
    return markers, malformed


def sweep(
    *,
    run_date: str,
    bucket: str = "alpha-engine-research",
    alert: bool = True,
    alert_severity: str = "error",
) -> dict:
    """Sweep backtest/{run_date}/.phases/*.json for status=error markers.

    Args:
        run_date: the SF-stamped run_date whose backtest/{run_date}/.phases/
            prefix to sweep (threaded from $.run_date — see
            tests/test_sf_run_date_threading.py for the stamping contract).
        bucket: S3 bucket holding the backtest phase markers.
        alert: if True, fire an `nousergon_lib.alerts.publish` on any
            status=error marker found. If False, return the findings
            without alerting (diagnostic mode).
        alert_severity: severity tag for the published alert.

    Returns:
        dict with keys: status (`ok` | `phase_errors_detected` |
        `malformed_markers_detected` | `error`), run_date,
        checked_count, error_phases (list of {phase, error, s3_key}),
        malformed_markers (list of {s3_key, reason}).
    """
    try:
        markers, malformed_markers = _list_phase_markers(bucket, run_date)
    except Exception as exc:
        logger.exception("Phase marker list/read failed")
        return {"status": "error", "error": str(exc), "stage": "s3_list"}

    error_phases = [
        {
            "phase": m.get("phase", "?"),
            "error": m.get("error"),
            "s3_key": m["_s3_key"],
        }
        for m in markers
        if m.get("status") == "error"
    ]

    logger.info(
        "Phase marker sweep run_date=%s: checked=%d error=%d malformed=%d",
        run_date, len(markers), len(error_phases), len(malformed_markers),
    )

    if error_phases:
        status = "phase_errors_detected"
    elif malformed_markers:
        # A marker that cannot be read is a finding with a name, not a pass
        # and not a harness fault (alpha-engine-config-I9262).
        status = "malformed_markers_detected"
    else:
        status = "ok"

    result = {
        "status": status,
        "run_date": run_date,
        "checked_count": len(markers),
        "error_phases": error_phases,
        "malformed_markers": malformed_markers,
    }

    if malformed_markers:
        logger.error(
            "MALFORMED PHASE MARKERS: %d object(s) under backtest/%s/.phases/ "
            "could not be read as markers: %s",
            len(malformed_markers), run_date,
            [m["s3_key"] for m in malformed_markers],
        )

    if error_phases and alert:
        try:
            from nousergon_lib import alerts  # noqa: PLC0415
        except ImportError as exc:
            logger.warning(
                "alerts publish skipped — nousergon_lib.alerts unavailable: %s",
                exc,
            )
            return result
        names = sorted(p["phase"] for p in error_phases)
        preview = "; ".join(
            f"{p['phase']} ({p['error']})" for p in error_phases[:_ALERT_PREVIEW_LIMIT]
        )
        suffix = (
            f" ... +{len(error_phases) - _ALERT_PREVIEW_LIMIT} more"
            if len(error_phases) > _ALERT_PREVIEW_LIMIT else ""
        )
        message = (
            f"Weekly-SF phase-marker sweep: {len(error_phases)} backtest "
            f"phase(s) completed with status=error on run_date={run_date}. "
            f"{preview}{suffix}. Weekly SF itself is still green "
            f"(report-not-abort) — these phases failed non-fatally and were "
            f"previously invisible. Investigate "
            f"backtest/{run_date}/.phases/ for full markers."
        )
        try:
            publish_result = alerts.publish(
                message,
                severity=alert_severity,
                source="alpha-engine-data/validators/phase_marker_sweep.py",
                dedup_key=f"phase_marker_sweep_{run_date}_{'_'.join(names)}",
                dedup_window_min=720,  # 12h — one alert per dry-pass window
            )
            logger.info(
                "Phase-marker sweep alert publish: sns_ok=%s telegram_ok=%s any_ok=%s",
                publish_result.sns.ok,
                publish_result.telegram.ok,
                publish_result.any_ok,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Phase-marker sweep alert publish failed: %s", exc)

    return result


def main(argv: Optional[list[str]] = None) -> int:
    # allow_abbrev=False (config-I7415): argparse's default accepts any
    # unambiguous prefix of a long option. A caller passing `--alert` —
    # believing it was turning alerting ON, which is already the default —
    # was silently rebound to `--alert-severity`, which then failed for a
    # missing value and took the whole sweep's exit code with it. The sweep
    # is invoked from a shell script on a box, so the only signal that the
    # flag did not mean what it read as is the run that already failed.
    parser = argparse.ArgumentParser(
        description="Weekly-SF post-run phase-marker sweep (config#2322)",
        allow_abbrev=False,
    )
    parser.add_argument(
        "--run-date", required=True,
        help="backtest run_date to sweep, e.g. 2026-07-18 (threaded from SF $.run_date)",
    )
    parser.add_argument("--bucket", default="alpha-engine-research")
    parser.add_argument("--no-alert", action="store_true",
                        help="diagnostic mode — no SNS/Telegram alert on phase errors")
    parser.add_argument(
        "--alert-severity", default="error",
        choices=["info", "warn", "warning", "error", "critical"],
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    result = sweep(
        run_date=args.run_date,
        bucket=args.bucket,
        alert=not args.no_alert,
        alert_severity=args.alert_severity,
    )

    if result["status"] == "error":
        logger.error("Phase-marker sweep failed at stage=%s: %s",
                     result.get("stage"), result.get("error"))
        return 2

    if result["status"] == "malformed_markers_detected":
        logger.error(
            "MALFORMED PHASE MARKERS: %d object(s) unreadable as markers on "
            "run_date=%s: %s",
            len(result["malformed_markers"]),
            args.run_date,
            [m["s3_key"] for m in result["malformed_markers"]],
        )
        return 1

    if result["status"] == "phase_errors_detected":
        logger.error(
            "PHASE ERRORS DETECTED: %d phase(s) status=error on run_date=%s: %s",
            len(result["error_phases"]),
            args.run_date,
            [p["phase"] for p in result["error_phases"]],
        )
        return 1

    logger.info("Phase-marker sweep OK: no error markers for run_date=%s", args.run_date)
    return 0


if __name__ == "__main__":
    sys.exit(main())
