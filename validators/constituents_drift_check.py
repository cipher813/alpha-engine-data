"""
validators/constituents_drift_check.py — Friday-Preflight detection of
Wikipedia-vs-ArcticDB constituents drift (close 5/23-SF P0 (g)).

Background:

  The 2026-05-23 SF FAILED at Research because BNY/P/SN were Wikipedia-
  listed S&P members but missing from ArcticDB universe — the constituents
  collector advanced the `latest_weekly.json` pointer AFTER the backfill
  ran, so the backfill saw last-week's constituents and skipped the new
  cohort. Friday-Preflight SF couldn't detect this directly because the
  Saturday constituents collector hadn't run yet.

  The fix: read membership DIRECTLY from any Friday-Preflight Lambda and
  diff it against the ArcticDB universe, with zero dependency on the
  Saturday collector cadence.

  MEMBERSHIP SOURCE — no longer Wikipedia. Since alpha-engine-config-I2812
  the ground truth is the SSGA SPDR daily holdings files (SPY / MDY), read
  through ``collectors.constituents._fetch_constituents``; Wikipedia is
  retained for GICS sector/sub-industry attestation only. Wikipedia's
  community-edited pages lag real index changes by weeks in the removal
  direction, which was the root cause of I2703/I2812. The docstring and the
  alert text below both said "Wikipedia" long after the source moved —
  corrected 2026-08-21 under alpha-engine-config-I8094, because an alert
  that misnames its own upstream sends every triage session to the wrong
  system first.

  WHAT A HIT ACTUALLY THREATENS. Not ``ResearchPreflight``: that stage
  checks only ``macro.SPY`` freshness and cannot fail on ticker
  completeness. The reachable gate is ``fetch_price_data``'s 5% per-ticker
  error-rate ceiling in crucible-research, so the alert states the observed
  fraction against that ceiling rather than asserting a failure. See the
  comment above the message construction.

Usage:

  python -m validators.constituents_drift_check          # checks + alerts on diff
  python -m validators.constituents_drift_check --no-alert  # diagnostic, no SNS/Telegram
  python -m validators.constituents_drift_check --max-stragglers 20  # allow up to N
                                                                      # Wikipedia tickers
                                                                      # missing from
                                                                      # arctic before
                                                                      # firing alert

Exit code 0 on clean diff (or under-threshold drift), 1 on alert-worthy
drift. SF Catch on the WeeklySubstrateHealthCheck state turns exit-1 into
an alert.

Composes with [[feedback_no_silent_fails]]: Wikipedia is the upstream
authority; if it lists a ticker that ArcticDB universe lacks, that's the
exact failure surface that hit production on 5/23.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Optional

from collectors.constituents import _fetch_constituents
from features.compute import _SKIP_TICKERS, _is_sector_etf

logger = logging.getLogger(__name__)

# Mirrored from crucible-research
# `data/fetchers/price_fetcher.py::_MAX_ERR_RATE`. This validator does not
# import crucible-research (separate repo, no dependency edge), so the value is
# duplicated deliberately and named here so the drift is greppable from both
# sides. It is used ONLY to phrase this alert's consequence clause — nothing
# here gates on it.
_RESEARCH_MAX_ERR_RATE = 0.05


def _open_universe_lib(bucket: str):
    """Open the ArcticDB universe library for read-only symbol listing."""
    from store.arctic_store import get_universe_lib
    return get_universe_lib(bucket)


def check_drift(
    *,
    bucket: str = "alpha-engine-research",
    max_stragglers: int = 0,
    alert: bool = True,
    alert_severity: str = "error",
) -> dict:
    """Run the Wikipedia → ArcticDB constituents drift check.

    Args:
        bucket: S3 bucket holding the ArcticDB universe library.
        max_stragglers: number of Wikipedia tickers allowed to be missing
            from ArcticDB before firing the alert. Default 0 (strict — any
            missing ticker fires). Set higher to tolerate known
            churn-in delay (e.g. the 1-Saturday backfill lag).
        alert: if True, fire an `nousergon_lib.alerts.publish` on drift.
            If False, return the diff without alerting (diagnostic mode).
        alert_severity: severity tag for the published alert.

    Returns:
        dict with keys: status (`ok` | `drift_detected` | `error`),
        wikipedia_count, arctic_count, missing_from_arctic (list),
        only_in_arctic (list), within_threshold (bool).
    """
    try:
        tickers, _sector_map, _sector_etf_map, _sub_industry_map, sp500_count, sp400_count = (
            _fetch_constituents()
        )
    except Exception as exc:
        logger.exception("Wikipedia constituents fetch failed")
        return {
            "status": "error",
            "error": str(exc),
            "stage": "wikipedia_fetch",
        }

    wikipedia_set = set(tickers)
    logger.info(
        "Wikipedia constituents: %d tickers (S&P 500=%d, S&P 400=%d)",
        len(wikipedia_set), sp500_count, sp400_count,
    )

    try:
        lib = _open_universe_lib(bucket)
        arctic_set = set(lib.list_symbols())
    except Exception as exc:
        logger.exception("ArcticDB universe list failed")
        return {
            "status": "error",
            "error": str(exc),
            "stage": "arctic_list",
        }

    # Strip macro/sector members and known-non-stock tickers from the
    # comparison surface — the universe-write set is
    # `wikipedia ∩ ¬_SKIP_TICKERS ∩ ¬sector_etfs` per builders/backfill.py.
    comparable_wiki = {
        t for t in wikipedia_set
        if t not in _SKIP_TICKERS and not _is_sector_etf(t)
    }
    comparable_arctic = {
        t for t in arctic_set
        if t not in _SKIP_TICKERS and not _is_sector_etf(t)
    }

    missing_from_arctic = sorted(comparable_wiki - comparable_arctic)
    only_in_arctic = sorted(comparable_arctic - comparable_wiki)

    logger.info(
        "Drift summary: missing_from_arctic=%d (cap=%d), only_in_arctic=%d "
        "(prune candidates)",
        len(missing_from_arctic), max_stragglers, len(only_in_arctic),
    )

    within_threshold = len(missing_from_arctic) <= max_stragglers

    result = {
        "status": "ok" if within_threshold else "drift_detected",
        "wikipedia_count": len(wikipedia_set),
        "arctic_count": len(arctic_set),
        "missing_from_arctic": missing_from_arctic,
        "only_in_arctic": only_in_arctic,
        "max_stragglers": max_stragglers,
        "within_threshold": within_threshold,
    }

    if not within_threshold and alert:
        try:
            from nousergon_lib import alerts  # noqa: PLC0415
        except ImportError as exc:
            logger.warning(
                "alerts publish skipped — nousergon_lib.alerts unavailable: %s",
                exc,
            )
            return result
        # Truncate the missing list at 20 for the alert message so we don't
        # blow the SNS subject length on a worst-case 50-ticker drift.
        preview = missing_from_arctic[:20]
        suffix = (
            f" ... +{len(missing_from_arctic) - 20} more"
            if len(missing_from_arctic) > 20 else ""
        )
        # The consequence clause names the gate that can ACTUALLY fail on this
        # input, and the margin it has (alpha-engine-config-I8094).
        #
        # It used to say "Saturday SF will likely fail at Research preflight".
        # Both halves were wrong, and the pairing turned routine index
        # reconstitution into an overnight-urgency page:
        #
        #   * `crucible-research/preflight.py::_check_arcticdb_universe` does
        #     not look at ticker completeness at all — it asserts `macro.SPY`
        #     is fresh within 5 trading days. No number of missing
        #     constituents can fail it.
        #   * The gate that CAN fail is
        #     `crucible-research/data/fetchers/price_fetcher.py::fetch_price_data`,
        #     which raises `PriceFetchError` above `_MAX_ERR_RATE = 0.05`. On
        #     2026-08-21 the drift was 2 of 903 comparable tickers — 0.2%
        #     against a 5% ceiling, two orders of magnitude of headroom — and
        #     the alert still told the reader the pipeline was about to fail.
        #
        # So the fraction is computed and stated. A reader deciding whether to
        # act tonight needs the margin, not an adjective.
        err_fraction = len(missing_from_arctic) / max(len(comparable_wiki), 1)
        consequence = (
            f"Blast radius: {len(missing_from_arctic)} of "
            f"{len(comparable_wiki)} comparable tickers = {err_fraction:.2%} "
            f"against the {_RESEARCH_MAX_ERR_RATE:.0%} per-ticker error-rate "
            f"ceiling in crucible-research "
            f"data/fetchers/price_fetcher.py::fetch_price_data "
            f"(_MAX_ERR_RATE). "
        )
        if err_fraction > _RESEARCH_MAX_ERR_RATE:
            consequence += (
                "OVER the ceiling — Research WILL raise PriceFetchError "
                "unless the Saturday backfill covers these first."
            )
        else:
            consequence += (
                "UNDER the ceiling — Research will not hard-fail on this "
                "alone. weekly_collector.py Phase 1 runs constituents -> "
                "historical_constituents -> prices.collect -> "
                "builders.backfill BEFORE ResearchPredictorParallel, and "
                "prices.collect refreshes any ticker with no parquet, so the "
                "scheduled Saturday run is expected to absorb this. A ticker "
                "still missing AFTER that run is the finding worth paging on."
            )
        message = (
            f"Friday-Preflight constituents drift detected: "
            f"{len(missing_from_arctic)} SSGA-listed S&P ticker(s) "
            f"missing from ArcticDB universe "
            f"(threshold={max_stragglers}). "
            f"Missing: {', '.join(preview)}{suffix}. "
            f"{consequence} "
            f"Membership source is the SSGA SPY/MDY daily holdings files "
            f"(alpha-engine-config-I2812); Wikipedia supplies GICS sector "
            f"attestation only. Common benign cause: an official index "
            f"add/rename in the last few sessions. "
            f"Tracker: alpha-engine-config-I8094."
        )
        try:
            publish_result = alerts.publish(
                message,
                severity=alert_severity,
                source="alpha-engine-data/validators/constituents_drift_check.py",
                dedup_key=f"constituents_drift_{len(missing_from_arctic)}",
                dedup_window_min=720,  # 12h — one alert per dry-pass window
            )
            logger.info(
                "Drift alert publish: sns_ok=%s telegram_ok=%s any_ok=%s",
                publish_result.sns.ok,
                publish_result.telegram.ok,
                publish_result.any_ok,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Drift alert publish failed: %s", exc)

    return result


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Friday-Preflight constituents drift check",
    )
    parser.add_argument("--bucket", default="alpha-engine-research")
    parser.add_argument("--max-stragglers", type=int, default=0,
                        help="Wikipedia tickers allowed missing from ArcticDB")
    parser.add_argument("--no-alert", action="store_true",
                        help="diagnostic mode — no SNS/Telegram alert on drift")
    parser.add_argument(
        "--alert-severity", default="error",
        choices=["info", "warn", "warning", "error", "critical"],
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    result = check_drift(
        bucket=args.bucket,
        max_stragglers=args.max_stragglers,
        alert=not args.no_alert,
        alert_severity=args.alert_severity,
    )

    if result["status"] == "error":
        logger.error("Drift check failed at stage=%s: %s",
                     result.get("stage"), result.get("error"))
        return 2

    if result["status"] == "drift_detected":
        logger.error(
            "DRIFT DETECTED: %d Wikipedia tickers missing from ArcticDB "
            "(threshold=%d). Missing: %s",
            len(result["missing_from_arctic"]),
            result["max_stragglers"],
            result["missing_from_arctic"][:20],
        )
        return 1

    logger.info("Drift check OK: arctic covers all Wikipedia-listed tickers")
    return 0


if __name__ == "__main__":
    sys.exit(main())
