"""
weekly_collector.py — Centralized weekly data collection for Alpha Engine.

Phase 1 (before research): constituents, prices, slim cache, macro, universe returns.
Phase 2 (after research): alternative data for promoted tickers.

Phase 1 runs on EC2 via SSM RunCommand (price refresh takes 15-25 min).
Phase 2 runs as Lambda (< 10 min for ~30 tickers).

Usage:
    python weekly_collector.py --phase 1              # Phase 1 only
    python weekly_collector.py --phase 2              # Phase 2 only
    python weekly_collector.py                        # Phase 1 (default)
    python weekly_collector.py --phase 1 --dry-run    # validate Phase 1
    python weekly_collector.py --phase 1 --only prices # single collector
    python weekly_collector.py --phase 2 --only alternative  # explicit
    python weekly_collector.py --daily                # weekday daily closes
    python weekly_collector.py --daily --dry-run      # validate daily
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
import yaml

from ssm_secrets import load_secrets
load_secrets()

from collectors import constituents, prices, slim_cache, macro, universe_returns, alternative, daily_closes, fundamentals

logger = logging.getLogger(__name__)


def load_config(path: str = "config.yaml") -> dict:
    """Load config.yaml with defaults."""
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config not found at {path}. Copy config.yaml.example to config.yaml."
        )
    with open(config_path) as f:
        return yaml.safe_load(f)


def run_weekly(config: dict, args: argparse.Namespace) -> dict:
    """Run collectors based on mode selection."""
    if args.daily:
        return _run_daily(config, args)

    phase = args.phase
    if phase is None:
        phase = 1

    if phase == 1:
        return _run_phase1(config, args)
    elif phase == 2:
        return _run_phase2(config, args)
    else:
        raise ValueError(f"Unknown phase: {phase}")


def _run_phase1(config: dict, args: argparse.Namespace) -> dict:
    """Phase 1: constituents, prices, slim cache, macro, universe returns."""
    bucket = config["bucket"]
    price_cfg = config.get("price_cache", {})
    market_prefix = config.get("market_data", {}).get("s3_prefix", "market_data/")
    ur_cfg = config.get("universe_returns", {})
    run_date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dry_run = args.dry_run
    only = args.only

    results: dict = {
        "phase": 1,
        "date": run_date,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "collectors": {},
    }

    # ── 1. Constituents ──────────────────────────────────────────────────────
    tickers: list[str] = []
    if only in (None, "constituents"):
        logger.info("=" * 60)
        logger.info("COLLECTING: constituents")
        logger.info("=" * 60)
        try:
            const_result = constituents.collect(
                bucket=bucket,
                s3_prefix=market_prefix,
                run_date=run_date,
                dry_run=dry_run,
            )
            results["collectors"]["constituents"] = const_result

            # Load the just-written constituents for downstream use
            if not dry_run:
                s3 = boto3.client("s3")
                key = f"{market_prefix}weekly/{run_date}/constituents.json"
                resp = s3.get_object(Bucket=bucket, Key=key)
                const_data = json.loads(resp["Body"].read())
                tickers = const_data.get("tickers", [])
            else:
                # In dry-run, fetch tickers directly for counting
                t, _, _, _, _ = constituents._fetch_constituents()
                tickers = t
        except Exception as e:
            logger.error("Constituents collection failed: %s", e)
            results["collectors"]["constituents"] = {"status": "error", "error": str(e)}

    # If we didn't collect constituents, load from S3
    if not tickers and only not in ("constituents",):
        try:
            existing = constituents.load_from_s3(bucket, market_prefix)
            if existing:
                tickers = existing.get("tickers", [])
                logger.info("Loaded %d tickers from existing constituents.json", len(tickers))
        except Exception:
            pass

    # ── 2. Price cache refresh ───────────────────────────────────────────────
    if only in (None, "prices"):
        logger.info("=" * 60)
        logger.info("COLLECTING: price cache")
        logger.info("=" * 60)
        if not tickers:
            logger.warning("No tickers available — skipping price cache refresh")
            results["collectors"]["prices"] = {"status": "skipped", "reason": "no tickers"}
        else:
            try:
                price_result = prices.collect(
                    bucket=bucket,
                    tickers=tickers,
                    s3_prefix=price_cfg.get("s3_prefix", "predictor/price_cache/"),
                    fetch_period=price_cfg.get("fetch_period", "10y"),
                    staleness_threshold_days=price_cfg.get("staleness_threshold_days", 3),
                    batch_size=price_cfg.get("refresh_batch_size", 50),
                    dry_run=dry_run,
                )
                results["collectors"]["prices"] = price_result
            except Exception as e:
                logger.error("Price cache refresh failed: %s", e)
                results["collectors"]["prices"] = {"status": "error", "error": str(e)}

    # ── 3. Slim cache ────────────────────────────────────────────────────────
    if only in (None, "slim"):
        logger.info("=" * 60)
        logger.info("COLLECTING: slim cache")
        logger.info("=" * 60)
        try:
            slim_result = slim_cache.collect(
                bucket=bucket,
                full_cache_prefix=price_cfg.get("s3_prefix", "predictor/price_cache/"),
                slim_prefix=price_cfg.get("slim_prefix", "predictor/price_cache_slim/"),
                lookback_days=price_cfg.get("slim_lookback_days", 730),
                dry_run=dry_run,
            )
            results["collectors"]["slim_cache"] = slim_result
        except Exception as e:
            logger.error("Slim cache write failed: %s", e)
            results["collectors"]["slim_cache"] = {"status": "error", "error": str(e)}

    # ── 4. Macro data ────────────────────────────────────────────────────────
    if only in (None, "macro"):
        logger.info("=" * 60)
        logger.info("COLLECTING: macro data")
        logger.info("=" * 60)
        try:
            macro_result = macro.collect(
                bucket=bucket,
                s3_prefix=market_prefix,
                run_date=run_date,
                dry_run=dry_run,
            )
            results["collectors"]["macro"] = macro_result
        except Exception as e:
            logger.error("Macro collection failed: %s", e)
            results["collectors"]["macro"] = {"status": "error", "error": str(e)}

    # ── 5. Universe returns ──────────────────────────────────────────────────
    if only in (None, "universe_returns"):
        logger.info("=" * 60)
        logger.info("COLLECTING: universe returns")
        logger.info("=" * 60)
        db_path = ur_cfg.get("db_path")
        if not db_path:
            # Download research.db from S3 to temp dir
            import tempfile
            tmp_dir = tempfile.mkdtemp(prefix="ae-data-")
            db_path = os.path.join(tmp_dir, "research.db")
            try:
                s3 = boto3.client("s3")
                s3.download_file(bucket, "research.db", db_path)
                logger.info("Downloaded research.db to %s", db_path)
            except Exception as e:
                logger.warning("Could not download research.db: %s", e)
                results["collectors"]["universe_returns"] = {"status": "error", "error": str(e)}
                db_path = None

        if db_path:
            try:
                ur_result = universe_returns.collect(
                    bucket=bucket,
                    db_path=db_path,
                    signals_prefix=ur_cfg.get("signals_prefix", "signals"),
                    sector_map_key=ur_cfg.get(
                        "sector_map_key", "predictor/price_cache/sector_map.json"
                    ),
                    dry_run=dry_run,
                )
                results["collectors"]["universe_returns"] = ur_result
            except Exception as e:
                logger.error("Universe returns collection failed: %s", e)
                results["collectors"]["universe_returns"] = {"status": "error", "error": str(e)}

    # ── 6. Fundamentals ───────────────────────────────────────────────────────
    if only in (None, "fundamentals"):
        logger.info("=" * 60)
        logger.info("COLLECTING: fundamentals (FMP)")
        logger.info("=" * 60)
        if not tickers:
            logger.warning("No tickers available — skipping fundamentals")
            results["collectors"]["fundamentals"] = {"status": "skipped", "reason": "no tickers"}
        else:
            try:
                fund_result = fundamentals.collect(
                    bucket=bucket,
                    tickers=tickers,
                    run_date=run_date,
                    dry_run=dry_run,
                )
                results["collectors"]["fundamentals"] = fund_result
            except Exception as e:
                logger.error("Fundamentals collection failed: %s", e)
                results["collectors"]["fundamentals"] = {"status": "error", "error": str(e)}

    # ── 7. Feature store compute ───────────────────────────────────────────
    if only in (None, "features"):
        logger.info("=" * 60)
        logger.info("COMPUTING: feature store snapshot")
        logger.info("=" * 60)
        try:
            from features.compute import compute_and_write
            fs_result = compute_and_write(
                date_str=run_date,
                bucket=bucket,
                dry_run=dry_run,
            )
            results["collectors"]["features"] = fs_result
        except Exception as e:
            logger.error("Feature store compute failed: %s", e)
            results["collectors"]["features"] = {"status": "error", "error": str(e)}

    # ── 8. ArcticDB universe rebuild ─────────────────────────────────────────
    if only in (None, "arcticdb"):
        logger.info("=" * 60)
        logger.info("REBUILDING: ArcticDB universe (full backfill)")
        logger.info("=" * 60)
        try:
            from builders.backfill import backfill
            arctic_result = backfill(bucket=bucket, dry_run=dry_run)
            results["collectors"]["arcticdb"] = arctic_result
        except Exception as e:
            logger.error("ArcticDB backfill failed: %s", e)
            results["collectors"]["arcticdb"] = {"status": "error", "error": str(e)}

    # ── Finalize ─────────────────────────────────────────────────────────────
    results["completed_at"] = datetime.now(timezone.utc).isoformat()
    _finalize(results, bucket, market_prefix, run_date, dry_run, only)
    return results


def _run_phase2(config: dict, args: argparse.Namespace) -> dict:
    """Phase 2: alternative data for promoted tickers (after research)."""
    bucket = config["bucket"]
    market_prefix = config.get("market_data", {}).get("s3_prefix", "market_data/")
    run_date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dry_run = args.dry_run

    results: dict = {
        "phase": 2,
        "date": run_date,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "collectors": {},
    }

    logger.info("=" * 60)
    logger.info("COLLECTING: alternative data (Phase 2)")
    logger.info("=" * 60)
    try:
        alt_result = alternative.collect(
            bucket=bucket,
            s3_prefix=market_prefix,
            run_date=run_date,
            dry_run=dry_run,
        )
        results["collectors"]["alternative"] = alt_result
    except Exception as e:
        logger.error("Alternative data collection failed: %s", e)
        results["collectors"]["alternative"] = {"status": "error", "error": str(e)}

    results["completed_at"] = datetime.now(timezone.utc).isoformat()
    _finalize(results, bucket, market_prefix, run_date, dry_run, None)
    return results


def _run_daily(config: dict, args: argparse.Namespace) -> dict:
    """Daily mode: capture today's OHLCV closes for all tracked tickers."""
    bucket = config["bucket"]
    run_date = args.date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dry_run = args.dry_run
    daily_cfg = config.get("daily_closes", {})

    results: dict = {
        "mode": "daily",
        "date": run_date,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "collectors": {},
    }

    # Load tickers: S3 constituents → Wikipedia fallback
    tickers: list[str] = []
    market_prefix = config.get("market_data", {}).get("s3_prefix", "market_data/")
    try:
        existing = constituents.load_from_s3(bucket, market_prefix)
        if existing:
            tickers = existing.get("tickers", [])
            logger.info("Loaded %d tickers from S3 constituents", len(tickers))
    except Exception:
        pass
    if not tickers:
        try:
            tickers, _, _, _, _ = constituents._fetch_constituents()
            logger.info("Loaded %d tickers from Wikipedia (S3 fallback)", len(tickers))
        except Exception:
            pass

    if not tickers:
        logger.error("No tickers available for daily closes")
        results["status"] = "failed"
        return results

    logger.info("=" * 60)
    logger.info("COLLECTING: daily closes")
    logger.info("=" * 60)
    try:
        dc_result = daily_closes.collect(
            bucket=bucket,
            tickers=tickers,
            run_date=run_date,
            s3_prefix=daily_cfg.get("s3_prefix", "predictor/daily_closes/"),
            dry_run=dry_run,
        )
        results["collectors"]["daily_closes"] = dc_result
    except Exception as e:
        logger.error("Daily closes collection failed: %s", e)
        results["collectors"]["daily_closes"] = {"status": "error", "error": str(e)}

    # ── Feature store compute ───────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("COMPUTING: feature store snapshot")
    logger.info("=" * 60)
    try:
        from features.compute import compute_and_write
        fs_result = compute_and_write(
            date_str=run_date,
            bucket=bucket,
            dry_run=dry_run,
        )
        results["collectors"]["features"] = fs_result
    except Exception as e:
        logger.error("Feature store compute failed: %s", e)
        results["collectors"]["features"] = {"status": "error", "error": str(e)}

    # ── ArcticDB daily append ────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("APPENDING: ArcticDB universe (daily)")
    logger.info("=" * 60)
    try:
        from builders.daily_append import daily_append
        arctic_result = daily_append(
            date_str=run_date,
            bucket=bucket,
            dry_run=dry_run,
        )
        results["collectors"]["arcticdb"] = arctic_result
    except Exception as e:
        logger.error("ArcticDB daily append failed: %s", e)
        results["collectors"]["arcticdb"] = {"status": "error", "error": str(e)}

    results["completed_at"] = datetime.now(timezone.utc).isoformat()

    # Status
    statuses = [r.get("status", "unknown") for r in results["collectors"].values()]
    if all(s in ("ok", "ok_dry_run") for s in statuses):
        results["status"] = "ok"
    else:
        results["status"] = "failed"

    # Health marker
    if not dry_run and results["status"] == "ok":
        _write_health_marker(bucket, 0, run_date, "ok")

    duration = ""
    try:
        start = datetime.fromisoformat(results["started_at"])
        end = datetime.fromisoformat(results["completed_at"])
        duration = f" in {(end - start).total_seconds():.0f}s"
    except Exception:
        pass
    logger.info("Daily collection %s: %s%s", results["status"].upper(),
                ", ".join(f"{k}={v.get('status', '?')}" for k, v in results["collectors"].items()),
                duration)

    return results


def _finalize(
    results: dict,
    bucket: str,
    market_prefix: str,
    run_date: str,
    dry_run: bool,
    only: str | None,
) -> None:
    """Compute status, write manifest, log summary."""
    statuses = [r.get("status", "unknown") for r in results["collectors"].values()]
    if all(s in ("ok", "ok_dry_run") for s in statuses):
        results["status"] = "ok"
    elif any(s == "error" for s in statuses):
        results["status"] = "partial" if any(s == "ok" for s in statuses) else "failed"
    else:
        results["status"] = "partial"

    if not dry_run and only is None:
        _write_manifest(bucket, market_prefix, run_date, results)
        _write_validation_json(bucket, market_prefix, run_date, results)

    # Write health marker for Step Functions
    phase = results.get("phase")
    if not dry_run and phase and only is None:
        _write_health_marker(bucket, phase, run_date, results["status"])

    duration = ""
    try:
        start = datetime.fromisoformat(results["started_at"])
        end = datetime.fromisoformat(results["completed_at"])
        duration = f" in {(end - start).total_seconds():.0f}s"
    except Exception:
        pass

    phase_label = f"Phase {phase} " if phase else ""
    logger.info(
        "%scollection %s: %s%s",
        phase_label,
        results["status"].upper(),
        ", ".join(f"{k}={v.get('status', '?')}" for k, v in results["collectors"].items()),
        duration,
    )

    # Send completion email (non-blocking)
    if not dry_run and only is None:
        try:
            from emailer import send_step_email
            step_name = f"Data Phase {phase}" if phase else "Data Collection"
            send_step_email(step_name, results, run_date)
        except Exception as exc:
            logger.warning("Step email failed (non-fatal): %s", exc)


def _write_manifest(bucket: str, s3_prefix: str, run_date: str, results: dict) -> None:
    """Write manifest.json and update latest_weekly.json pointer."""
    s3 = boto3.client("s3")

    # Manifest
    manifest_key = f"{s3_prefix}weekly/{run_date}/manifest.json"
    s3.put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(results, indent=2, default=str),
        ContentType="application/json",
    )

    # Latest pointer
    pointer = {"date": run_date, "s3_prefix": f"{s3_prefix}weekly/{run_date}/"}
    s3.put_object(
        Bucket=bucket,
        Key=f"{s3_prefix}latest_weekly.json",
        Body=json.dumps(pointer, indent=2),
        ContentType="application/json",
    )
    logger.info("Wrote manifest + latest pointer for %s", run_date)


def _write_validation_json(
    bucket: str, s3_prefix: str, run_date: str, results: dict,
) -> None:
    """Aggregate validation results from all collectors and write to S3."""
    collectors = results.get("collectors", {})
    validations: dict[str, dict] = {}

    for name, info in collectors.items():
        val = info.get("validation")
        if val:
            validations[name] = val

    if not validations:
        return

    total_validated = sum(v.get("total_validated", 0) for v in validations.values())
    total_anomalies = sum(v.get("anomalies", 0) for v in validations.values())
    total_clean = sum(v.get("clean", 0) for v in validations.values())

    payload = {
        "date": run_date,
        "total_validated": total_validated,
        "total_clean": total_clean,
        "total_anomalies": total_anomalies,
        "collectors": validations,
    }

    s3 = boto3.client("s3")
    key = f"{s3_prefix}weekly/{run_date}/validation.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, indent=2, default=str),
        ContentType="application/json",
    )
    logger.info(
        "Wrote validation.json: %d validated, %d anomalies → s3://%s/%s",
        total_validated, total_anomalies, bucket, key,
    )


def _write_health_marker(bucket: str, phase: int, run_date: str, status: str) -> None:
    """Write health marker for Step Functions dependency checking."""
    s3 = boto3.client("s3")
    key = f"health/data_phase{phase}.json"
    marker = {
        "phase": phase,
        "date": run_date,
        "status": status,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(marker, indent=2),
        ContentType="application/json",
    )
    logger.info("Wrote health marker: s3://%s/%s", bucket, key)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Alpha Engine Weekly Data Collector")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml")
    parser.add_argument("--dry-run", action="store_true", help="Validate without writing to S3")
    parser.add_argument("--date", default=None, help="Override run date (YYYY-MM-DD)")
    parser.add_argument(
        "--daily", action="store_true",
        help="Daily mode: capture today's OHLCV closes for all tickers.",
    )
    parser.add_argument(
        "--phase", type=int, choices=[1, 2], default=None,
        help="Phase 1: pre-research data. Phase 2: post-research alternative data.",
    )
    parser.add_argument(
        "--only",
        choices=["constituents", "prices", "slim", "macro", "universe_returns", "alternative", "daily_closes", "features", "arcticdb"],
        help="Run a single collector instead of all",
    )
    parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args()


def _load_dotenv() -> None:
    """Load .env file into os.environ (lightweight, no dependency)."""
    env_path = Path(".env")
    if not env_path.exists():
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key, val = key.strip(), val.strip()
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ('"', "'"):
                val = val[1:-1]
            if key and val and key not in os.environ:
                os.environ[key] = val


def main() -> None:
    args = _parse_args()
    _load_dotenv()

    from log_config import setup_logging
    setup_logging("data-collector")
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    config = load_config(args.config)
    results = run_weekly(config, args)

    if results["status"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
