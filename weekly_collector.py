"""
weekly_collector.py — Centralized weekly data collection for Alpha Engine.

Phase 1 (before research): constituents, prices, slim cache, macro, universe returns.
Phase 2 (after research): alternative data for promoted tickers.

Phase 1 runs on EC2 via SSM RunCommand (price refresh takes 15-25 min).
Phase 2 runs as Lambda (< 10 min for ~30 tickers).

Usage:
    python weekly_collector.py --phase 1              # Phase 1 only
    python weekly_collector.py --phase 2              # Phase 2 only
    python weekly_collector.py                        # both phases (legacy)
    python weekly_collector.py --phase 1 --dry-run    # validate Phase 1
    python weekly_collector.py --phase 1 --only prices # single collector
    python weekly_collector.py --phase 2 --only alternative  # explicit
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

from collectors import constituents, prices, slim_cache, macro, universe_returns, alternative

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
    """Run weekly collectors based on phase selection."""
    phase = args.phase
    if phase is None:
        # Legacy: run Phase 1 only (Phase 2 requires signals.json from research)
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
        "--phase", type=int, choices=[1, 2], default=None,
        help="Phase 1: pre-research data. Phase 2: post-research alternative data.",
    )
    parser.add_argument(
        "--only",
        choices=["constituents", "prices", "slim", "macro", "universe_returns", "alternative"],
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

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )

    config = load_config(args.config)
    results = run_weekly(config, args)

    if results["status"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
