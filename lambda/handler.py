"""
Lambda entry point — Phase 2 alternative data collector.

Triggered by Step Functions after research produces signals.json.
Fetches alternative data (analyst, revisions, options, insider, institutional,
news) for promoted tickers (~25-30) and writes per-ticker JSON to S3.

Pass {"force": true} to bypass date checks (manual testing).
Pass {"dry_run": true} to validate without writing to S3.
"""

from __future__ import annotations

import logging
import os
import sys
import time
import traceback

# Load secrets from SSM Parameter Store (must run before any os.environ.get)
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from ssm_secrets import load_secrets
load_secrets()

logger = logging.getLogger(__name__)


def handler(event, context):
    """
    AWS Lambda handler for Phase 2 alternative data collection.

    Event payload:
        phase: int (must be 2)
        date: str (optional, YYYY-MM-DD override)
        force: bool (bypass checks)
        dry_run: bool (validate without writing)

    Returns:
        dict with status: "OK" | "SKIPPED" | "ERROR"
    """
    os.environ.setdefault("XDG_CACHE_HOME", "/tmp")

    force = event.get("force", False)
    dry_run = event.get("dry_run", False)
    run_date = event.get("date")

    _start = time.time()

    try:
        # Validate required env vars
        missing = []
        if not os.environ.get("FMP_API_KEY"):
            missing.append("FMP_API_KEY")
        # EDGAR_IDENTITY and POLYGON_API_KEY are optional (graceful degradation)

        if missing:
            msg = f"Missing required env vars: {', '.join(missing)}"
            logger.error(msg)
            return {"status": "ERROR", "error": msg}

        # Import collector (deferred to reduce cold-start time)
        import yaml
        from collectors import alternative

        # Load config
        config_path = os.environ.get("CONFIG_PATH", "config.yaml")
        if os.path.exists(config_path):
            with open(config_path) as f:
                config = yaml.safe_load(f)
        else:
            config = {"bucket": "alpha-engine-research", "market_data": {"s3_prefix": "market_data/"}}

        bucket = config.get("bucket", "alpha-engine-research")
        market_prefix = config.get("market_data", {}).get("s3_prefix", "market_data/")

        # Run Phase 2
        result = alternative.collect(
            bucket=bucket,
            s3_prefix=market_prefix,
            run_date=run_date,
            dry_run=dry_run,
        )

        status = result.get("status", "error")
        duration = time.time() - _start

        # Write health marker
        if not dry_run and status in ("ok", "partial"):
            try:
                import json
                from datetime import datetime, timezone
                import boto3
                s3 = boto3.client("s3")
                s3.put_object(
                    Bucket=bucket,
                    Key="health/data_phase2.json",
                    Body=json.dumps({
                        "phase": 2,
                        "date": run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                        "status": status,
                        "duration_seconds": round(duration, 1),
                        "tickers_processed": result.get("tickers_processed", 0),
                        "completed_at": datetime.now(timezone.utc).isoformat(),
                    }, indent=2),
                    ContentType="application/json",
                )
            except Exception as he:
                logger.warning("Health marker write failed: %s", he)

        if status in ("ok", "partial", "ok_dry_run"):
            logger.info(
                "Phase 2 complete in %.0fs: %s", duration,
                f"{result.get('tickers_processed', 0)} tickers processed"
            )
            return {
                "status": "OK",
                "tickers_processed": result.get("tickers_processed", 0),
                "tickers_failed": result.get("tickers_failed", 0),
                "duration_seconds": round(duration, 1),
                "dry_run": dry_run,
            }
        else:
            return {
                "status": "ERROR",
                "error": result.get("reason") or "collection failed",
                "duration_seconds": round(duration, 1),
            }

    except Exception as e:
        tb = traceback.format_exc()
        logger.error("Phase 2 failed: %s\n%s", e, tb)
        return {
            "status": "ERROR",
            "error": str(e),
            "duration_seconds": round(time.time() - _start, 1),
        }
