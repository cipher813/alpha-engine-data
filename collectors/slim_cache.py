"""
slim_cache.py — Write 2-year slices of each price cache parquet to S3.

Extracted from alpha-engine-predictor/training/train_handler.py:write_slim_cache().

The predictor inference Lambda downloads these slim parquets at 6:15 AM PT instead
of fetching 2 years from yfinance — reducing daily yfinance calls from ~450,000
rows to at most a few hundred (the Mon-Fri delta from daily_closes/).
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path

import boto3
import pandas as pd

logger = logging.getLogger(__name__)


def collect(
    bucket: str,
    full_cache_prefix: str = "predictor/price_cache/",
    slim_prefix: str = "predictor/price_cache_slim/",
    lookback_days: int = 730,
    dry_run: bool = False,
) -> dict:
    """
    Download full price cache from S3, write 2-year slices to the slim prefix.

    Args:
        bucket: S3 bucket name
        full_cache_prefix: S3 prefix for full 10y parquets
        slim_prefix: S3 prefix for 2y slim parquets
        lookback_days: calendar days of history to keep (default 730 = 2 years)
        dry_run: if True, count files but don't write

    Returns:
        dict with status, written count, failed count
    """
    s3 = boto3.client("s3")
    cutoff = pd.Timestamp.now().normalize() - pd.Timedelta(days=lookback_days)

    with tempfile.TemporaryDirectory() as tmpdir:
        local_dir = Path(tmpdir)

        # Download full cache parquets
        parquet_keys = _list_parquets(s3, bucket, full_cache_prefix)

        if dry_run:
            logger.info("[dry-run] slim_cache: %d parquets would be sliced", len(parquet_keys))
            return {"status": "ok_dry_run", "count": len(parquet_keys)}

        logger.info(
            "Writing slim cache: %d parquets → s3://%s/%s (cutoff %s)",
            len(parquet_keys), bucket, slim_prefix, cutoff.date(),
        )

        written = 0
        failed = 0

        for s3_key in parquet_keys:
            filename = s3_key.split("/")[-1]
            if filename == "sector_map.json":
                continue

            local_path = local_dir / filename
            try:
                s3.download_file(bucket, s3_key, str(local_path))

                df = pd.read_parquet(local_path)
                df.index = pd.to_datetime(df.index)
                if df.index.tz is not None:
                    df.index = df.index.tz_convert("UTC").tz_localize(None)

                slim_df = df[df.index >= cutoff]
                if slim_df.empty:
                    local_path.unlink(missing_ok=True)
                    continue

                slim_path = local_dir / f"_slim_{filename}"
                slim_df.to_parquet(slim_path, engine="pyarrow", compression="snappy")

                slim_key = f"{slim_prefix}{filename}"
                s3.upload_file(str(slim_path), bucket, slim_key)
                written += 1

                # Cleanup
                slim_path.unlink(missing_ok=True)
                local_path.unlink(missing_ok=True)

            except Exception as e:
                logger.warning("Slim cache write failed for %s: %s", filename, e)
                failed += 1
                local_path.unlink(missing_ok=True)

        if failed > 0:
            fail_pct = failed / max(len(parquet_keys), 1) * 100
            logger.warning("Slim cache: %d/%d failed (%.1f%%)", failed, len(parquet_keys), fail_pct)

        logger.info("Slim cache: %d / %d uploaded to s3://%s/%s", written, len(parquet_keys), bucket, slim_prefix)
        return {"status": "ok" if failed == 0 else "partial", "written": written, "failed": failed}


def _list_parquets(s3, bucket: str, prefix: str) -> list[str]:
    """List all .parquet keys under the given S3 prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])
    return keys
