"""
store/parquet_loader.py — Shared S3 parquet / slim-cache loading helpers.

Extracted from features/compute.py so that non-feature callers (e.g. the
macro collector's breadth computation) can reuse the same normalized
DataFrame shape without importing private helpers out of features.*.
"""

from __future__ import annotations

import io
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd

log = logging.getLogger(__name__)

SLIM_CACHE_PREFIX = "predictor/price_cache_slim/"


def load_parquet_from_s3(s3, bucket: str, key: str) -> pd.DataFrame:
    """Download a single parquet from S3 and return a normalized DataFrame.

    Normalizes the index to a tz-naive UTC DatetimeIndex (sorted ascending),
    matching the convention used by the predictor and feature store so that
    downstream reindex / join operations never raise on mixed tz.
    """
    obj = s3.get_object(Bucket=bucket, Key=key)
    buf = io.BytesIO(obj["Body"].read())
    df = pd.read_parquet(buf, engine="pyarrow")
    if not isinstance(df.index, pd.DatetimeIndex):
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.set_index("Date")
        elif "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")
        else:
            df.index = pd.to_datetime(df.index)
    if isinstance(df.index, pd.DatetimeIndex) and df.index.tz is not None:
        df.index = df.index.tz_convert("UTC").tz_localize(None)
    if isinstance(df.index, pd.DatetimeIndex) and not df.index.is_monotonic_increasing:
        df = df.sort_index()
    return df


def load_slim_cache(
    s3,
    bucket: str,
    prefix: str = SLIM_CACHE_PREFIX,
    max_workers: int = 20,
) -> dict[str, pd.DataFrame]:
    """Load every parquet under `prefix` into a ticker -> DataFrame dict.

    Returns an empty dict if the prefix is empty. Individual ticker failures
    are logged and skipped; the caller decides how to handle a partial load.
    """
    keys: list[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                keys.append(obj["Key"])

    if not keys:
        log.warning("No parquets found in s3://%s/%s", bucket, prefix)
        return {}

    log.info("Downloading %d slim cache parquets...", len(keys))

    price_data: dict[str, pd.DataFrame] = {}
    errors = 0

    def _download(key: str) -> tuple[str, pd.DataFrame | None]:
        ticker = key.split("/")[-1].replace(".parquet", "")
        try:
            df = load_parquet_from_s3(s3, bucket, key)
            if df.empty:
                return ticker, None
            return ticker, df
        except Exception:
            return ticker, None

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_download, k): k for k in keys}
        for fut in as_completed(futures):
            ticker, df = fut.result()
            if df is not None:
                price_data[ticker] = df
            else:
                errors += 1

    log.info("Slim cache loaded: %d tickers OK, %d errors", len(price_data), errors)
    return price_data
