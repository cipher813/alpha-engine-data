"""
store/arctic_store.py — ArcticDB connection manager.

Thin wrapper over ArcticDB that provides library access for all modules.
Uses S3 backend — no additional infrastructure beyond the existing bucket.

Usage:
    from store.arctic_store import get_universe_lib, get_macro_lib

    universe = get_universe_lib()
    df = universe.read("AAPL").data

Libraries:
    universe — per-ticker time series (OHLCV + 53 computed features)
    macro    — market-wide time series (VIX, yields, commodities, macro features)
"""

from __future__ import annotations

import logging
import os

import arcticdb as adb

log = logging.getLogger(__name__)

DEFAULT_BUCKET = "alpha-engine-research"
ARCTIC_PREFIX = "arcticdb"

_arctic_instance: adb.Arctic | None = None


def _get_arctic(bucket: str | None = None) -> adb.Arctic:
    """Get or create the ArcticDB connection singleton."""
    global _arctic_instance
    if _arctic_instance is not None:
        return _arctic_instance

    bucket = bucket or os.environ.get("ARCTIC_BUCKET", DEFAULT_BUCKET)
    region = os.environ.get("AWS_REGION", "us-east-1")
    uri = f"s3s://s3.{region}.amazonaws.com:{bucket}?path_prefix={ARCTIC_PREFIX}&aws_auth=true"

    log.info("Connecting to ArcticDB: s3://%s/%s (region=%s)", bucket, ARCTIC_PREFIX, region)
    _arctic_instance = adb.Arctic(uri)
    return _arctic_instance


def get_universe_lib(bucket: str | None = None) -> adb.library.Library:
    """Get the universe library (per-ticker OHLCV + features)."""
    arctic = _get_arctic(bucket)
    return arctic.get_library("universe", create_if_missing=True)


def get_macro_lib(bucket: str | None = None) -> adb.library.Library:
    """Get the macro library (market-wide time series)."""
    arctic = _get_arctic(bucket)
    return arctic.get_library("macro", create_if_missing=True)


def reset_connection():
    """Reset the singleton (useful for testing or credential rotation)."""
    global _arctic_instance
    _arctic_instance = None
