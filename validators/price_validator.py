"""
validators/price_validator.py — Data quality checks for price data.

Validates OHLCV data after collection. Non-blocking — logs warnings and
returns anomaly summary for inclusion in manifest and completion emails.
Never blocks data writes.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# Thresholds
MAX_DAILY_RETURN = 0.50       # Flag >50% single-day price move
MAX_VOLUME_SPIKE = 10.0       # Flag volume >10x 20-day rolling median
MAX_GAP_TRADING_DAYS = 3      # Flag gaps >3 trading days in a parquet


def validate_parquet(df: pd.DataFrame, ticker: str) -> dict:
    """
    Validate a single ticker's OHLCV DataFrame.

    Returns dict with anomaly counts and details. Empty anomalies = clean.
    """
    anomalies: list[str] = []

    if df.empty:
        return {"ticker": ticker, "status": "empty", "anomalies": ["empty dataframe"]}

    # ── 1. OHLC relationship ────────────────────────────────────────────────
    if all(c in df.columns for c in ("Open", "High", "Low", "Close")):
        bad_hl = (df["High"] < df["Low"]).sum()
        if bad_hl > 0:
            anomalies.append(f"High<Low on {bad_hl} days")

    # ── 2. Zero or negative prices ──────────────────────────────────────────
    if "Close" in df.columns:
        bad_prices = (df["Close"] <= 0).sum()
        if bad_prices > 0:
            anomalies.append(f"Close<=0 on {bad_prices} days")

    # ── 3. Extreme daily returns ────────────────────────────────────────────
    if "Close" in df.columns and len(df) >= 2:
        returns = df["Close"].pct_change().dropna()
        extreme = returns.abs() > MAX_DAILY_RETURN
        n_extreme = extreme.sum()
        if n_extreme > 0:
            dates = returns[extreme].index.strftime("%Y-%m-%d").tolist()
            anomalies.append(f">{MAX_DAILY_RETURN:.0%} daily move on {n_extreme} days: {dates[:5]}")

    # ── 4. Zero volume on trading days ──────────────────────────────────────
    if "Volume" in df.columns:
        zero_vol = (df["Volume"] == 0).sum()
        if zero_vol > 0:
            anomalies.append(f"zero volume on {zero_vol} days")

    # ── 5. Volume spikes ────────────────────────────────────────────────────
    if "Volume" in df.columns and len(df) >= 25:
        rolling_med = df["Volume"].rolling(20, min_periods=10).median()
        with_baseline = df["Volume"][rolling_med > 0]
        baseline = rolling_med[rolling_med > 0]
        if not baseline.empty:
            ratio = with_baseline / baseline
            spikes = (ratio > MAX_VOLUME_SPIKE).sum()
            if spikes > 0:
                anomalies.append(f"volume >{MAX_VOLUME_SPIKE:.0f}x median on {spikes} days")

    # ── 6. Trading day gaps ─────────────────────────────────────────────────
    if len(df) >= 2:
        idx = pd.to_datetime(df.index).sort_values()
        # Business day diff
        gaps = pd.Series(idx).diff().dt.days.dropna()
        # Exclude weekends (2-day gaps) — flag >5 calendar days (~3 trading days)
        big_gaps = gaps[gaps > 5]
        if not big_gaps.empty:
            gap_details = [
                f"{idx[i-1].strftime('%Y-%m-%d')}→{idx[i].strftime('%Y-%m-%d')} ({int(g)}d)"
                for i, g in big_gaps.items()
            ]
            anomalies.append(f"{len(big_gaps)} gaps >{MAX_GAP_TRADING_DAYS} trading days: {gap_details[:3]}")

    return {
        "ticker": ticker,
        "status": "anomaly" if anomalies else "clean",
        "anomalies": anomalies,
    }


def validate_batch(parquet_dir: Path, tickers: list[str] | None = None) -> dict:
    """
    Validate all parquets in a directory (or a specific subset).

    Returns summary dict suitable for inclusion in manifest.json.
    """
    results = []
    files = sorted(parquet_dir.glob("*.parquet"))

    for f in files:
        ticker = f.stem
        if tickers and ticker not in tickers:
            continue
        try:
            df = pd.read_parquet(f)
            df.index = pd.to_datetime(df.index)
            result = validate_parquet(df, ticker)
            results.append(result)
        except Exception as e:
            results.append({"ticker": ticker, "status": "error", "anomalies": [str(e)]})

    anomaly_tickers = [r for r in results if r["status"] != "clean"]
    total = len(results)

    summary = {
        "total_validated": total,
        "clean": total - len(anomaly_tickers),
        "anomalies": len(anomaly_tickers),
        "anomaly_details": anomaly_tickers[:20],  # Cap for manifest size
    }

    if anomaly_tickers:
        logger.warning(
            "Price validation: %d/%d tickers have anomalies",
            len(anomaly_tickers), total,
        )
        for r in anomaly_tickers[:10]:
            logger.warning("  %s: %s", r["ticker"], "; ".join(r["anomalies"]))
    else:
        logger.info("Price validation: all %d tickers clean", total)

    return summary


def validate_refreshed(
    s3_client,
    bucket: str,
    s3_prefix: str,
    tickers: list[str],
) -> dict:
    """
    Validate freshly refreshed tickers by downloading from S3.

    Only validates the tickers that were just refreshed (not the full cache).
    """
    import tempfile

    results = []
    for ticker in tickers[:100]:  # Cap at 100 to limit S3 calls
        key = f"{s3_prefix}{ticker}.parquet"
        try:
            with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
                s3_client.download_file(bucket, key, tmp.name)
                df = pd.read_parquet(tmp.name)
                df.index = pd.to_datetime(df.index)
                result = validate_parquet(df, ticker)
                results.append(result)
        except Exception as e:
            results.append({"ticker": ticker, "status": "error", "anomalies": [str(e)]})

    anomaly_tickers = [r for r in results if r["status"] != "clean"]

    summary = {
        "total_validated": len(results),
        "clean": len(results) - len(anomaly_tickers),
        "anomalies": len(anomaly_tickers),
        "anomaly_details": anomaly_tickers[:20],
    }

    if anomaly_tickers:
        logger.warning(
            "Post-refresh validation: %d/%d tickers have anomalies",
            len(anomaly_tickers), len(results),
        )
    else:
        logger.info("Post-refresh validation: all %d tickers clean", len(results))

    return summary
