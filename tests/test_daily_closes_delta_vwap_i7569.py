"""alpha-engine-config-I7569: vwap_divergence_pct regression.

Root cause, measured on s3://alpha-engine-research/features/2026-08-14/technical.parquet
(901/901 tickers, literal 0.0 — not NaN): ``_load_delta_from_daily_closes``
built its per-row delta dict from ``staging/daily_closes/{date}.parquet``
without ever reading that parquet's ``VWAP`` column (present per
``collectors/daily_closes.py``'s own documented schema). ``pd.concat`` in
``_apply_daily_delta`` then UNIONED the missing column in as NaN for every
delta-merged (i.e. every "latest") row, every ticker, every day.
``feature_engineer.compute_features``'s ``(Close - VWAP) / VWAP`` produced
NaN for that NaN input, and ``compute_and_write``'s FEATURES-loop fallback
(``row[f] = float(val) if pd.notna(val) else 0.0`` — the same line I7539
already implicated once for residual_momentum_ratio) silently turned that
universe-wide NaN into a universe-wide constant 0.0.
"""
from __future__ import annotations

import io

import pandas as pd
import pytest

import features.compute as compute


class _Body:
    def __init__(self, raw: bytes) -> None:
        self._raw = raw

    def read(self) -> bytes:
        return self._raw


class _FakeS3:
    def __init__(self, day_df: pd.DataFrame) -> None:
        self._day_df = day_df

        class _Exceptions:
            class NoSuchKey(Exception):
                pass

        self.exceptions = _Exceptions()

    def get_object(self, Bucket: str, Key: str):  # noqa: N803
        buf = io.BytesIO()
        self._day_df.to_parquet(buf, engine="pyarrow")
        return {"Body": _Body(buf.getvalue())}


def _daily_closes_frame() -> pd.DataFrame:
    """One trading day, two tickers — mirrors collectors/daily_closes.py's
    documented schema: index=ticker, columns include VWAP."""
    return pd.DataFrame(
        {
            "date": ["2026-08-18", "2026-08-18"],
            "Open": [100.0, 50.0],
            "High": [101.0, 51.0],
            "Low": [99.0, 49.0],
            "Close": [100.5, 50.5],
            "Volume": [1_000_000, 2_000_000],
            "VWAP": [100.3, 50.2],
            "source": ["polygon", "polygon"],
        },
        index=pd.Index(["AAPL", "MSFT"], name="ticker"),
    )


def test_delta_rows_carry_the_real_vwap_value():
    s3 = _FakeS3(_daily_closes_frame())
    rows = compute._load_delta_from_daily_closes(
        s3, "bucket", pd.Timestamp("2026-08-17"), pd.Timestamp("2026-08-18"),
    )
    assert rows["AAPL"][0]["VWAP"] == pytest.approx(100.3)
    assert rows["MSFT"][0]["VWAP"] == pytest.approx(50.2)


def test_delta_rows_preserve_nan_vwap_on_yfinance_fallback_rows():
    """VWAP genuinely absent (yfinance fallback / FRED) must stay NaN, not
    get backfilled — those rows correctly neutral-default downstream."""
    day_df = _daily_closes_frame()
    day_df["VWAP"] = [None, None]
    s3 = _FakeS3(day_df)
    rows = compute._load_delta_from_daily_closes(
        s3, "bucket", pd.Timestamp("2026-08-17"), pd.Timestamp("2026-08-18"),
    )
    assert pd.isna(rows["AAPL"][0]["VWAP"])
    assert pd.isna(rows["MSFT"][0]["VWAP"])


def test_delta_merged_frame_carries_vwap_through_to_feature_input():
    """The regression that matters: a delta-merged latest row must retain a
    real VWAP column so feature_engineer's (Close-VWAP)/VWAP has a real
    (not universally-NaN-then-zeroed) input."""
    s3 = _FakeS3(_daily_closes_frame())
    base = pd.DataFrame(
        {
            "Open": [99.0], "High": [100.0], "Low": [98.0],
            "Close": [99.5], "Volume": [900_000], "VWAP": [99.4],
        },
        index=pd.DatetimeIndex(["2026-08-17"]),
    )
    price_data = {"AAPL": base}
    out, _split_tickers = compute._apply_daily_delta(
        s3=s3, bucket="bucket", date_str="2026-08-18",
        price_data=price_data, registry=None,
    )
    latest = out["AAPL"].iloc[-1]
    assert pd.notna(latest["VWAP"])
    assert latest["VWAP"] == pytest.approx(100.3)
    assert latest["Close"] != latest["VWAP"]  # real divergence, not a same-value artifact
