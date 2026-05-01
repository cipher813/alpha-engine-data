"""Tests for the producer-side universe-freshness scan + receipt emit
in builders/daily_append.py.

The scan is the canonical owner for "every universe symbol got
written today" — replaces the per-Lambda-invocation scans that
previously lived in predictor inference, executor, and backtester
preflights. Hard-fails the daily_append run on any stale symbol
(catches the 2026-04-21 partial-write class). On all-fresh, writes
a receipt JSON to S3 that downstream consumers read in O(1).
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pandas as pd
import pytest

from builders.daily_append import (
    UNIVERSE_FRESHNESS_RECEIPT_KEY,
    UNIVERSE_FRESHNESS_MAX_STALE_DAYS,
    _scan_universe_and_emit_freshness_receipt,
)


def _mock_lib_with_dates(symbol_to_date: dict[str, str]) -> MagicMock:
    """Build a mock ArcticDB library that responds to list_symbols + tail."""
    lib = MagicMock()
    lib.list_symbols.return_value = list(symbol_to_date.keys())

    def _tail(sym, n=1):
        date_str = symbol_to_date[sym]
        if date_str is None:
            df = pd.DataFrame()
        else:
            df = pd.DataFrame({"Close": [100.0]}, index=[pd.Timestamp(date_str)])
        result = MagicMock()
        result.data = df
        return result

    lib.tail.side_effect = _tail
    return lib


def _today_str(offset_days: int = 0) -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=offset_days)).isoformat()


class TestUniverseFreshnessReceipt:
    def test_all_fresh_emits_receipt(self):
        """Happy path: every symbol fresh → receipt is written to the
        canonical S3 key with all_fresh=True and per-symbol metadata."""
        s3 = MagicMock()
        lib = _mock_lib_with_dates({
            "AAPL": _today_str(0),
            "MSFT": _today_str(1),
            "GOOGL": _today_str(2),
        })

        receipt = _scan_universe_and_emit_freshness_receipt(s3, "test-bucket", lib)

        assert receipt["all_fresh"] is True
        assert receipt["n_symbols_checked"] == 3
        assert receipt["stalest_symbol"] == "GOOGL"
        assert receipt["stalest_age_days"] == 2

        s3.put_object.assert_called_once()
        kwargs = s3.put_object.call_args.kwargs
        assert kwargs["Bucket"] == "test-bucket"
        assert kwargs["Key"] == UNIVERSE_FRESHNESS_RECEIPT_KEY
        body = json.loads(kwargs["Body"].decode("utf-8"))
        assert body["all_fresh"] is True
        assert body["library"] == "universe"

    def test_stale_symbol_raises_and_does_not_write(self):
        """Hard-fail mode: if any symbol is older than the threshold,
        raise RuntimeError and do NOT write the receipt — a bad scan
        must not leave a stale artifact that consumers would trust."""
        s3 = MagicMock()
        lib = _mock_lib_with_dates({
            "AAPL": _today_str(0),
            "STALE": _today_str(UNIVERSE_FRESHNESS_MAX_STALE_DAYS + 2),
        })

        with pytest.raises(RuntimeError, match="STALE"):
            _scan_universe_and_emit_freshness_receipt(s3, "test-bucket", lib)

        s3.put_object.assert_not_called()

    def test_empty_library_raises(self):
        """Zero symbols means upstream pipeline never wrote anything;
        consumers must not receive a misleading all_fresh receipt."""
        s3 = MagicMock()
        lib = _mock_lib_with_dates({})

        with pytest.raises(RuntimeError, match="library is empty"):
            _scan_universe_and_emit_freshness_receipt(s3, "test-bucket", lib)

        s3.put_object.assert_not_called()

    def test_read_error_raises(self):
        """A single tail() raise means we can't trust our own scan —
        cannot prove all-fresh, so hard-fail rather than emit the
        receipt with an asterisk."""
        s3 = MagicMock()
        lib = MagicMock()
        lib.list_symbols.return_value = ["AAPL", "BROKEN"]

        def _tail(sym, n=1):
            if sym == "BROKEN":
                raise RuntimeError("simulated arctic read failure")
            result = MagicMock()
            result.data = pd.DataFrame(
                {"Close": [100.0]}, index=[pd.Timestamp(_today_str(0))]
            )
            return result

        lib.tail.side_effect = _tail

        with pytest.raises(RuntimeError, match="failed to read"):
            _scan_universe_and_emit_freshness_receipt(s3, "test-bucket", lib)

        s3.put_object.assert_not_called()

    def test_threshold_boundary_inclusive(self):
        """A symbol exactly at the threshold counts as fresh (≤, not <).
        One day older fails — same boundary as the old preflight gate."""
        s3 = MagicMock()
        lib = _mock_lib_with_dates({
            "EDGE": _today_str(UNIVERSE_FRESHNESS_MAX_STALE_DAYS),
        })
        receipt = _scan_universe_and_emit_freshness_receipt(s3, "test-bucket", lib)
        assert receipt["all_fresh"] is True

        lib2 = _mock_lib_with_dates({
            "EDGE": _today_str(UNIVERSE_FRESHNESS_MAX_STALE_DAYS + 1),
        })
        with pytest.raises(RuntimeError, match="EDGE"):
            _scan_universe_and_emit_freshness_receipt(s3, "test-bucket", lib2)
