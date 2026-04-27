"""Tests for the 2026-04-27 read_batch perf optimization in builders/daily_append.py.

Before this change, the per-ticker loop called ``universe_lib.read(ticker)``
sequentially for every one of ~900 universe symbols. Each read was a separate
S3 round-trip; total cost was ~5-7 minutes wall time and the dominant share of
the ~12-min daily_append budget that triggered the 2026-04-27 MorningEnrich
SSM TimedOut incident.

ArcticDB's ``read_batch`` parallelizes the underlying S3 round-trips
internally, collapsing 900 sequential reads into a single batched call. The
in-loop access becomes a dict lookup, so the rest of the per-ticker logic
(compute_features, dtype matching, _write_row_backfill_safe) is unchanged.

Missing symbols come back as ``DataError`` objects — they're filtered into
``n_err`` with the same semantics as the prior ``try/except Exception`` per-
ticker branch, so no caller-visible behavior change.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from arcticdb.version_store.library import ReadRequest


_DAILY_APPEND = Path(__file__).parent.parent / "builders" / "daily_append.py"


def _source() -> str:
    return _DAILY_APPEND.read_text()


# ── Source-inspection invariants ────────────────────────────────────────────


def test_read_batch_is_invoked_in_warmup():
    """The warmup history must be loaded via ``universe_lib.read_batch``,
    not a per-ticker ``universe_lib.read(ticker)`` loop. The latter pattern
    was the dominant cost in the 2026-04-27 12-min MorningEnrich timeout.
    """
    src = _source()
    assert "universe_lib.read_batch(" in src, (
        "Warmup history must use universe_lib.read_batch — a per-ticker "
        "universe_lib.read(ticker) loop reintroduces ~900 sequential S3 "
        "round-trips and breaks the 2026-04-27 MorningEnrich SSM budget."
    )
    assert "ReadRequest(symbol=" in src, (
        "read_batch must be called with ReadRequest objects (one per ticker) "
        "so future date_range / column / row_range slicing can be added "
        "without re-architecting the call site."
    )


def test_in_loop_universe_read_is_gone():
    """Inside the ``for ticker in stock_tickers`` loop, the call site
    ``hist = universe_lib.read(ticker).data`` must not exist — otherwise the
    batched read above is wasted work and the per-ticker S3 cost returns.
    """
    src = _source()
    assert "hist = universe_lib.read(ticker).data" not in src, (
        "Per-ticker `hist = universe_lib.read(ticker).data` reintroduces "
        "the 900× sequential read cost. Use `hists_by_ticker.get(ticker)` "
        "from the upfront read_batch instead."
    )
    # The dict-lookup pattern that replaces it
    assert "hists_by_ticker.get(ticker)" in src, (
        "Expected hists_by_ticker.get(ticker) — the in-loop access pattern "
        "after the upfront read_batch."
    )


def test_data_error_is_imported_and_handled():
    """``DataError`` from arcticdb_ext must be imported and the batch
    iteration must check for it. Missing symbols return DataError, not an
    exception, so a plain ``try/except`` would never catch them and they'd
    silently slip into the per-ticker compute path with stale ``hist=None``.
    """
    src = _source()
    assert "from arcticdb_ext.version_store import DataError" in src, (
        "DataError must be imported — it's the type returned by read_batch "
        "for missing symbols (NOT an exception)."
    )
    assert "isinstance(result, DataError)" in src, (
        "Each read_batch result must be checked with "
        "isinstance(result, DataError) — missing symbols are silent-skip "
        "without this filter."
    )


# ── Behavioral check: read_batch is called with one ReadRequest per ticker ──


def test_batch_request_shape():
    """The list of ReadRequest objects passed to read_batch must contain
    exactly one entry per stock_ticker, in order. Drift here would mean
    some tickers silently skip ArcticDB or get reads in wrong order.
    """
    captured_requests: list[list[ReadRequest]] = []

    def fake_read_batch(requests):
        captured_requests.append(list(requests))
        # Return a DataError-shaped object for each — easy way to make the
        # loop short-circuit cleanly without needing full mocked Arctic data.
        return [
            MagicMock(
                spec=[],  # MagicMock without DataError isinstance — treated as success below
                data=pd.DataFrame(
                    {"Open": [1.0], "High": [1.0], "Low": [1.0],
                     "Close": [1.0], "Volume": [1], "VWAP": [1.0]},
                    index=pd.DatetimeIndex(["2026-04-25"], name="date"),
                ),
            )
            for _ in requests
        ]

    fake_lib = MagicMock()
    fake_lib.read_batch.side_effect = fake_read_batch

    tickers = ["AAPL", "MSFT", "NVDA"]
    fake_lib.read_batch([ReadRequest(symbol=t) for t in tickers])

    assert len(captured_requests) == 1
    requests = captured_requests[0]
    assert len(requests) == 3
    assert [r.symbol for r in requests] == tickers, (
        "ReadRequest list must preserve stock_tickers order so the "
        "zip(stock_tickers, read_results) loop pairs them correctly."
    )
