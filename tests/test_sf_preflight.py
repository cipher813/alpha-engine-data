"""Tests for sf_preflight.py — Saturday SF dry-rehearsal.

Each check tested independently with mocked S3 / ArcticDB / polygon /
Wikipedia. Asserts both the happy path and the specific failure mode
each check is designed to catch.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import sf_preflight as sfp


def _ctx(bucket: str = "test-bucket") -> sfp.PreflightContext:
    return sfp.PreflightContext(
        bucket=bucket,
        today="2026-05-02",
        prior_trading_day="2026-05-01",
    )


# ── check_constituents_fetch ──────────────────────────────────────────────────


def test_constituents_fetch_ok_populates_context():
    ctx = _ctx()
    fake_return = (
        ["AAPL"] * 500 + ["MSFT"] * 400,  # tickers (totals: ~903 like prod)
        {**{f"T{i}": "Industrials" for i in range(900)},  # sector_map covers all
         "AAPL": "Information Technology", "MSFT": "Information Technology"},
        {"AAPL": "XLK", "MSFT": "XLK"},  # sector_etf_map
        500,  # sp500_count
        400,  # sp400_count
    )
    # Actually use realistic-shape data: deduped tickers + complete sector_map.
    real_tickers = [f"T{i}" for i in range(900)]
    real_sectors = {t: "Industrials" for t in real_tickers}
    fake_return = (real_tickers, real_sectors, {}, 500, 400)

    with patch("collectors.constituents._fetch_constituents", return_value=fake_return):
        result = sfp.check_constituents_fetch(ctx)
    assert result.status == "ok"
    assert "900 tickers" in result.message
    assert ctx.fresh_constituents == set(real_tickers)


def test_constituents_fetch_fails_on_zero_tickers():
    ctx = _ctx()
    with patch("collectors.constituents._fetch_constituents", return_value=([], {}, {}, 0, 0)):
        result = sfp.check_constituents_fetch(ctx)
    assert result.status == "fail"
    assert "0 tickers" in result.message
    assert ctx.fresh_constituents is None


def test_constituents_fetch_fails_on_unmapped_tickers():
    """Pre-empts the RuntimeError that constituents.collect would raise."""
    ctx = _ctx()
    tickers = [f"T{i}" for i in range(900)]
    # Sector map is missing 50 tickers — collect() would hard-fail at write time.
    sectors = {t: "Industrials" for t in tickers[:850]}
    with patch("collectors.constituents._fetch_constituents",
               return_value=(tickers, sectors, {}, 500, 400)):
        result = sfp.check_constituents_fetch(ctx)
    assert result.status == "fail"
    assert "sector_map missing" in result.message


def test_constituents_fetch_fails_on_sp500_count_drift():
    """If Wikipedia parsing drops the table, sp500_count tanks."""
    ctx = _ctx()
    tickers = [f"T{i}" for i in range(400)]
    with patch(
        "collectors.constituents._fetch_constituents",
        return_value=(tickers, {t: "Industrials" for t in tickers}, {}, 0, 400),
    ):
        result = sfp.check_constituents_fetch(ctx)
    assert result.status == "fail"
    assert "S&P 500 count" in result.message


def test_constituents_fetch_fails_on_wikipedia_exception():
    ctx = _ctx()
    with patch("collectors.constituents._fetch_constituents",
               side_effect=ConnectionError("Wikipedia 503")):
        result = sfp.check_constituents_fetch(ctx)
    assert result.status == "fail"
    assert "Wikipedia 503" in result.message


# ── check_universe_drift (PR #134 class) ──────────────────────────────────────


def _stub_universe_lib_for_drift(stragglers_with_dates: dict[str, str]):
    """ArcticDB stub returning specified last_dates for stragglers."""
    lib = MagicMock()

    def fake_tail(sym, n=1):
        if sym in stragglers_with_dates:
            df = pd.DataFrame({"Close": [100.0]},
                              index=[pd.Timestamp(stragglers_with_dates[sym])])
        else:
            df = pd.DataFrame({"Close": [100.0]},
                              index=[pd.Timestamp("2026-05-01")])  # fresh
        return MagicMock(data=df)

    lib.tail.side_effect = fake_tail
    return lib


def test_universe_drift_predicts_prune_outcome():
    """The 2026-05-02 case: 8 stragglers in arctic, all stale enough to prune."""
    ctx = _ctx()
    ctx.fresh_constituents = {"AAPL", "MSFT"}
    ctx.arctic_universe_symbols = {"AAPL", "MSFT", "ASGN", "GTM", "HOLX",
                                    "KMPR", "LW", "MOH", "MTCH", "PAYC"}

    stale_dates = {
        "ASGN": "2026-04-24", "GTM": "2026-04-24", "HOLX": "2026-04-07",
        "KMPR": "2026-04-24", "LW": "2026-04-24", "MOH": "2026-04-24",
        "MTCH": "2026-04-24", "PAYC": "2026-04-24",
    }
    ctx.universe_lib = _stub_universe_lib_for_drift(stale_dates)

    result = sfp.check_universe_drift(ctx)

    assert result.status == "ok"
    assert result.details["candidates_count"] == 8
    assert result.details["would_prune_count"] == 8


def test_universe_drift_no_stragglers_passes_quietly():
    ctx = _ctx()
    ctx.fresh_constituents = {"AAPL", "MSFT"}
    ctx.arctic_universe_symbols = {"AAPL", "MSFT"}

    result = sfp.check_universe_drift(ctx)
    assert result.status == "ok"
    assert "No straggler candidates" in result.message


def test_universe_drift_skipped_if_context_unpopulated():
    """If constituents fetch failed upstream, this check fails loudly
    instead of misleadingly passing on partial data."""
    ctx = _ctx()
    # ctx.fresh_constituents and ctx.arctic_universe_symbols left None
    result = sfp.check_universe_drift(ctx)
    assert result.status == "fail"


# ── check_polygon_grouped_coverage (PR #131 class) ────────────────────────────


def test_polygon_grouped_coverage_ok_at_full_coverage(monkeypatch):
    monkeypatch.setenv("POLYGON_API_KEY", "stub")
    ctx = _ctx()
    ctx.fresh_constituents = {"AAPL", "MSFT"}
    fake_client = MagicMock()
    fake_client.get_grouped_daily.return_value = {"AAPL": {}, "MSFT": {}, "GOOG": {}}
    with patch("polygon_client.polygon_client", return_value=fake_client):
        result = sfp.check_polygon_grouped_coverage(ctx)
    assert result.status == "ok"
    assert ctx.polygon_returned_tickers == {"AAPL", "MSFT", "GOOG"}


def test_polygon_grouped_coverage_fails_below_95pct(monkeypatch):
    """The exact PR #131 scenario: polygon returns fewer-than-needed tickers."""
    monkeypatch.setenv("POLYGON_API_KEY", "stub")
    ctx = _ctx()
    ctx.fresh_constituents = {f"T{i}" for i in range(100)}
    # polygon returns only 50/100 — 50% coverage, below 95% threshold.
    fake_client = MagicMock()
    fake_client.get_grouped_daily.return_value = {f"T{i}": {} for i in range(50)}
    with patch("polygon_client.polygon_client", return_value=fake_client):
        result = sfp.check_polygon_grouped_coverage(ctx)
    assert result.status == "fail"
    assert "coverage" in result.message.lower()


def test_polygon_grouped_coverage_fails_on_403(monkeypatch):
    monkeypatch.setenv("POLYGON_API_KEY", "stub")
    from polygon_client import PolygonForbiddenError
    ctx = _ctx()
    ctx.fresh_constituents = {"AAPL"}
    fake_client = MagicMock()
    fake_client.get_grouped_daily.side_effect = PolygonForbiddenError("free tier same-day")
    with patch("polygon_client.polygon_client", return_value=fake_client):
        result = sfp.check_polygon_grouped_coverage(ctx)
    assert result.status == "fail"
    assert "403" in result.message


def test_polygon_grouped_coverage_skips_when_no_api_key(monkeypatch):
    """Local-laptop preflight without POLYGON_API_KEY must skip gracefully
    (WARN, not FAIL) so the rest of the report stays actionable."""
    monkeypatch.delenv("POLYGON_API_KEY", raising=False)
    ctx = _ctx()
    ctx.fresh_constituents = {"AAPL"}
    result = sfp.check_polygon_grouped_coverage(ctx)
    assert result.status == "warn"
    assert "POLYGON_API_KEY" in result.message


# ── check_predicted_missing_from_closes (PR #132 class) ───────────────────────


def test_predicted_missing_under_threshold_passes():
    """Post-prune state: only the chronic 4 polygon-coverage tickers missing
    from constituents — under the threshold of 5."""
    ctx = _ctx()
    ctx.fresh_constituents = {"AAPL", "MSFT", "BF-B", "BRK-B", "MOG-A", "PSTG"}
    ctx.arctic_universe_symbols = ctx.fresh_constituents  # post-prune coherent
    ctx.polygon_returned_tickers = {"AAPL", "MSFT"}  # polygon misses the 4 chronic
    result = sfp.check_predicted_missing_from_closes(ctx)
    assert result.status == "ok"


def test_predicted_missing_above_threshold_fails():
    """Pre-prune state (or stragglers missed): would trip the SF hard-fail."""
    ctx = _ctx()
    ctx.fresh_constituents = {f"T{i}" for i in range(20)}
    ctx.arctic_universe_symbols = ctx.fresh_constituents
    ctx.polygon_returned_tickers = {"T0", "T1"}  # 18 missing, threshold is 5
    result = sfp.check_predicted_missing_from_closes(ctx)
    assert result.status == "fail"
    assert "would halt" in result.message.lower()


def test_predicted_missing_excludes_stragglers_correctly():
    """The PR #134 + PR #132 intersection: stragglers in arctic but not in
    fresh constituents must be excluded from the 'expected' set so they
    don't inflate the missing count post-prune."""
    ctx = _ctx()
    ctx.fresh_constituents = {"AAPL", "MSFT"}
    # Arctic still has stragglers (pre-prune state).
    ctx.arctic_universe_symbols = {"AAPL", "MSFT", "STRAGGLER1", "STRAGGLER2"}
    ctx.polygon_returned_tickers = {"AAPL", "MSFT"}
    result = sfp.check_predicted_missing_from_closes(ctx)
    # Post-prune (arctic ∩ constituents) = {AAPL, MSFT}; closes covers both.
    assert result.status == "ok"


# ── check_backfill_source_freshness (PR #130 class) ───────────────────────────


def _bytes_for_parquet(last_date_str: str, has_spy: bool = True) -> bytes:
    import io
    df = pd.DataFrame(
        {"Close": [100.0]},
        index=pd.DatetimeIndex([pd.Timestamp(last_date_str)]),
    )
    if has_spy:
        df.index = pd.Index(["SPY"])  # daily_closes uses ticker as index
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow")
    return buf.getvalue()


def _stub_macro_lib(spy_last_date: str):
    lib = MagicMock()
    lib.tail.return_value = MagicMock(
        data=pd.DataFrame({"Close": [100.0]}, index=[pd.Timestamp(spy_last_date)])
    )
    return lib


def test_backfill_source_freshness_passes_when_delta_covers_arctic():
    """Happy path: ArcticDB SPY at 2026-04-30, daily_closes has 2026-05-01,
    backfill source ≥ arctic → no regression predicted."""
    ctx = _ctx()
    ctx.macro_lib = _stub_macro_lib("2026-04-30")

    import io
    cache_df = pd.DataFrame({"Close": [100.0]},
                            index=[pd.Timestamp("2026-04-30")])
    cache_buf = io.BytesIO()
    cache_df.to_parquet(cache_buf, engine="pyarrow")

    delta_df = pd.DataFrame({"Close": [100.0]}, index=pd.Index(["SPY"]))
    delta_buf = io.BytesIO()
    delta_df.to_parquet(delta_buf, engine="pyarrow")

    fake_s3 = MagicMock()
    def fake_get(**kw):
        body = MagicMock()
        if "price_cache" in kw["Key"]:
            body.read.return_value = cache_buf.getvalue()
        else:
            body.read.return_value = delta_buf.getvalue()
        return {"Body": body}
    fake_s3.get_object.side_effect = fake_get

    with patch("boto3.client", return_value=fake_s3):
        result = sfp.check_backfill_source_freshness(ctx)
    assert result.status == "ok"


def test_backfill_source_freshness_fails_when_source_regresses():
    """The PR #130 scenario: ArcticDB has 5/1 (from MorningEnrich earlier),
    but cache is only 4/30 and no daily_closes delta exists → backfill
    would clobber 5/1 → regression."""
    ctx = _ctx()
    ctx.macro_lib = _stub_macro_lib("2026-05-01")  # arctic ahead

    import io
    cache_df = pd.DataFrame({"Close": [100.0]},
                            index=[pd.Timestamp("2026-04-30")])
    cache_buf = io.BytesIO()
    cache_df.to_parquet(cache_buf, engine="pyarrow")

    fake_s3 = MagicMock()
    def fake_get(**kw):
        if "price_cache" in kw["Key"]:
            body = MagicMock()
            body.read.return_value = cache_buf.getvalue()
            return {"Body": body}
        raise Exception("NoSuchKey")
    fake_s3.get_object.side_effect = fake_get

    with patch("boto3.client", return_value=fake_s3):
        result = sfp.check_backfill_source_freshness(ctx)
    assert result.status == "fail"
    assert "regression" in result.message.lower()


# ── Orchestrator ──────────────────────────────────────────────────────────────


def test_run_preflight_isolates_check_failures():
    """A single check raising must NOT abort the suite — we want the full
    picture. Forces one check to raise; asserts the others still ran."""
    def raising_check(ctx):
        raise RuntimeError("boom")

    raising_check.__name__ = "check_test_raise"

    with patch.object(sfp, "CHECKS", [raising_check, sfp.check_arctic_connectivity]), \
         patch("arcticdb.Arctic", side_effect=Exception("arctic stub")):
        n_fail, results = sfp.run_preflight(bucket="test-bucket")

    assert len(results) == 2  # both ran; first wrapped to fail, second ran
    assert results[0].status == "fail"
    assert "boom" in results[0].message


def test_run_preflight_returns_failure_count():
    def fail_check(ctx):
        return sfp.CheckResult(name="x", status="fail", message="nope")
    fail_check.__name__ = "check_x"

    def ok_check(ctx):
        return sfp.CheckResult(name="y", status="ok", message="fine")
    ok_check.__name__ = "check_y"

    with patch.object(sfp, "CHECKS", [fail_check, ok_check, fail_check]):
        n_fail, results = sfp.run_preflight(bucket="test-bucket")
    assert n_fail == 2
    assert len(results) == 3
