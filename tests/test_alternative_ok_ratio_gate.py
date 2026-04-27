"""Tests for the per-source ok_ratio gate in ``collectors/alternative.py``.

Background (ROADMAP P1):
``alternative.collect`` aggregates 6 heterogeneous sub-fetchers per ticker
(analyst_consensus, eps_revision, options_flow, insider_activity,
institutional, news). Before this gate, a single source going dark
(Finnhub auth fail, FMP 402, SEC rate-limit) silently nulled out only its
sub-section of the output dict; overall ``_fetch_all_alternative`` still
returned successfully and the manifest landed as ``status="ok"`` with a
silently degraded payload that flowed into research scoring.

The gate tracks per-source "did this ticker get real data?" coverage and
hard-fails the run when any source's populated ratio falls below its
source-specific floor. Heterogeneous floors are the load-bearing design
choice — a flat ratio would false-positive on legitimate sparsity (e.g.
small-caps with no 13F filings, low-vol days with flat options metrics).

These tests lock the gate's contract so a future "simplify to flat ratio"
revert reintroduces the silent-fail surface the gate was built to close.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from collectors import alternative


# ── Sample populated / unpopulated payloads ──────────────────────────────────

def _populated_alt_payload(ticker: str) -> dict:
    """A `_fetch_all_alternative` return where every source has real data."""
    return {
        "ticker": ticker,
        "fetched_at": "2026-04-27T20:00:00+00:00",
        "analyst_consensus": {
            "rating": "Buy",
            "target_price": 200.0,
            "num_analysts": 25,
            "earnings_surprises": [{"date": "2026-Q1", "surprise_pct": 5.2}],
        },
        "eps_revision": {"current_estimate": 6.50, "revision_4w": 1.2, "streak": 2},
        "options_flow": {"put_call_ratio": 0.7, "iv_rank": 35, "expected_move_pct": 4.5},
        "insider_activity": {
            "cluster_buying": True, "net_shares_30d": 50000,
            "transactions": [{"insider": "CEO", "shares": 50000}],
        },
        "institutional": {
            "accumulation": True, "funds_increasing": 7, "funds_decreasing": 2,
        },
        "news": {
            "articles": [{"headline": "X", "source": "Yahoo"}],
            "sec_filings_8k": [{"title": "Item 2.02", "date": "2026-04-25"}],
        },
    }


def _unpopulated_source_payload(ticker: str, dark_sources: list[str]) -> dict:
    """Return a populated payload with `dark_sources` zeroed out (provider went dark)."""
    payload = _populated_alt_payload(ticker)
    defaults = {
        "analyst_consensus": {
            "rating": None, "target_price": None,
            "num_analysts": None, "earnings_surprises": [],
        },
        "eps_revision": {"current_estimate": None, "revision_4w": None, "streak": 0},
        "options_flow": {"put_call_ratio": None, "iv_rank": None, "expected_move_pct": None},
        "insider_activity": {
            "cluster_buying": False, "net_shares_30d": 0, "transactions": [],
        },
        "institutional": {
            "accumulation": False, "funds_increasing": 0, "funds_decreasing": 0,
        },
        "news": {"articles": [], "sec_filings_8k": []},
    }
    for src in dark_sources:
        payload[src] = defaults[src]
    return payload


# ── Helper: stub the collector's IO surface ──────────────────────────────────


def _patch_collect(monkeypatch, *, fetch_returns: list[dict]):
    """Stub `_fetch_all_alternative` to return the prepared per-ticker payloads
    in order, plus mock S3 client + ticker resolver."""
    s3 = MagicMock()
    monkeypatch.setattr(alternative, "boto3", MagicMock(client=lambda *a, **k: s3))
    monkeypatch.setattr(
        alternative, "_load_promoted_tickers",
        lambda *a, **k: [d["ticker"] for d in fetch_returns],
    )
    fetch_iter = iter(fetch_returns)
    monkeypatch.setattr(
        alternative, "_fetch_all_alternative",
        lambda ticker, run_date, bucket: next(fetch_iter),
    )
    return s3


# ── A. _has_data predicates ──────────────────────────────────────────────────


def test_has_analyst_data_truthy_on_any_field():
    assert alternative._has_analyst_data({"rating": "Buy"}) is True
    assert alternative._has_analyst_data({"target_price": 200.0}) is True
    assert alternative._has_analyst_data({"num_analysts": 5}) is True
    assert alternative._has_analyst_data({"earnings_surprises": [{}]}) is True
    assert alternative._has_analyst_data({"rating": None}) is False
    assert alternative._has_analyst_data({}) is False


def test_has_revision_data_only_current_estimate():
    assert alternative._has_revision_data({"current_estimate": 5.0}) is True
    # streak=0 + revision_4w=None alone shouldn't count — that's the
    # default shape returned by _fetch_revisions on a complete miss.
    assert alternative._has_revision_data({"streak": 0, "revision_4w": None}) is False


def test_has_options_data_any_metric():
    assert alternative._has_options_data({"put_call_ratio": 0.5}) is True
    assert alternative._has_options_data({"iv_rank": 25}) is True
    # iv_rank=0 IS real data (low-vol periods produce zero rank); only
    # None (the _fetch_options default) means the source went dark.
    assert alternative._has_options_data({"iv_rank": 0}) is True
    assert alternative._has_options_data({
        "put_call_ratio": None, "iv_rank": None, "expected_move_pct": None,
    }) is False
    assert alternative._has_options_data({}) is False


def test_has_insider_data_filings_or_net_shares():
    assert alternative._has_insider_data({"transactions": [{}]}) is True
    assert alternative._has_insider_data({"net_shares_30d": 100}) is True
    assert alternative._has_insider_data({
        "cluster_buying": False, "net_shares_30d": 0, "transactions": [],
    }) is False


def test_has_institutional_data_increasing_or_decreasing():
    assert alternative._has_institutional_data({"funds_increasing": 1}) is True
    assert alternative._has_institutional_data({"funds_decreasing": 2}) is True
    # accumulation=False alone (without fund counts) doesn't count
    assert alternative._has_institutional_data({
        "accumulation": False, "funds_increasing": 0, "funds_decreasing": 0,
    }) is False


def test_has_news_data_articles_or_8k():
    assert alternative._has_news_data({"articles": [{"headline": "X"}]}) is True
    assert alternative._has_news_data({"sec_filings_8k": [{}]}) is True
    assert alternative._has_news_data({"articles": [], "sec_filings_8k": []}) is False


# ── B. Threshold loader ──────────────────────────────────────────────────────


def test_default_thresholds_are_heterogeneous():
    """The flat-ratio failure mode the ROADMAP warns against would be a
    bug if any future refactor accidentally collapsed all 6 floors to the
    same value. Lock the heterogeneity invariant."""
    ratios = alternative._load_min_ok_ratios()
    distinct = set(ratios.values())
    assert len(distinct) >= 4, (
        f"Per-source thresholds should be heterogeneous (different "
        f"sources have different baseline coverage); got values: {ratios}"
    )
    # Specific anchor values from the design (commentary in alternative.py)
    assert ratios["analyst_consensus"] == 0.80
    assert ratios["insider_activity"] == 0.10  # episodic Form 4 filings
    assert ratios["institutional"] == 0.20  # quarterly 13F lag


def test_env_override_merges(monkeypatch):
    monkeypatch.setenv("ALT_MIN_OK_RATIOS", '{"institutional": 0.05}')
    ratios = alternative._load_min_ok_ratios()
    assert ratios["institutional"] == 0.05
    # Other sources unaffected
    assert ratios["analyst_consensus"] == 0.80


def test_env_override_invalid_json_raises(monkeypatch):
    """Malformed override must hard-fail — silent fallback to defaults
    would leave an operator thinking their tuning landed when it didn't."""
    monkeypatch.setenv("ALT_MIN_OK_RATIOS", "not-json")
    with pytest.raises(RuntimeError, match="not valid JSON"):
        alternative._load_min_ok_ratios()


def test_env_override_unknown_source_raises(monkeypatch):
    monkeypatch.setenv("ALT_MIN_OK_RATIOS", '{"made_up_source": 0.5}')
    with pytest.raises(RuntimeError, match="unknown sources"):
        alternative._load_min_ok_ratios()


def test_env_override_out_of_range_raises(monkeypatch):
    monkeypatch.setenv("ALT_MIN_OK_RATIOS", '{"analyst_consensus": 1.5}')
    with pytest.raises(RuntimeError, match=r"in \[0.0, 1.0\]"):
        alternative._load_min_ok_ratios()


def test_env_override_non_dict_raises(monkeypatch):
    monkeypatch.setenv("ALT_MIN_OK_RATIOS", '[0.5, 0.6]')
    with pytest.raises(RuntimeError, match="must be a JSON object"):
        alternative._load_min_ok_ratios()


# ── C. Gate behavior — happy path ────────────────────────────────────────────


def test_all_sources_above_floor_returns_ok(monkeypatch):
    """All 6 sources fully populated for 10 tickers → status="ok"."""
    payloads = [_populated_alt_payload(f"TKR{i}") for i in range(10)]
    s3 = _patch_collect(monkeypatch, fetch_returns=payloads)

    result = alternative.collect(
        bucket="test-bucket",
        s3_prefix="market_data/",
        run_date="2026-04-27",
        tickers=[f"TKR{i}" for i in range(10)],
    )

    assert result["status"] == "ok"
    assert result["tickers_processed"] == 10
    # All 6 sources at 100% coverage
    for source in alternative._HAS_DATA_PREDICATES:
        assert result["source_ok_ratios"][source] == 1.0


def test_legitimate_sparsity_does_not_breach(monkeypatch):
    """`insider_activity` at 10% floor — even if 9/10 tickers have no
    insider trades (legitimately sparse), the run must pass."""
    payloads = [
        _populated_alt_payload("TKR0"),
        # 9 tickers with no insider activity (legitimate — Form 4 filings
        # are episodic) but other sources populated.
        *(_unpopulated_source_payload(f"TKR{i}", ["insider_activity"])
          for i in range(1, 10)),
    ]
    _patch_collect(monkeypatch, fetch_returns=payloads)

    result = alternative.collect(
        bucket="test-bucket",
        s3_prefix="market_data/",
        run_date="2026-04-27",
        tickers=[f"TKR{i}" for i in range(10)],
    )

    assert result["status"] == "ok", (
        f"10% insider coverage is at the floor; should pass. "
        f"Got: {result.get('error', 'ok')}"
    )
    assert result["source_ok_ratios"]["insider_activity"] == 0.1


# ── D. Gate behavior — breach path ───────────────────────────────────────────


def test_finnhub_outage_breaches_analyst_floor(monkeypatch):
    """0/10 tickers have analyst data (Finnhub recommendation endpoint
    going dark) → status="error", breached_sources lists analyst_consensus."""
    payloads = [
        _unpopulated_source_payload(f"TKR{i}", ["analyst_consensus"])
        for i in range(10)
    ]
    _patch_collect(monkeypatch, fetch_returns=payloads)

    result = alternative.collect(
        bucket="test-bucket",
        s3_prefix="market_data/",
        run_date="2026-04-27",
        tickers=[f"TKR{i}" for i in range(10)],
    )

    assert result["status"] == "error"
    assert "analyst_consensus" in result["breached_sources"]
    assert result["source_ok_ratios"]["analyst_consensus"] == 0.0
    assert "Finnhub" in result["error"]  # remediation hint mentions providers


def test_multiple_simultaneous_breaches_listed_together(monkeypatch):
    """If both Finnhub recs AND FMP estimates die at once, the error
    must name BOTH so the operator knows which providers to investigate."""
    payloads = [
        _unpopulated_source_payload(
            f"TKR{i}", ["analyst_consensus", "eps_revision"]
        )
        for i in range(10)
    ]
    _patch_collect(monkeypatch, fetch_returns=payloads)

    result = alternative.collect(
        bucket="test-bucket",
        s3_prefix="market_data/",
        run_date="2026-04-27",
        tickers=[f"TKR{i}" for i in range(10)],
    )

    assert result["status"] == "error"
    assert set(result["breached_sources"]) == {"analyst_consensus", "eps_revision"}


def test_manifest_written_even_on_breach(monkeypatch):
    """Operators triaging a gate breach need the manifest to identify
    which provider failed. Mirror fundamentals.py pattern: status is the
    decision, but the diagnostic payload always lands."""
    payloads = [
        _unpopulated_source_payload(f"TKR{i}", ["analyst_consensus"])
        for i in range(10)
    ]
    s3 = _patch_collect(monkeypatch, fetch_returns=payloads)

    result = alternative.collect(
        bucket="test-bucket",
        s3_prefix="market_data/",
        run_date="2026-04-27",
        tickers=[f"TKR{i}" for i in range(10)],
    )

    assert result["status"] == "error"
    # find the manifest write among s3.put_object calls
    manifest_calls = [
        c for c in s3.put_object.call_args_list
        if c.kwargs.get("Key", "").endswith("/alternative/manifest.json")
    ]
    assert len(manifest_calls) == 1, "Manifest must be written exactly once"
    body = json.loads(manifest_calls[0].kwargs["Body"])
    assert body["source_ok_counts"]["analyst_consensus"] == 0
    assert body["source_ok_ratios"]["analyst_consensus"] == 0.0


def test_below_floor_just_barely_still_breaches(monkeypatch):
    """analyst_consensus floor is 0.80. 7/10 tickers populated = 0.70 →
    breach. Locks the boundary — < not ≤ as the predicate."""
    payloads = [
        *(_populated_alt_payload(f"TKR{i}") for i in range(7)),
        *(_unpopulated_source_payload(f"TKR{i}", ["analyst_consensus"])
          for i in range(7, 10)),
    ]
    _patch_collect(monkeypatch, fetch_returns=payloads)

    result = alternative.collect(
        bucket="test-bucket",
        s3_prefix="market_data/",
        run_date="2026-04-27",
        tickers=[f"TKR{i}" for i in range(10)],
    )

    assert result["status"] == "error"
    assert result["source_ok_ratios"]["analyst_consensus"] == 0.7
    assert "analyst_consensus" in result["breached_sources"]


def test_at_floor_exactly_does_not_breach(monkeypatch):
    """analyst_consensus floor is 0.80. 8/10 tickers populated = 0.80 → pass.
    Must be `< floor` (not `<=`) so a clean 80% coverage isn't a false alarm."""
    payloads = [
        *(_populated_alt_payload(f"TKR{i}") for i in range(8)),
        *(_unpopulated_source_payload(f"TKR{i}", ["analyst_consensus"])
          for i in range(8, 10)),
    ]
    _patch_collect(monkeypatch, fetch_returns=payloads)

    result = alternative.collect(
        bucket="test-bucket",
        s3_prefix="market_data/",
        run_date="2026-04-27",
        tickers=[f"TKR{i}" for i in range(10)],
    )

    assert result["status"] == "ok"
    assert result["source_ok_ratios"]["analyst_consensus"] == 0.8


# ── E. Override interaction ──────────────────────────────────────────────────


def test_env_override_can_relax_a_specific_breach(monkeypatch):
    """Operator-tunable: if a known edgartools outage drops 13F coverage
    to 5%, the operator can set ALT_MIN_OK_RATIOS to relax that single
    source without breaking the others."""
    payloads = [
        _unpopulated_source_payload(f"TKR{i}", ["institutional"])
        for i in range(10)
    ]
    _patch_collect(monkeypatch, fetch_returns=payloads)

    # Default: institutional floor is 0.20, 0/10 → breach.
    result_default = alternative.collect(
        bucket="test-bucket",
        s3_prefix="market_data/",
        run_date="2026-04-27",
        tickers=[f"TKR{i}" for i in range(10)],
    )
    assert result_default["status"] == "error"
    assert "institutional" in result_default["breached_sources"]

    # Override: relax institutional to 0.0 → passes.
    monkeypatch.setenv("ALT_MIN_OK_RATIOS", '{"institutional": 0.0}')
    fresh_payloads = [
        _unpopulated_source_payload(f"TKR{i}", ["institutional"])
        for i in range(10)
    ]
    _patch_collect(monkeypatch, fetch_returns=fresh_payloads)
    result_relaxed = alternative.collect(
        bucket="test-bucket",
        s3_prefix="market_data/",
        run_date="2026-04-27",
        tickers=[f"TKR{i}" for i in range(10)],
    )
    assert result_relaxed["status"] == "ok"


# ── F. Dry-run still bypasses ────────────────────────────────────────────────


def test_dry_run_does_not_evaluate_gate(monkeypatch):
    """Dry-run short-circuits before any fetch happens — gate logic
    can't fire on data that wasn't fetched."""
    monkeypatch.setattr(
        alternative, "boto3", MagicMock(client=lambda *a, **k: MagicMock()),
    )
    monkeypatch.setattr(
        alternative, "_load_promoted_tickers",
        lambda *a, **k: ["AAPL", "MSFT"],
    )
    result = alternative.collect(
        bucket="test-bucket",
        s3_prefix="market_data/",
        run_date="2026-04-27",
        tickers=["AAPL", "MSFT"],
        dry_run=True,
    )
    assert result["status"] == "ok_dry_run"
    assert "breached_sources" not in result
