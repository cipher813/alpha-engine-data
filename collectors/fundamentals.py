"""
collectors/fundamentals.py — Finnhub TTM fundamentals collection.

Fetches P/E, P/B, D/E, revenue growth, FCF yield, gross margin, ROE,
current ratio for all universe tickers from Finnhub's
``/stock/metric?symbol=X&metric=all`` endpoint.

Runs weekly in DataPhase1. Cached to S3 at archive/fundamentals/{date}.json.
Daily pipeline reads the cached file (fundamentals are quarterly — don't
change within a week).

Migration history
-----------------
- v1: FMP v3 (sunset 2025-08-31).
- v2: FMP /stable (multi-endpoint: key-metrics-ttm + ratios-ttm + income-statement).
- v3 (this file, 2026-04-24): Finnhub /stock/metric?metric=all — single
  endpoint replaces the three FMP calls; FMP /stable moved to paid tier
  on key-metrics-ttm (HTTP 402 observed 2026-04-24 Sat SF run).

Endpoint contract
-----------------
Single Finnhub call per ticker::

    /stock/metric?symbol=AAPL&metric=all

Response shape::

    {
      "metric": {
        "peTTM": ..., "pbAnnual": ..., "totalDebt/totalEquityAnnual": ...,
        "revenueGrowthTTMYoy": ..., "freeCashFlowTTM": ...,
        "marketCapitalization": ..., "grossMarginTTM": ...,
        "roeTTM": ..., "currentRatioAnnual": ...,
        ...
      },
      "metricType": "all",
      "symbol": "AAPL"
    }

FCF yield isn't directly exposed; computed as ``freeCashFlowTTM /
marketCapitalization``. Other fields map 1-to-1 to Finnhub names with
TTM-preferred / annual-fallback semantics for fields where TTM may be
missing for newer listings.

Rate limiting
-------------
Finnhub free tier is 60 req/min. The shared client in
``collectors.finnhub_client`` enforces a 1.1s minimum interval between
calls (~54/min). 903 universe tickers × 1 call each = ~17 min total —
well within DataPhase1's 30-min budget.

Failure semantics
-----------------
Per-ticker errors are logged at WARNING and fall through to NEUTRAL
values, but the collector hard-fails (``status="error"``) if fewer than
``_MIN_OK_RATIO`` of tickers produced real (non-NEUTRAL) data — catches
silent zero outputs (matches the short_interest collector's guard,
matches the original FMP version's no-silent-fails behavior).
"""

from __future__ import annotations

import json
import logging
import os
import time

from nousergon_lib.secrets import get_secret

from validators.price_validator import (
    ALL_FEATURE_ANOMALY_TYPES,
    DEFAULT_FEATURE_BLOCK_ANOMALY_TYPES,
    validate_feature_record,
)

from .finnhub_client import finnhub_get

logger = logging.getLogger(__name__)

# ── Write-time value-range gate (ROADMAP L1243, extends #215) ──────────────
# fundamentals.py writes a feature-source snapshot to S3 that bypasses
# builders/daily_append.py's validate_today_row gate entirely. A single
# corrupt field (NaN from a divide-by-near-zero FCF computation, or a
# negative gross_margin from a malformed Finnhub payload) silently poisons
# the predictor feature store + research scoring with no pipeline failure —
# the exact FMP-zero'd-fundamentals class that already burned ~2 weeks of
# alpha. Field specs declare the value-range invariant per output field.
# Clipping (_clip) already bounds the *range*, so the load-bearing residual
# this gate catches is NaN/inf (clip of NaN propagates NaN) + the
# structural non-negativity of margin/ratio fields. lo/hi mirror the clip
# bands so a gross outlier surfaces if a future refactor drops a _clip.
_FUNDAMENTALS_FIELD_SPECS: dict[str, dict] = {
    "pe_ratio":           {"lo": -3.0, "hi": 3.0},
    "pb_ratio":           {"lo": -3.0, "hi": 3.0},
    "debt_to_equity":     {"lo": -3.0, "hi": 3.0},
    "revenue_growth_yoy": {"lo": -1.0, "hi": 2.0},
    "fcf_yield":          {"lo": -0.5, "hi": 0.5},
    "gross_margin":       {"nonneg": True, "lo": 0.0, "hi": 1.0},
    "roe":                {"lo": -1.0, "hi": 1.0},
    "current_ratio":      {"nonneg": True, "lo": 0.0, "hi": 3.0},
}


# ── Cross-sectional collapse gate (alpha-engine-config-I7583) ────────────────
#
# The per-ticker `ok_ratio` gate below asks "did THIS TICKER return anything at
# all". It cannot see a field that is dead for EVERY ticker: a ticker returning
# 13 of 14 fields counts as fully populated, so a field absent from the vendor
# response for the whole universe passes it 903 times over.
#
# That is not hypothetical. `capitalSpendingGrowth5Y` and `freeCashFlowTTM` do
# not exist in Finnhub's `metric=all` response at all; `_pick`'s `default=0.0`
# turned both into a universe-wide 0.0, and this collector reported `ok` every
# run for as long as the Finnhub integration has been live. The same shape hit
# a second way when the percent-point units were wrong: every value exceeded
# its clip bound, so `gross_margin` and `roe` collapsed onto the ceiling for
# ~90% of the universe. Both were found by hand, off a question in a chat
# (alpha-engine-config-I7569); nothing in this file would have raised, and
# nothing would raise for the next one.
#
# This gate is deliberately written over the OUTPUT rather than over the vendor
# keys, so it catches the class regardless of cause — an absent key defaulting
# to a constant, a units mismatch saturating a clip bound, a scaling divisor
# that collapses a range, or a vendor silently switching a field to a fixed
# value. Any cause that ends in "this column carries no cross-sectional
# information" is caught by the same predicate.
#
# It also sits where `_FUNDAMENTALS_FIELD_SPECS` cannot: that gate runs on
# values `_clip` has ALREADY bounded to its own bands, so it is structurally
# incapable of firing on saturation. This one runs on the assembled universe.
#
# FAIL is set at 0.99 rather than 1.00 so a single stray ticker cannot mask a
# dead field. WARN is the observe band: it does not halt, because a legitimately
# concentrated field (a payout ratio where most of the universe pays nothing) is
# plausible, and per alpha-engine-config-I7581 a new gate must not go straight
# to enforcing on a range whose real distribution has not been measured over the
# full universe. Promote the warn band to fail once a full post-I7569 run has
# been observed — tracked on I7583.
_FIELD_COLLAPSE_FAIL_SHARE = 0.99
_FIELD_COLLAPSE_WARN_SHARE = 0.70

# Below this many fetched tickers the gate does not apply. "Every value is the
# same" is unremarkable across three tickers and says nothing about whether a
# field carries cross-sectional information — the statement this gate makes is
# only meaningful over a real universe. 50 is well under the ~900 production
# universe and well over any fixture or smoke run, so a narrowed operator run
# is not falsely failed and a production run is never exempted. Recorded rather
# than silently skipped: a run below the floor logs that the gate did not
# apply, because "the check did not fire" and "the check was not run" must not
# look the same on the log (principles.md §2.7).
_FIELD_COLLAPSE_MIN_TICKERS = 50

# Fields exempt from the gate, each with the reason it may legitimately
# collapse. Deliberately by NAME, never by a heuristic: a heuristic that
# tolerates "few distinct values" would also tolerate the defect.
_COLLAPSE_EXEMPT: frozenset[str] = frozenset()


def _field_collapse_report(records: list[dict]) -> dict[str, tuple[float, object]]:
    """{field: (modal_share, modal_value)} for every field, over ``records``.

    Callers pass only records that actually came back from the vendor —
    NEUTRAL rows are fetch failures and are already accounted for by
    ``ok_ratio``; including them would let a bad fetch day manufacture a
    collapse that is really an outage.
    """
    from collections import Counter

    if not records:
        return {}
    fields = sorted({k for r in records for k in r})
    out: dict[str, tuple[float, object]] = {}
    for field in fields:
        if field in _COLLAPSE_EXEMPT:
            continue
        counts = Counter(r.get(field) for r in records)
        value, n = counts.most_common(1)[0]
        out[field] = (n / len(records), value)
    return out


def _load_fundamentals_block_anomaly_types() -> frozenset[str]:
    """Read ``FUNDAMENTALS_BLOCK_ANOMALY_TYPES`` env var or fall back.

    Format + validation mirror ``daily_append._load_block_anomaly_types``:
    a JSON list of feature-anomaly type strings; unknown types raise (a
    silent typo would let corrupt rows through — NoSilentFails). Empty /
    unset uses the conservative default (NaN/inf + negative-where-nonneg
    block; gross_outlier warns).
    """
    raw = os.environ.get("FUNDAMENTALS_BLOCK_ANOMALY_TYPES", "").strip()
    if not raw:
        return DEFAULT_FEATURE_BLOCK_ANOMALY_TYPES
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"FUNDAMENTALS_BLOCK_ANOMALY_TYPES is not valid JSON: {exc}. "
            f"Expected a JSON list of feature-anomaly type strings."
        ) from exc
    if not isinstance(parsed, list) or not all(isinstance(x, str) for x in parsed):
        raise RuntimeError(
            f"FUNDAMENTALS_BLOCK_ANOMALY_TYPES must be a JSON list of strings, "
            f"got {parsed!r}"
        )
    unknown = set(parsed) - ALL_FEATURE_ANOMALY_TYPES
    if unknown:
        raise RuntimeError(
            f"FUNDAMENTALS_BLOCK_ANOMALY_TYPES contains unknown anomaly "
            f"types: {sorted(unknown)}. Known types: "
            f"{sorted(ALL_FEATURE_ANOMALY_TYPES)}"
        )
    return frozenset(parsed)


def _emit_quality_gate_metrics(
    counts_by_type: dict[str, int], n_blocked: int, n_warned: int
) -> None:
    """Emit ``AlphaEngine/Data/fundamentals_quality_*`` gauges.

    Best-effort: CloudWatch errors WARN but don't fail the collector — the
    aggregated run-level quality-gate logger.error is the load-bearing
    Flow Doctor surface; the metric catches slow drift. Mirrors
    ``builders.daily_append._emit_quality_gate_metrics``.
    """
    if not counts_by_type and n_blocked == 0 and n_warned == 0:
        return
    try:
        import boto3

        cw = boto3.client("cloudwatch")
        metric_data: list[dict] = [
            {
                "MetricName": "fundamentals_quality_blocked_count",
                "Value": float(n_blocked),
                "Unit": "Count",
            },
            {
                "MetricName": "fundamentals_quality_warned_count",
                "Value": float(n_warned),
                "Unit": "Count",
            },
        ]
        for atype, count in counts_by_type.items():
            metric_data.append({
                "MetricName": "fundamentals_quality_anomaly_count",
                "Dimensions": [{"Name": "anomaly_type", "Value": atype}],
                "Value": float(count),
                "Unit": "Count",
            })
        cw.put_metric_data(Namespace="AlphaEngine/Data", MetricData=metric_data)
    except Exception as exc:
        logger.warning(
            "CloudWatch fundamentals_quality_* metric failed: %s. Not "
            "blocking — the aggregated run-level quality-gate logger.error "
            "is the load-bearing Flow Doctor surface.",
            exc,
        )

# Minimum fraction of requested tickers that must produce real fundamentals
# (at least one non-zero field) for the run to be considered OK. Below
# this threshold the endpoint is probably broken (auth, quota, schema
# change) — don't let a silently-zeroed output flow into the predictor
# feature store and research scoring.
_MIN_OK_RATIO = 0.90


def _safe_float(val, default: float = 0.0) -> float:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _clip(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))


def _pick(metrics: dict, *keys: str, default: float = 0.0) -> float:
    """Return the first key with a non-None value, as a float.

    Finnhub exposes most fields with both TTM and Annual variants;
    callers list TTM first and fall through to Annual / Quarterly when
    a field isn't populated for the given ticker (newer listings, ADRs,
    etc.). The same pattern in the legacy FMP collector accepted both
    ``returnOnEquityTTM`` and ``roeTTM`` for forward compatibility —
    Finnhub's schema is similar but with different naming.
    """
    for key in keys:
        if key in metrics and metrics[key] is not None:
            return _safe_float(metrics[key], default=default)
    return default


# Neutral values for tickers where Finnhub returns nothing usable
#
# Phase 3a of attractiveness-pillars-260520 (2026-05-20): added 5 new
# fundamental fields backing the Growth + Stewardship pillar quant
# subscores. All Finnhub ``/stock/metric?metric=all`` derived — no new
# API integrations. The composites that consume these fields are added
# in alpha-engine-research/scoring/factor_scoring.py Phase 3b.
NEUTRAL = {
    "pe_ratio": 0.0,
    "pb_ratio": 0.0,
    "debt_to_equity": 0.0,
    "revenue_growth_yoy": 0.0,
    "fcf_yield": 0.0,
    "gross_margin": 0.0,
    "roe": 0.0,
    "current_ratio": 0.0,
    # Growth pillar substrate (Phase 3a) — 3y CAGR signals (smoother than
    # TTM YoY; less noise from base-effect / single-quarter anomalies)
    "revenue_growth_3y": 0.0,
    "eps_growth_3y": 0.0,
    # Stewardship pillar substrate (Phase 3a) — payout discipline +
    # reinvestment intensity. Insider-ownership not surfaced here
    # (Finnhub doesn't expose it via metric=all; deferred to a separate
    # PR if/when it becomes load-bearing).
    "payout_ratio": 0.0,
    "dividend_yield": 0.0,
    "capex_growth_5y": 0.0,
    # SIZE pillar substrate (config#1142) — raw market cap (absolute units).
    # Surfaced from the already-fetched ``marketCapitalization`` metric;
    # the feature engineer takes log() and the cross-sectional pass emits
    # the Barra SIZE loading (size_zscore). 0.0 here -> size NaN downstream
    # (log guard), so a missing-cap ticker is excluded rather than mis-sized.
    "market_cap_raw": 0.0,
}


# ── Source-key declaration + absent-source gate (alpha-engine-config-I7583) ──
#
# The vendor keys each output field reads, in fallback order. SINGLE OWNER:
# _fetch_single_ticker picks through this map instead of repeating the key
# lists inline, so the absence check cannot drift from what is actually read.
#
# Why this exists, and why it is separate from the collapse gate below:
# `_pick` returns 0.0 for a key absent from the response, and nothing
# distinguished that from a genuine zero. `capitalSpendingGrowth5Y` and
# `freeCashFlowTTM` do not exist in Finnhub's `metric=all` response AT ALL, so
# both read as a universe-wide 0.0 for the life of the integration with this
# collector reporting `ok` every run. They were found by hand off a question in
# a chat (alpha-engine-config-I7569), not by any check.
#
# The collapse gate catches that class by its SYMPTOM (a field carrying one
# value across the universe). This gate catches it at the CAUSE, before any
# default has been applied, which buys two things the symptom check cannot:
# the error names the missing vendor keys instead of the constant they
# produced, and it still fires when an absent field's default happens to VARY.
_FIELD_SOURCE_KEYS: dict[str, tuple[str, ...]] = {
    "pe_ratio": ("peTTM", "peExclExtraTTM", "peNormalizedAnnual"),
    "pb_ratio": ("pbAnnual", "pbQuarterly"),
    "debt_to_equity": ("totalDebt/totalEquityAnnual", "totalDebt/totalEquityQuarterly"),
    "revenue_growth_yoy": (
        "revenueGrowthTTMYoy", "revenueGrowthQuarterlyYoy", "revenueGrowth5Y",
    ),
    "gross_margin": ("grossMarginTTM", "grossMarginAnnual", "grossMargin5Y"),
    "roe": ("roeTTM", "roeRfy"),
    "current_ratio": ("currentRatioAnnual", "currentRatioQuarterly"),
    "revenue_growth_3y": ("revenueGrowth3Y", "revenueGrowth5Y"),
    "eps_growth_3y": ("epsGrowth3Y", "epsBasicExclExtraItemsAnnual5Y", "epsGrowth5Y"),
    "payout_ratio": ("payoutRatioTTM", "payoutRatioAnnual"),
    "dividend_yield": ("dividendYieldIndicatedAnnual", "currentDividendYieldTTM"),
    "capex_growth_5y": ("capexCagr5Y",),
    # fcf_yield is DERIVED (1 / P-FCF-per-share), not read directly; these are
    # the keys whose absence makes it underivable.
    "fcf_yield": ("pfcfShareTTM", "pfcfShareAnnual"),
    "market_cap_raw": ("marketCapitalization",),
}

# A field absent for MORE than this fraction of the fetched universe is not
# per-ticker sparsity (newer listings, ADRs, funds without the metric) — it is
# the vendor not exposing the key. Set well above realistic sparsity and below
# 1.0 so it fires before the field is fully dead rather than only at the extreme.
_SOURCE_ABSENCE_FAIL_SHARE = 0.95

# Transport-only key on the per-ticker record. Popped in collect() before the
# snapshot is assembled — never written to S3, never seen by a consumer.
_ABSENT_KEY = "__absent_source_fields__"


def _absent_source_fields(metrics: dict) -> set[str]:
    """Output fields for which NONE of the declared vendor keys is present.

    A property of the RESPONSE, read before any default is applied — the one
    point where "the vendor did not send this" and "the value is zero" are
    still distinguishable.
    """
    return {
        field for field, keys in _FIELD_SOURCE_KEYS.items()
        if not any(k in metrics and metrics[k] is not None for k in keys)
    }


def _fetch_single_ticker(ticker: str) -> dict:
    """Fetch and normalize fundamental data for a single ticker via Finnhub.

    One round-trip replaces the three-endpoint FMP version. Unrecognized
    or missing tickers (delisted, ADRs without coverage, etc.) return
    NEUTRAL — same shape as before so downstream consumers don't change.
    """
    payload = finnhub_get("stock/metric", {"symbol": ticker, "metric": "all"})
    if not isinstance(payload, dict):
        return NEUTRAL.copy()

    metrics = payload.get("metric") or {}
    if not isinstance(metrics, dict) or not metrics:
        return NEUTRAL.copy()

    # Recorded BEFORE any default is applied (alpha-engine-config-I7583).
    _absent = _absent_source_fields(metrics)

    # P/E: TTM preferred; peExclExtraTTM smooths special-item noise; annual fallback
    pe_raw = _pick(metrics, *_FIELD_SOURCE_KEYS["pe_ratio"])

    # P/B: annual is the canonical book-value reference; quarterly fallback for newly-listed
    pb_raw = _pick(metrics, *_FIELD_SOURCE_KEYS["pb_ratio"])

    # D/E: Finnhub uses literal slash in field name. Quarterly fallback when annual missing.
    de_raw = _pick(metrics, *_FIELD_SOURCE_KEYS["debt_to_equity"])

    # Revenue growth: TTM YoY preferred; quarterly YoY fallback; 5Y last (smooths cycles).
    revenue_growth_raw = _pick(metrics, *_FIELD_SOURCE_KEYS["revenue_growth_yoy"])

    # Gross margin: TTM preferred; annual fallback; 5Y last.
    gross_margin_raw = _pick(metrics, *_FIELD_SOURCE_KEYS["gross_margin"])

    # ROE: TTM preferred; Rfy (rolling fiscal year) fallback.
    roe_raw = _pick(metrics, *_FIELD_SOURCE_KEYS["roe"])

    # Current ratio: annual; quarterly fallback.
    current_ratio_raw = _pick(metrics, *_FIELD_SOURCE_KEYS["current_ratio"])

    # FCF yield: ``freeCashFlowTTM``/``freeCashFlowAnnual`` were assumed
    # present by analogy with the old FMP integration but are absent from
    # Finnhub's ``metric=all`` response on this plan for every ticker
    # sampled (alpha-engine-config-I7569) — confirmed live against AAPL,
    # INTC and F. Finnhub does expose price-to-FCF-per-share directly
    # (``pfcfShareTTM`` / ``pfcfShareAnnual`` fallback), which is the same
    # ratio inverted: FCF yield = 1 / (P/FCF). Fall back to NEUTRAL (0.0)
    # when the ratio is missing or non-positive — inverting a non-positive
    # P/FCF would silently emit a meaningless (or sign-flipped) yield.
    market_cap = _pick(metrics, *_FIELD_SOURCE_KEYS["market_cap_raw"])
    pfcf_share = _pick(metrics, *_FIELD_SOURCE_KEYS["fcf_yield"])
    if pfcf_share and pfcf_share > 0:
        fcf_yield_raw = 1.0 / pfcf_share
    else:
        fcf_yield_raw = 0.0

    # SIZE pillar substrate (config#1142): persist the already-fetched raw
    # market cap, UN-clipped / UN-normalized (it is the ``_raw`` column).
    # ``marketCapitalization`` is surfaced as Finnhub reports it (the same
    # source the fcf_yield ratio above already consumes). Non-positive /
    # missing -> 0.0; the feature engineer's log() guard maps 0.0 -> NaN so
    # a capless ticker is EXCLUDED from the SIZE cross-section, never mis-
    # sized. The SIZE loading is scale-invariant (a constant log shift is
    # removed by cross-sectional z-scoring), so the native unit is fine.
    market_cap_raw = market_cap if (market_cap and market_cap > 0) else 0.0

    # ── Growth pillar substrate (Phase 3a of attractiveness-pillars-260520) ──
    # 3-year CAGR signals from Finnhub. Smoother than TTM YoY for
    # composite ranking (base-effect noise + single-quarter anomalies
    # average out). Annual fallbacks for newer listings without a full
    # 3y history.
    revenue_growth_3y_raw = _pick(metrics, *_FIELD_SOURCE_KEYS["revenue_growth_3y"])
    eps_growth_3y_raw = _pick(metrics, *_FIELD_SOURCE_KEYS["eps_growth_3y"])

    # ── Stewardship pillar substrate (Phase 3a) ──
    # Payout ratio + dividend yield + capex growth proxy. Insider ownership
    # is NOT here — Finnhub's metric=all does not surface it; would require
    # a separate /stock/insider-transactions integration. Deferred to a
    # follow-up if/when stewardship gains discriminative weight in the
    # composite. The three signals here cover the "capital allocation
    # discipline" axis: payout (return-of-capital intensity), dividend
    # yield (vs. payout, identifies low-yield + low-payout = buyback-
    # heavy retainers), and capex growth (reinvestment intensity).
    payout_ratio_raw = _pick(metrics, *_FIELD_SOURCE_KEYS["payout_ratio"])
    dividend_yield_raw = _pick(metrics, *_FIELD_SOURCE_KEYS["dividend_yield"])
    # capitalSpendingGrowth5Y does not exist in Finnhub's response (same
    # absent-field problem as freeCashFlowTTM above); the real 5y CAPEX CAGR
    # field is capexCagr5Y (alpha-engine-config-I7569).
    capex_growth_5y_raw = _pick(metrics, *_FIELD_SOURCE_KEYS["capex_growth_5y"])

    # Finnhub reports growth/margin/return/payout/yield metrics as PERCENT
    # POINTS (e.g. ``grossMarginTTM: 48.65`` means 48.65%, ``roeTTM: 137.18``
    # means 137.18%) — verified live against AAPL/INTC/F
    # (alpha-engine-config-I7569). The comment this replaced claimed Finnhub
    # matched FMP's decimal-fraction convention (0.42 for 42%); that was
    # never verified and is wrong. Every percent-point field below is
    # divided by 100 before the pre-existing clip ranges (which were always
    # decimal-fraction bounds, e.g. dividend_yield hi=0.20 for 20%) are
    # applied — without it, values routinely exceed the clip bounds and get
    # silently clamped to the same boundary constant for most of the
    # universe (measured: gross_margin was 0.0/1.0 for every sampled
    # ticker, payout_ratio and dividend_yield pinned at their clip
    # ceilings). ``pe_ratio``/``pb_ratio``/``debt_to_equity``/
    # ``current_ratio`` are genuine ratios, not percentages, and are
    # unaffected — their existing fixed-divisor normalization is unchanged.
    return {
        "pe_ratio": _clip(pe_raw / 30.0, -3.0, 3.0),
        "pb_ratio": _clip(pb_raw / 5.0, -3.0, 3.0),
        "debt_to_equity": _clip(de_raw / 2.0, -3.0, 3.0),
        "revenue_growth_yoy": _clip(revenue_growth_raw / 100.0, -1.0, 2.0),
        "fcf_yield": _clip(fcf_yield_raw, -0.5, 0.5),
        "gross_margin": _clip(gross_margin_raw / 100.0, 0.0, 1.0),
        "roe": _clip(roe_raw / 100.0, -1.0, 1.0),
        "current_ratio": _clip(current_ratio_raw / 3.0, 0.0, 3.0),
        # Growth pillar quant signals
        "revenue_growth_3y": _clip(revenue_growth_3y_raw / 100.0, -0.5, 1.5),
        "eps_growth_3y": _clip(eps_growth_3y_raw / 100.0, -1.0, 2.0),
        # Stewardship pillar quant signals
        "payout_ratio": _clip(payout_ratio_raw / 100.0, 0.0, 2.0),
        "dividend_yield": _clip(dividend_yield_raw / 100.0, 0.0, 0.20),
        "capex_growth_5y": _clip(capex_growth_5y_raw / 100.0, -1.0, 2.0),
        # SIZE pillar substrate (config#1142): raw market cap in USD,
        # deliberately UN-clipped/UN-normalized (it's a _raw column). The
        # SIZE loading's log + cross-sectional z-score downstream tames the
        # scale; clipping here would corrupt the absolute units.
        "market_cap_raw": market_cap_raw,
        _ABSENT_KEY: _absent,
    }


def collect(
    bucket: str,
    tickers: list[str],
    run_date: str,
    dry_run: bool = False,
) -> dict:
    """
    Fetch fundamentals for all tickers and cache to S3.

    Returns summary dict with counts. ``status="error"`` if the ok_ratio
    gate is breached — downstream orchestrator treats the phase as failed.
    """
    import boto3

    api_key = get_secret("FINNHUB_API_KEY", required=False, default="")
    if not api_key:
        # Preflight is expected to catch this earlier; hard-fail here too
        # so a missing key can never land as "0 OK / N errors / all-zeros".
        return {
            "status": "error",
            "error": "FINNHUB_API_KEY not set — refusing to write all-NEUTRAL fundamentals",
        }

    logger.info(
        "Fetching fundamentals for %d tickers from Finnhub (/stock/metric)...",
        len(tickers),
    )
    t0 = time.time()

    # Read FUNDAMENTALS_BLOCK_ANOMALY_TYPES once per run (raises on
    # malformed env — fail fast before fetching).
    block_anomaly_types = _load_fundamentals_block_anomaly_types()

    results: dict[str, dict] = {}
    n_ok = 0
    n_err = 0
    # Write-time value-range gate accounting (parallels daily_append).
    n_quality_blocked = 0  # records replaced with NEUTRAL (block severity)
    n_quality_warned = 0   # records kept but flagged (warn severity)
    quality_counts_by_type: dict[str, int] = {}
    quality_blocked_details: list[str] = []  # "TICKER.type" per block

    absent_counts: dict[str, int] = {}
    n_fetched = 0

    for ticker in tickers:
        try:
            data = _fetch_single_ticker(ticker)
            # I7583: strip the transport-only absence set before ANY consumer
            # (the value-range gate, NEUTRAL comparison, the snapshot) sees it.
            _absent = data.pop(_ABSENT_KEY, None)
            if _absent is not None:
                n_fetched += 1
                for _f in _absent:
                    absent_counts[_f] = absent_counts.get(_f, 0) + 1
            # ── Write-time value-range gate ─────────────────────────────
            # Runs on the fully-shaped (clipped) per-ticker dict before it
            # is queued for the S3 snapshot. block → drop the corrupt row
            # to NEUTRAL + count (a NaN/inf or negative margin would
            # otherwise poison the predictor feature store); the aggregated
            # run-level logger.error below surfaces blocks to Flow Doctor.
            # warn → keep + log + count.
            qg = validate_feature_record(
                data, _FUNDAMENTALS_FIELD_SPECS, ticker
            )
            blocking = [
                a for a in qg["anomalies"]
                if a["type"] in block_anomaly_types
            ]
            if blocking:
                for a in blocking:
                    # WARNING per ticker; the single aggregated run-level
                    # logger.error below is the Flow Doctor surface (one
                    # systemic event → one alert, not one per ticker).
                    logger.warning(
                        "Fundamentals quality gate BLOCK %s.%s: %s",
                        ticker, a["type"], a["detail"],
                    )
                    quality_counts_by_type[a["type"]] = (
                        quality_counts_by_type.get(a["type"], 0) + 1
                    )
                    quality_blocked_details.append(f"{ticker}.{a['type']}")
                n_quality_blocked += 1
                # Refuse the corrupt row; NEUTRAL is the existing
                # no-data sentinel the ok_ratio gate already accounts for.
                results[ticker] = NEUTRAL.copy()
                n_err += 1
                continue
            if qg["anomalies"]:
                for a in qg["anomalies"]:
                    logger.warning(
                        "Fundamentals quality gate WARN %s.%s: %s",
                        ticker, a["type"], a["detail"],
                    )
                    quality_counts_by_type[a["type"]] = (
                        quality_counts_by_type.get(a["type"], 0) + 1
                    )
                n_quality_warned += 1
            results[ticker] = data
            if data != NEUTRAL:
                n_ok += 1
        except Exception as e:
            logger.warning("Fundamental fetch failed for %s: %s", ticker, e)
            results[ticker] = NEUTRAL.copy()
            n_err += 1

    elapsed = time.time() - t0
    ok_ratio = n_ok / max(len(tickers), 1)
    logger.info(
        "Fundamentals fetched in %.1fs: %d populated, %d errors, %d total (ok_ratio=%.1f%%)",
        elapsed, n_ok, n_err, len(results), ok_ratio * 100,
    )
    if n_quality_blocked:
        # Single aggregated ERROR per run — the Flow Doctor surface for the
        # block path (per-ticker lines above are WARNING-only; one systemic
        # event must produce one alert, not one per ticker — see the
        # 2026-06-11 daily_append EOD storm note).
        detail_list = ", ".join(quality_blocked_details[:20])
        if len(quality_blocked_details) > 20:
            detail_list += f", … +{len(quality_blocked_details) - 20} more"
        logger.error(
            "Fundamentals quality gate blocked %d ticker(s) this run "
            "(counts=%s): %s",
            n_quality_blocked, quality_counts_by_type, detail_list,
        )
    elif n_quality_warned:
        logger.info(
            "Fundamentals quality gate: %d blocked, %d warned, counts=%s",
            n_quality_blocked, n_quality_warned, quality_counts_by_type,
        )
    _emit_quality_gate_metrics(
        quality_counts_by_type, n_quality_blocked, n_quality_warned
    )
    _quality_fields = {
        "tickers_quality_blocked": n_quality_blocked,
        "tickers_quality_warned": n_quality_warned,
        "quality_anomaly_counts": quality_counts_by_type,
        "quality_block_anomaly_types": sorted(block_anomaly_types),
    }

    # ── Cross-sectional collapse gate (alpha-engine-config-I7583) ───────────
    # Runs BEFORE the ok_ratio gate below and is deliberately independent of
    # it: ok_ratio answers "did each ticker return anything", this answers
    # "does each FIELD carry any information across the universe". A field
    # that is dead for every ticker passes ok_ratio 903 times over — that is
    # exactly how capitalSpendingGrowth5Y and freeCashFlowTTM went unnoticed
    # for the life of the Finnhub integration (alpha-engine-config-I7569).
    # ── Absent-source gate (alpha-engine-config-I7583) ──────────────────────
    # The CAUSE-side check, run before the collapse gate below, which is the
    # SYMPTOM-side one. Both are kept: this names the missing vendor keys and
    # fires even when an absent field's default varies; the collapse gate
    # catches every other route to a dead column (units saturation, a scaling
    # divisor, a vendor pinning a value) without needing to know the cause.
    _absent_everywhere = {
        f: n for f, n in absent_counts.items()
        if n_fetched and (n / n_fetched) >= _SOURCE_ABSENCE_FAIL_SHARE
    }
    if _absent_everywhere and n_fetched >= _FIELD_COLLAPSE_MIN_TICKERS:
        msg = (
            "source key(s) absent from the vendor response for effectively the "
            f"whole universe over {n_fetched} fetched tickers: "
            + ", ".join(
                f"{f} ({n}/{n_fetched}, reads {list(_FIELD_SOURCE_KEYS[f])})"
                for f, n in sorted(_absent_everywhere.items())
            )
            + ". Each is currently being written as a default value a consumer "
            "cannot distinguish from real data. Refusing to write the snapshot."
        )
        logger.error("Fundamentals absent-source gate: %s", msg)
        return {
            "status": "error",
            "error": msg,
            "tickers_ok": n_ok,
            "tickers_error": n_err,
            "absent_source_fields": _absent_everywhere,
            **_quality_fields,
        }
    if absent_counts:
        # Below the fail share this is ordinary per-ticker sparsity, but it is
        # RECORDED rather than dropped: a field drifting from 5% to 60% absent
        # is the shape of a vendor deprecating a key, and it should be visible
        # before it crosses the threshold, not at the moment it halts a run.
        logger.info(
            "Fundamentals source-key coverage over %d fetched tickers "
            "(absent counts, below the %.0f%% fail share): %s",
            n_fetched, _SOURCE_ABSENCE_FAIL_SHARE * 100,
            dict(sorted(absent_counts.items())),
        )

    _real_records = [r for r in results.values() if r != NEUTRAL]
    if len(_real_records) < _FIELD_COLLAPSE_MIN_TICKERS:
        logger.info(
            "Fundamentals field-collapse gate NOT APPLIED: %d fetched ticker(s) "
            "is below the %d floor, where 'every value is identical' carries no "
            "information about cross-sectional coverage.",
            len(_real_records), _FIELD_COLLAPSE_MIN_TICKERS,
        )
        _collapse = {}
    else:
        _collapse = _field_collapse_report(_real_records)
    _collapsed = {
        f: (share, val) for f, (share, val) in _collapse.items()
        if share >= _FIELD_COLLAPSE_FAIL_SHARE
    }
    _concentrated = {
        f: (share, val) for f, (share, val) in _collapse.items()
        if _FIELD_COLLAPSE_WARN_SHARE <= share < _FIELD_COLLAPSE_FAIL_SHARE
    }
    if _concentrated:
        # Observe band — loud, not halting. See the promotion note on
        # _FIELD_COLLAPSE_WARN_SHARE.
        logger.error(
            "Fundamentals field-collapse WARN over %d fetched tickers: %s. "
            "Each of these carries one value for most of the universe. That is "
            "plausible for a genuinely concentrated field and is the signature "
            "of an absent source key or a units/clip mismatch — check before "
            "the next consumer reads it.",
            len(_real_records),
            {f: f"{share:.1%} at {val!r}" for f, (share, val) in sorted(_concentrated.items())},
        )
    if _collapsed:
        msg = (
            f"field(s) carry a single value for effectively the whole universe "
            f"over {len(_real_records)} fetched tickers: "
            + ", ".join(
                f"{f} ({share:.1%} at {val!r})"
                for f, (share, val) in sorted(_collapsed.items())
            )
            + ". A source key absent from the vendor response, a units mismatch "
            "saturating a clip bound, or a vendor field switched to a constant "
            "all land here. Refusing to write a fundamentals snapshot whose "
            "column(s) would read as fully covered while carrying zero "
            "cross-sectional information (alpha-engine-config-I7583)."
        )
        logger.error("Fundamentals collapse gate: %s", msg)
        return {
            "status": "error",
            "error": msg,
            "tickers_ok": n_ok,
            "tickers_error": n_err,
            "collapsed_fields": {f: share for f, (share, _v) in _collapsed.items()},
            "concentrated_fields": {f: share for f, (share, _v) in _concentrated.items()},
            **_quality_fields,
        }

    if ok_ratio < _MIN_OK_RATIO:
        msg = (
            f"only {n_ok}/{len(tickers)} tickers ({ok_ratio:.1%}) had populated "
            f"fundamentals — below {_MIN_OK_RATIO:.0%} threshold. Finnhub endpoint "
            f"likely auth-failed, quota-exhausted, or schema-changed. Refusing "
            f"to write a mostly-zero fundamentals snapshot that would silently "
            f"degrade the predictor + research scoring layers."
        )
        logger.error(msg)
        return {
            "status": "error",
            "error": msg,
            "n_tickers": len(results),
            "n_ok": n_ok,
            "n_errors": n_err,
            "elapsed_seconds": round(elapsed, 1),
            **_quality_fields,
        }

    if dry_run:
        logger.info("[dry-run] Would write fundamentals for %d tickers", len(results))
        return {
            "status": "ok",
            "n_tickers": len(results),
            "n_ok": n_ok,
            "n_errors": n_err,
            "elapsed_seconds": round(elapsed, 1),
            "dry_run": True,
            **_quality_fields,
        }

    # Write to S3
    s3 = boto3.client("s3")
    key = f"archive/fundamentals/{run_date}.json"
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(results, default=str),
        ContentType="application/json",
    )
    logger.info("Fundamentals cached to s3://%s/%s", bucket, key)

    return {
        "status": "ok",
        "n_tickers": len(results),
        "n_ok": n_ok,
        "n_errors": n_err,
        "elapsed_seconds": round(elapsed, 1),
        "s3_key": key,
        **_quality_fields,
    }
