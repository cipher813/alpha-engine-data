"""alpha-engine-config-I9287 — the FRED-only macro history refresh
(``collectors/fred_history.py::backfill_to_s3``) must run on the weekly
schedule, not stay a one-shot operator step.

Measured 2026-08-29: ``reference/price_cache/HYOAS.parquet`` last modified
2026-05-19 and never refreshed since, while ``builders/backfill.py`` rewrote
``macro/HYOAS`` from it every Saturday — a rewrite-with-frozen-source, not a
missing writer.
"""

from __future__ import annotations

from pathlib import Path

_WEEKLY_COLLECTOR = Path(__file__).parent.parent / "weekly_collector.py"


def _section(src: str, def_line: str) -> str:
    body = src.split(def_line)[1]
    next_def = body.find("\ndef ")
    return body if next_def == -1 else body[:next_def]


def test_run_phase1_invokes_fred_history_backfill():
    src = _WEEKLY_COLLECTOR.read_text()
    phase1_section = _section(src, "def _run_phase1(")
    assert "fred_history.backfill_to_s3(" in phase1_section, (
        "_run_phase1 must schedule collectors.fred_history.backfill_to_s3 — "
        "without it, TWO/HYOAS/BAA10Y's price-cache parquet is never "
        "refreshed and the weekly macro rebuild rewrites ArcticDB from a "
        "frozen source indefinitely."
    )


def test_fred_history_backfill_call_pins_explicit_fred_only_tickers():
    src = _WEEKLY_COLLECTOR.read_text()
    phase1_section = _section(src, "def _run_phase1(")
    call_start = phase1_section.index("fred_history.backfill_to_s3(")
    call_text = phase1_section[call_start:call_start + 400]
    assert 'tickers=["TWO", "HYOAS", "BAA10Y"]' in call_text, (
        "the weekly call must pin an EXPLICIT ticker list, never the "
        "FRED_HISTORY_MAP default — that map may grow to include the caret "
        "index tickers (VIX/VIX3M/TNX/IRX), which are collectors/prices.py's "
        "longest-of-yfinance-and-FRED job (alpha-engine-config-I9286); "
        "re-deriving them here from FRED alone would silently discard that "
        "fallback."
    )
