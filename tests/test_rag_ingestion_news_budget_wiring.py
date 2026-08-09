"""config#2938 — the weekly RAGIngestion timeouts must hold the FULL-universe
news sweep, and all surfaces of the 4h budget must stay in lockstep.

The 2026-07-18 weekly SF failure was a DRIFT bug: the news universe grew ~9x
while the RAGIngestion SSM ``executionTimeout`` stayed at a DataPhase1-sized
3600s, so the ~3.15h Polygon sweep SIGKILLed twice. The fix sizes the runtime
Polygon budget from the LIVE universe (``fetch_budget``) and raises the outer
step timeouts to the config#2938 4h hard cap. This guard pins that the
static timeouts equal the single ``WEEKLY_RAG_EXECUTION_TIMEOUT_SECONDS``
constant they were derived from, so a future edit to any one of them fails CI
unless the others move with it:

  1. RAGIngestion ``executionTimeout`` in infrastructure/step_function.json,
  2. the ``run_ssm "rag-ingestion"`` workload timeout in
     infrastructure/spot_rag_ingestion.sh (the LIVE path since the
     alpha-engine-config-I4442/I4497 SF cutover, 2026-08-09,
     nousergon-data#1122 — pre-cutover this was spot_data_weekly.sh's
     ``run_ssm "rag-only"``),
  3. the rag spot-watchdog ``MAX_RUNTIME_SECONDS`` (cap + shutdown margin) in
     spot_rag_ingestion.sh,

and that the runtime per-universe budget always leaves reserve for the rest of
the step inside that cap.

spot_data_weekly.sh is retained on disk, unchanged, only as the rollback
path — RAGIngestion no longer invokes it. Two regression guards below
(``test_other_modes_keep_dataphase1_watchdog_default``,
``test_max_runtime_explicit_default_initialized_before_use``) still pin
spot_data_weekly.sh's own internal mode-crosstalk behavior so the rollback
path stays correct if it is ever needed; they are not exercising the live
RAGIngestion path.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from collectors.news_sources.fetch_budget import (
    WEEKLY_RAG_EXECUTION_TIMEOUT_SECONDS,
    weekly_news_max_fetch_seconds,
)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_SF = _REPO_ROOT / "infrastructure" / "step_function.json"
_SCRIPT = _REPO_ROOT / "infrastructure" / "spot_data_weekly.sh"  # rollback path only
_RAG_SCRIPT = _REPO_ROOT / "infrastructure" / "spot_rag_ingestion.sh"  # live path


def _find_state(node, name):
    if isinstance(node, dict):
        for k, v in node.items():
            if k == name and isinstance(v, dict) and v.get("Type"):
                return v
            found = _find_state(v, name)
            if found is not None:
                return found
    elif isinstance(node, list):
        for x in node:
            found = _find_state(x, name)
            if found is not None:
                return found
    return None


def _rag_execution_timeout() -> int:
    sf = json.loads(_SF.read_text())
    state = _find_state(sf, "RAGIngestion")
    assert state is not None, "RAGIngestion state not found in step_function.json"
    et = state["Parameters"]["Parameters"]["executionTimeout"]
    assert isinstance(et, list) and len(et) == 1, f"unexpected executionTimeout shape: {et!r}"
    return int(et[0])


def test_rag_execution_timeout_matches_4h_cap():
    # The SIGKILL boundary that fired on 2026-07-18 (was 3600s).
    assert _rag_execution_timeout() == WEEKLY_RAG_EXECUTION_TIMEOUT_SECONDS


def test_rag_execution_timeout_holds_full_universe_sweep():
    # ruling 1: the outer cap must strictly exceed the runtime Polygon budget
    # for any universe, so the fetch + the rest of the step fit inside it.
    et = _rag_execution_timeout()
    for n in (944, 2000, 100_000):
        assert weekly_news_max_fetch_seconds(n) < et


def test_inner_run_ssm_rag_ingestion_timeout_in_lockstep():
    """Live path (post I4442/I4497 cutover): spot_rag_ingestion.sh's own
    workload timeout must equal WEEKLY_RAG_EXECUTION_TIMEOUT_SECONDS, the
    same constant the SF's RAGIngestion executionTimeout is derived from."""
    text = _RAG_SCRIPT.read_text()
    m = re.search(r'_RAG_WORKLOAD_TIMEOUT="\$\{RAG_WORKLOAD_TIMEOUT:-(\d+)\}"', text)
    assert m, "_RAG_WORKLOAD_TIMEOUT default not set in spot_rag_ingestion.sh"
    assert int(m.group(1)) == WEEKLY_RAG_EXECUTION_TIMEOUT_SECONDS
    # ...and the ingestion workload SSM call actually uses that variable, not
    # a re-introduced literal.
    assert 'run_ssm "rag-ingestion"' in text
    assert '"${_RAG_WORKLOAD_TIMEOUT}"' in text


def test_rag_ingestion_spot_watchdog_exceeds_outer_cap():
    # The box's shutdown watchdog must be a BACKSTOP: strictly greater than the
    # outer SF executionTimeout so cleanup (not a premature box shutdown) ends
    # the run. spot_rag_ingestion.sh is a single-purpose script (no mode
    # dispatch), so MAX_RUNTIME_SECONDS is a flat top-level constant.
    text = _RAG_SCRIPT.read_text()
    m = re.search(r"^MAX_RUNTIME_SECONDS=(\d+)", text, re.MULTILINE)
    assert m, "MAX_RUNTIME_SECONDS not set in spot_rag_ingestion.sh"
    assert int(m.group(1)) > WEEKLY_RAG_EXECUTION_TIMEOUT_SECONDS


def test_other_modes_keep_dataphase1_watchdog_default():
    # ROLLBACK-PATH GUARD ONLY (spot_data_weekly.sh is no longer RAGIngestion's
    # live script post I4442/I4497 — see module docstring). Only rag-only gets
    # the 4h watchdog; the shared default (DataPhase1 / workloads) stays 5400s
    # so those modes are not silently over-budgeted if the monolith is ever
    # invoked manually for a rollback.
    text = _SCRIPT.read_text()
    assert 'MAX_RUNTIME_SECONDS="${MAX_RUNTIME_SECONDS:-5400}"' in text


def test_max_runtime_explicit_default_initialized_before_use():
    # ROLLBACK-PATH GUARD ONLY (see module docstring). The script runs under
    # `set -u`; the --max-runtime-seconds flag path is the only assignment of
    # MAX_RUNTIME_EXPLICIT=1. Without a default init BEFORE the rag-only
    # override check, every SF-driven rag-only dispatch dies with
    # "MAX_RUNTIME_EXPLICIT: unbound variable" (2026-07-18
    # watch-rerun-2026-07-18-1 failure — the exact incident this pins).
    text = _SCRIPT.read_text()
    default = 'MAX_RUNTIME_EXPLICIT="${MAX_RUNTIME_EXPLICIT:-0}"'
    assert default in text, "MAX_RUNTIME_EXPLICIT must be default-initialized"
    assert text.index(default) < text.index('"$MAX_RUNTIME_EXPLICIT" != "1"'), (
        "default init must precede the rag-only override check"
    )
