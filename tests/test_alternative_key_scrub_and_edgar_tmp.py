"""Regression tests for API-key leak in alt-data exception logs.

FMP-backed warnings ("EPS estimate failed for AFL: 402 ... ?apikey=<KEY>")
and Finnhub-backed warnings (``token=<KEY>``) embed the live credential in
the request URL inside ``HTTPError.str()``. ``_scrub_url_creds`` masks
``apikey=``/``api_key=``/``token=`` querystring secrets before logging.

(EDGAR data-dir redirect tests were removed 2026-07-25 along with the
_fetch_institutional function — the edgar package is no longer imported.)
"""

from __future__ import annotations

import pytest


# ── _scrub_url_creds ───────────────────────────────────────────────────────


def test_scrub_masks_fmp_apikey_in_402_url():
    from collectors import alternative

    msg = (
        "402 Client Error: Payment Required for url: "
        "https://financialmodelingprep.com/stable/analyst-estimates"
        "?apikey=4509846484a78c3ee667a118d5179de7&symbol=AFL&period=annual"
    )
    scrubbed = alternative._scrub_url_creds(msg)
    assert "4509846484a78c3ee667a118d5179de7" not in scrubbed
    assert "apikey=***" in scrubbed
    # querystring after the key must survive (regex stops at ``&``).
    assert "symbol=AFL" in scrubbed and "period=annual" in scrubbed


def test_scrub_masks_api_key_underscore_variant():
    from collectors import alternative

    msg = "url: https://x/?api_key=SECRETVALUEXYZ&file_type=json"
    scrubbed = alternative._scrub_url_creds(msg)
    assert "SECRETVALUEXYZ" not in scrubbed
    assert "api_key=***" in scrubbed
    assert scrubbed == "url: https://x/?api_key=***&file_type=json"


def test_scrub_masks_token_variant():
    from collectors import alternative

    msg = "https://finnhub.io/api/v1/stock/recommendation?token=LIVEFINNHUB&symbol=AFL"
    scrubbed = alternative._scrub_url_creds(msg)
    assert "LIVEFINNHUB" not in scrubbed
    assert "token=***" in scrubbed


def test_scrub_accepts_exception_object_directly():
    """The helper is invoked at ``logger.warning("... %s", _scrub(e))``
    sites — it must accept an exception object, not just a str."""
    import requests

    try:
        resp = requests.Response()
        resp.status_code = 402
        resp.url = (
            "https://financialmodelingprep.com/stable/analyst-estimates"
            "?apikey=EXC_OBJ_SECRET&symbol=AFL"
        )
        resp.reason = "Payment Required"
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        from collectors import alternative

        scrubbed = alternative._scrub_url_creds(e)

    assert "EXC_OBJ_SECRET" not in scrubbed
    assert "apikey=***" in scrubbed


def test_scrub_is_noop_on_clean_string():
    from collectors import alternative

    msg = "Finnhub recommendation failed for AFL: connection timed out"
    assert alternative._scrub_url_creds(msg) == msg


def test_scrub_is_idempotent():
    from collectors import alternative

    msg = "url: https://fmp/x?apikey=SECRET&symbol=AFL"
    once = alternative._scrub_url_creds(msg)
    twice = alternative._scrub_url_creds(once)
    assert once == twice
    assert "SECRET" not in twice


def test_scrub_case_insensitive():
    from collectors import alternative

    msg = "https://x/?APIKEY=MIXEDCASE&a=1"
    scrubbed = alternative._scrub_url_creds(msg)
    assert "MIXEDCASE" not in scrubbed
    assert "APIKEY=***" in scrubbed
