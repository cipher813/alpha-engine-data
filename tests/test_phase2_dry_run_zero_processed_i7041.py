"""Regression tests for alpha-engine-config-I7041.

Root cause, measured against CloudWatch log group
/aws/lambda/alpha-engine-data-collector: since the Saturday SF's
DataPhase2 state was repointed to EC2-spot ``weekly_collector.py``
(PR #1186, 2026-07-31), the ONLY invoker of this Lambda is
``infrastructure/deploy.sh``'s post-deploy canary, which always calls
with ``dry_run=true``. ``collectors.alternative.collect()``'s dry-run
branch returned the validated ticker count under the key ``"tickers"``,
never ``"tickers_processed"``; ``lambda/handler.py``'s completion log
unconditionally read ``result.get("tickers_processed", 0)``, so every
canary run — including ones that validated the full 903-ticker universe
— logged "Phase 2 complete: 0 tickers processed", indistinguishable
from a genuine empty-work-list failure.

These tests pin: (1) the dry-run branch reports its count under its own
key, distinct from a real run's; (2) that count round-trips correctly
for a healthy (non-zero) validation; (3) a CloudWatch metric is emitted
so the distinction is machine-readable, not just a log substring.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import collectors.alternative as alternative

PREFIX = "market_data/"


def _stub_s3_for_scope_guard(monkeypatch):
    """No-op the scope guard's S3 lookups — irrelevant to this test's focus."""
    monkeypatch.setattr(alternative, "_assert_scope_stable", lambda *a, **k: None)


def test_dry_run_reports_tickers_validated_not_tickers_processed(monkeypatch):
    _stub_s3_for_scope_guard(monkeypatch)
    monkeypatch.setattr(alternative.boto3, "client", lambda *a, **k: MagicMock())

    result = alternative.collect(
        bucket="alpha-engine-research",
        s3_prefix=PREFIX,
        run_date="2026-08-12",
        tickers=[f"T{i}" for i in range(903)],
        dry_run=True,
    )

    assert result["status"] == "ok_dry_run"
    assert result["tickers_validated"] == 903, (
        "A dry-run over the full universe must report the real validated "
        f"count, got: {result}"
    )
    assert "tickers_processed" not in result, (
        "The dry-run branch must not claim tickers_processed — a "
        "downstream reader (lambda/handler.py) treats that key as proof "
        "of real writes, and none happened."
    )


def test_dry_run_emits_a_cloudwatch_metric_distinct_from_real_runs(monkeypatch):
    """The dry-run validated count must be machine-readable, not just logged.

    Before this, no metric distinguished "canary validated 903" from
    "canary validated 0 (upstream universe collapsed)" — both rendered as
    the same log line and the same canary-OK exit code.
    """
    _stub_s3_for_scope_guard(monkeypatch)
    cw = MagicMock()
    monkeypatch.setattr(
        alternative.boto3, "client",
        lambda name, *a, **k: cw if name == "cloudwatch" else MagicMock(),
    )

    alternative.collect(
        bucket="alpha-engine-research",
        s3_prefix=PREFIX,
        run_date="2026-08-12",
        tickers=[f"T{i}" for i in range(903)],
        dry_run=True,
    )

    cw.put_metric_data.assert_called_once()
    _, kwargs = cw.put_metric_data.call_args
    assert kwargs["Namespace"] == "AlphaEngine/Data"
    metric_names = {m["MetricName"] for m in kwargs["MetricData"]}
    assert "phase2_dry_run_tickers_validated" in metric_names
    validated_metric = next(
        m for m in kwargs["MetricData"]
        if m["MetricName"] == "phase2_dry_run_tickers_validated"
    )
    assert validated_metric["Value"] == 903.0


def test_dry_run_metric_failure_does_not_fail_the_collector(monkeypatch, caplog):
    """CloudWatch errors are best-effort — mirrors ``_emit_quality_gate_metrics``."""
    _stub_s3_for_scope_guard(monkeypatch)

    def _client(name, *_a, **_k):
        if name == "cloudwatch":
            raise RuntimeError("cloudwatch unreachable")
        return MagicMock()

    monkeypatch.setattr(alternative.boto3, "client", _client)

    result = alternative.collect(
        bucket="alpha-engine-research",
        s3_prefix=PREFIX,
        run_date="2026-08-12",
        tickers=["AAPL", "MSFT"],
        dry_run=True,
    )

    assert result["status"] == "ok_dry_run"
    assert result["tickers_validated"] == 2
