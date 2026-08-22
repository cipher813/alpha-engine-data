"""Unit tests for alpha-engine-preopen-deploy-readiness-probe
(alpha-engine-config-I7800 deliverable #2).

Covers the branches named in the issue's closes-when: non-trading-day no-op,
clean stamp (no self-heal, no page), drift that self-heals (dispatch clears
it, no page), and drift that survives self-heal (pages with diagnostics).
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

import index


def _lambda_payload(sf_drift: bool, **extra) -> dict:
    body = {
        "sf_drift": sf_drift,
        "sf_sha": "abc1234",
        "upstream_sha": "def5678",
        "cf_drift": False,
        "cf_drift_reason": "in_sync",
    }
    body.update(extra)
    return body


def _lam_client(responses: list[dict]) -> MagicMock:
    """A boto3 lambda client mock whose .invoke() returns successive
    payloads from `responses`, one per call."""
    cli = MagicMock()
    calls = iter(responses)

    def _invoke(**kwargs):
        payload = next(calls)
        m = MagicMock()
        m.get.side_effect = lambda k, default=None: {"FunctionError": None}.get(k, default)
        m.__getitem__ = lambda self, k: {"Payload": MagicMock(read=lambda: __import__("json").dumps(payload).encode())}[k]
        return m

    cli.invoke.side_effect = _invoke
    return cli


class TestNonTradingDay:
    def test_noop_when_not_a_trading_day(self):
        with patch("index.is_trading_day", return_value=False), \
             patch("index.boto3.client") as mock_boto:
            result = index.handler({}, None)
        assert result["action"] == "noop"
        assert result["reason"] == "not_a_trading_day"
        mock_boto.assert_not_called()


class TestCleanStamp:
    def test_no_self_heal_no_page_when_sf_drift_false(self):
        clean = _lambda_payload(sf_drift=False)
        s3_mock = MagicMock()
        lam_mock = _lam_client([clean])

        def boto_side_effect(service, **kw):
            return lam_mock if service == "lambda" else s3_mock

        with patch("index.is_trading_day", return_value=True), \
             patch("index.boto3.client", side_effect=boto_side_effect), \
             patch("index._page") as mock_page, \
             patch("index._dispatch_deploy_infrastructure") as mock_dispatch:
            result = index.handler({}, None)

        assert result["action"] == "noop"
        assert result["reason"] == "clean"
        mock_page.assert_not_called()
        mock_dispatch.assert_not_called()
        s3_mock.put_object.assert_called_once()


class TestDriftSelfHeals:
    def test_dispatches_reprobes_and_stays_silent_when_cleared(self):
        drifted = _lambda_payload(sf_drift=True)
        clean_after = _lambda_payload(sf_drift=False)
        s3_mock = MagicMock()
        lam_mock = _lam_client([drifted, clean_after])

        def boto_side_effect(service, **kw):
            return lam_mock if service == "lambda" else s3_mock

        with patch("index.is_trading_day", return_value=True), \
             patch("index.boto3.client", side_effect=boto_side_effect), \
             patch("index._dispatch_deploy_infrastructure", return_value=(True, "dispatched ok")), \
             patch("index._poll_dispatch_conclusion", return_value=("success", "run completed: success")), \
             patch("index._page") as mock_page:
            result = index.handler({}, None)

        assert result["action"] == "self_healed"
        assert result["self_heal_dispatched"] is True
        mock_page.assert_not_called()
        s3_mock.put_object.assert_called_once()


class TestDriftSurvivesSelfHeal:
    def test_pages_with_diagnostics_when_still_drifted(self):
        drifted = _lambda_payload(sf_drift=True)
        still_drifted = _lambda_payload(sf_drift=True, cf_drift=True, cf_drift_reason="sha_mismatch")
        s3_mock = MagicMock()
        lam_mock = _lam_client([drifted, still_drifted])

        def boto_side_effect(service, **kw):
            return lam_mock if service == "lambda" else s3_mock

        with patch("index.is_trading_day", return_value=True), \
             patch("index.boto3.client", side_effect=boto_side_effect), \
             patch("index._dispatch_deploy_infrastructure", return_value=(True, "dispatched ok")), \
             patch("index._poll_dispatch_conclusion", return_value=("failure", "run completed: failure")), \
             patch("index.alerts.publish") as mock_publish:
            mock_publish.return_value = MagicMock(sns=MagicMock(ok=True))
            result = index.handler({}, None)

        assert result["action"] == "paged"
        assert result["reason"] == "still_drifted_after_self_heal"
        mock_publish.assert_called_once()
        call_kwargs = mock_publish.call_args.kwargs
        assert call_kwargs["severity"] == "critical"
        assert "preopen will halt" in call_kwargs["message"]
        s3_mock.put_object.assert_called_once()

    def test_pages_even_when_dispatch_itself_fails(self):
        drifted = _lambda_payload(sf_drift=True)
        still_drifted = _lambda_payload(sf_drift=True)
        s3_mock = MagicMock()
        lam_mock = _lam_client([drifted, still_drifted])

        def boto_side_effect(service, **kw):
            return lam_mock if service == "lambda" else s3_mock

        with patch("index.is_trading_day", return_value=True), \
             patch("index.boto3.client", side_effect=boto_side_effect), \
             patch("index._dispatch_deploy_infrastructure",
                   return_value=(False, "workflow_dispatch HTTP 403: insufficient scope")), \
             patch("index.alerts.publish") as mock_publish:
            mock_publish.return_value = MagicMock(sns=MagicMock(ok=True))
            result = index.handler({}, None)

        assert result["action"] == "paged"
        assert result["self_heal_dispatched"] is False
        mock_publish.assert_called_once()


class TestVerdictWrite:
    def test_verdict_key_is_dated(self):
        clean = _lambda_payload(sf_drift=False)
        s3_mock = MagicMock()
        lam_mock = _lam_client([clean])

        def boto_side_effect(service, **kw):
            return lam_mock if service == "lambda" else s3_mock

        fixed_today = date(2026, 8, 21)
        with patch("index.is_trading_day", return_value=True), \
             patch("index.boto3.client", side_effect=boto_side_effect), \
             patch("index.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 8, 21, 11, 30, tzinfo=timezone.utc)
            index.handler({}, None)

        _, kwargs = s3_mock.put_object.call_args
        assert kwargs["Key"] == "deploy_readiness/2026-08-21.json"


# ── alpha-engine-config-I8142: absence is not clean ──────────────────────────
#
# `check_deploy_drift` OMITS `sf_drift` when it could not measure it, and the
# preopen `DeployDriftGate` halts on exactly that payload. This probe read the
# same payload with `drift.get("sf_drift")` — falsy — and wrote
# action=noop reason=clean, 45 minutes before the session it had already
# decided. The three states must stay three all the way down.

def _unmeasured_payload(**extra) -> dict:
    """The literal shape the probe emits post-I8142: no `sf_drift` key at all,
    with the positive state field and the typed cause beside it."""
    body = {
        "sf_sha": None,
        "upstream_sha": "def5678",
        "sf_drift_state": "unmeasured",
        "sf_drift_reason": "live_definition_unreadable",
        "sf_definition_reason": "live_definition_unreadable",
        "live_definition_error": {
            "reason": "describe_state_machine_error",
            "detail": "AccessDeniedException: not authorized",
        },
        "cf_drift": False,
        "cf_drift_reason": "in_sync",
    }
    body.update(extra)
    return body


class TestUnmeasuredVerdict:
    def _run(self, payloads):
        s3_mock = MagicMock()
        lam_mock = _lam_client(payloads)

        def boto_side_effect(service, **kw):
            return lam_mock if service == "lambda" else s3_mock

        with patch("index.is_trading_day", return_value=True), \
             patch("index.boto3.client", side_effect=boto_side_effect), \
             patch("index._dispatch_deploy_infrastructure") as mock_dispatch, \
             patch("index.alerts.publish") as mock_publish:
            mock_publish.return_value = MagicMock(sns=MagicMock(ok=True))
            result = index.handler({}, None)
        return result, mock_publish, mock_dispatch, s3_mock

    def test_an_unmeasured_verdict_pages_and_is_never_reported_clean(self):
        result, mock_publish, mock_dispatch, s3_mock = self._run(
            [_unmeasured_payload()],
        )
        assert result["action"] == "paged"
        assert result["reason"] == "unmeasured"
        assert result["sf_drift_state_initial"] == "unmeasured"
        mock_publish.assert_called_once()
        assert mock_publish.call_args.kwargs["severity"] == "critical"
        # No self-heal: a deploy dispatch on an unmeasured verdict acts on
        # evidence nobody has.
        mock_dispatch.assert_not_called()
        s3_mock.put_object.assert_called_once()

    def test_the_page_names_the_unreadable_definition_and_the_halt(self):
        _, mock_publish, _, _ = self._run([_unmeasured_payload()])
        message = mock_publish.call_args.kwargs["message"]
        assert "UNMEASURED" in message
        assert "PREOPEN WILL HALT" in message
        assert "live_definition_unreadable" in message

    def test_absence_of_the_key_is_unmeasured_without_the_state_field(self):
        """Rollout safety: a predictor Lambda older than I8142 omits `sf_drift`
        without emitting `sf_drift_state`. Key presence must still decide."""
        payload = _unmeasured_payload()
        payload.pop("sf_drift_state")
        result, mock_publish, _, _ = self._run([payload])
        assert result["action"] == "paged"
        assert result["reason"] == "unmeasured"
        mock_publish.assert_called_once()

    def test_an_unmeasured_reprobe_is_not_a_self_heal(self):
        """The same collapse on the second read: after a dispatch, an
        unmeasured verdict is 'we no longer know', not 'fixed'."""
        s3_mock = MagicMock()
        lam_mock = _lam_client([_lambda_payload(sf_drift=True), _unmeasured_payload()])

        def boto_side_effect(service, **kw):
            return lam_mock if service == "lambda" else s3_mock

        with patch("index.is_trading_day", return_value=True), \
             patch("index.boto3.client", side_effect=boto_side_effect), \
             patch("index._dispatch_deploy_infrastructure", return_value=(True, "dispatched ok")), \
             patch("index._poll_dispatch_conclusion", return_value=("success", "run completed: success")), \
             patch("index.alerts.publish") as mock_publish:
            mock_publish.return_value = MagicMock(sns=MagicMock(ok=True))
            result = index.handler({}, None)

        assert result["action"] == "paged"
        assert result["reason"] == "unmeasured_after_self_heal"
        assert result["sf_drift_state_after_self_heal"] == "unmeasured"
        mock_publish.assert_called_once()


class TestDriftStateHelper:
    def test_the_three_states_are_three(self):
        assert index._drift_state({"sf_drift": True}) == "drift"
        assert index._drift_state({"sf_drift": False}) == "no_drift"
        assert index._drift_state({}) == "unmeasured"
        # The positive field wins when present — it is the one the producer
        # states rather than the one a consumer infers.
        assert index._drift_state({"sf_drift_state": "unmeasured"}) == "unmeasured"
