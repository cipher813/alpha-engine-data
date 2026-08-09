"""Unit tests for the alpha-engine-eod-snapshot-existence-check Lambda
(alpha-engine-config-I6705, I5569 deliverable #3).

Covers the four handler branches named in the issue's closes-when:
non-trading-day no-op, snapshot-present silent success, snapshot-absent
paging (the "demonstrated firing against a deliberately absent key" case —
see ``TestHandler.test_pages_operator_when_snapshot_absent_verified_red``),
and fail-loud on any non-absence S3 error.
"""

from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest
from botocore.exceptions import ClientError

import index


def _s3_head(*, present: bool, error_code: str | None = None):
    """An S3 client mock whose ``head_object`` either succeeds (present),
    raises a NoSuchKey/404-shaped ClientError (absent), or raises an
    arbitrary error (probe-infra failure)."""
    cli = MagicMock()
    if present:
        cli.head_object.return_value = {"ContentLength": 1234}
    else:
        code = error_code or "404"
        cli.head_object.side_effect = ClientError(
            {"Error": {"Code": code, "Message": "Not Found"}}, "HeadObject"
        )
    return cli


class TestSnapshotKey:
    def test_matches_snapshot_capturer_convention(self):
        # Mirrors executor/snapshot_capturer.py::_snapshot_key exactly.
        assert index._snapshot_key("2026-08-07") == "trades/snapshots/2026-08-07.json"


class TestSnapshotExists:
    def test_present_returns_true(self):
        cli = _s3_head(present=True)
        assert index._snapshot_exists(cli, "bucket", "key") is True

    def test_404_returns_false(self):
        cli = _s3_head(present=False, error_code="404")
        assert index._snapshot_exists(cli, "bucket", "key") is False

    def test_nosuchkey_returns_false(self):
        cli = _s3_head(present=False, error_code="NoSuchKey")
        assert index._snapshot_exists(cli, "bucket", "key") is False

    def test_other_client_error_raises(self):
        cli = _s3_head(present=False, error_code="AccessDenied")
        with pytest.raises(ClientError):
            index._snapshot_exists(cli, "bucket", "key")


class TestPageAbsentSnapshot:
    def test_publishes_to_watchdog_topic_with_irreversibility_message(self):
        with patch("index.alerts.publish") as mock_publish:
            mock_publish.return_value = MagicMock(sns=MagicMock(ok=True))
            index._page_absent_snapshot("2026-08-07", "trades/snapshots/2026-08-07.json")

        kwargs = mock_publish.call_args.kwargs
        assert kwargs["sns_topic_arn"] == index.SNS_TOPIC_ARN
        assert kwargs["sns"] is True
        assert kwargs["telegram"] is False
        assert kwargs["severity"] == "critical"
        assert "UNRECOVERABLE" in kwargs["message"]
        assert "alpha-engine-config-I5325" in kwargs["message"]
        assert "snapshot_capturer.py --date 2026-08-07" in kwargs["message"]


class TestHandler:
    ET = ZoneInfo("America/New_York")

    def _frozen_now(self, iso: str):
        class _FrozenDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                base = datetime.fromisoformat(iso)
                return base.replace(tzinfo=tz) if tz else base

        return _FrozenDatetime

    def test_noop_when_not_a_trading_day(self):
        with patch("index.datetime", self._frozen_now("2026-08-08T20:30:00")), \
             patch("index.is_trading_day", return_value=False) as mock_trading_day, \
             patch("index.boto3.client") as mock_boto:
            result = index.handler({}, None)

        assert result == {"action": "noop", "reason": "not_a_trading_day", "date": "2026-08-08"}
        mock_trading_day.assert_called_once_with(date(2026, 8, 8))
        mock_boto.assert_not_called()

    def test_noop_when_snapshot_present(self):
        cli = _s3_head(present=True)
        with patch("index.datetime", self._frozen_now("2026-08-07T20:30:00")), \
             patch("index.is_trading_day", return_value=True), \
             patch("index.boto3.client", return_value=cli):
            result = index.handler({}, None)

        assert result == {
            "action": "noop",
            "reason": "snapshot_present",
            "trading_day": "2026-08-07",
        }
        cli.head_object.assert_called_once_with(
            Bucket=index.BUCKET, Key="trades/snapshots/2026-08-07.json"
        )

    def test_pages_operator_when_snapshot_absent_verified_red(self):
        """The closes-when demonstration (I5569 / I6705): a deliberately
        absent key must take the PAGE path, not silently pass. A guard not
        verified red is indistinguishable from no guard at all."""
        cli = _s3_head(present=False, error_code="NoSuchKey")
        with patch("index.datetime", self._frozen_now("2026-08-07T20:30:00")), \
             patch("index.is_trading_day", return_value=True), \
             patch("index.boto3.client", return_value=cli), \
             patch("index._page_absent_snapshot") as mock_page:
            result = index.handler({}, None)

        assert result == {
            "action": "paged",
            "reason": "snapshot_absent",
            "trading_day": "2026-08-07",
            "key": "trades/snapshots/2026-08-07.json",
        }
        mock_page.assert_called_once_with(
            "2026-08-07", "trades/snapshots/2026-08-07.json"
        )

    def test_raises_on_s3_probe_infra_failure(self):
        """Fail-loud: an AccessDenied/throttling/etc S3 error must never
        resolve to a silent no-op or a silently-skipped page."""
        cli = _s3_head(present=False, error_code="AccessDenied")
        with patch("index.datetime", self._frozen_now("2026-08-07T20:30:00")), \
             patch("index.is_trading_day", return_value=True), \
             patch("index.boto3.client", return_value=cli):
            with pytest.raises(ClientError):
                index.handler({}, None)
