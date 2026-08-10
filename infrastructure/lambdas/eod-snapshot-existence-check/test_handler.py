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


def _s3_head(*, present: bool, error_code: str | None = None,
             pnl_csv: str | None = "date,total_cash\n2026-08-07,100.0\n",
             pnl_error_code: str | None = None):
    """An S3 client mock whose ``head_object`` either succeeds (present),
    raises a NoSuchKey/404-shaped ClientError (absent), or raises an
    arbitrary error (probe-infra failure). ``get_object`` serves
    ``pnl_csv`` for the eod_pnl export (default: a CSV containing the
    2026-08-07 row most handler tests target), or raises
    ``pnl_error_code``."""
    cli = MagicMock()
    if present:
        cli.head_object.return_value = {"ContentLength": 1234}
    else:
        code = error_code or "404"
        cli.head_object.side_effect = ClientError(
            {"Error": {"Code": code, "Message": "Not Found"}}, "HeadObject"
        )
    if pnl_error_code is not None:
        cli.get_object.side_effect = ClientError(
            {"Error": {"Code": pnl_error_code, "Message": "err"}}, "GetObject"
        )
    else:
        body = MagicMock()
        body.read.return_value = (pnl_csv or "").encode()
        cli.get_object.return_value = {"Body": body}
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
            "reason": "artifacts_present",
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


# ── eod_pnl NAV-row check (alpha-engine-config-I6733, §4.1 NAV continuity) ──


_CSV_WITH_ROW = "date,total_cash\n2026-08-06,99.0\n2026-08-07,100.0\n"
_CSV_WITHOUT_ROW = "date,total_cash\n2026-08-06,99.0\n"


class TestEodPnlRowState:
    def test_present_when_row_exists(self):
        cli = _s3_head(present=True, pnl_csv=_CSV_WITH_ROW)
        assert index._eod_pnl_row_state(cli, "bucket", "2026-08-07") == "PRESENT"

    def test_row_absent_when_day_missing(self):
        cli = _s3_head(present=True, pnl_csv=_CSV_WITHOUT_ROW)
        assert index._eod_pnl_row_state(cli, "bucket", "2026-08-07") == "ROW_ABSENT"

    def test_csv_absent_on_nosuchkey(self):
        cli = _s3_head(present=True, pnl_error_code="NoSuchKey")
        assert index._eod_pnl_row_state(cli, "bucket", "2026-08-07") == "CSV_ABSENT"

    def test_other_s3_error_raises(self):
        cli = _s3_head(present=True, pnl_error_code="AccessDenied")
        with pytest.raises(ClientError):
            index._eod_pnl_row_state(cli, "bucket", "2026-08-07")

    def test_csv_without_date_column_raises_not_certifies(self):
        cli = _s3_head(present=True, pnl_csv="nav,cash\n1,2\n")
        with pytest.raises(ValueError, match="no 'date' column"):
            index._eod_pnl_row_state(cli, "bucket", "2026-08-07")

    def test_datetime_formatted_date_still_matches(self):
        cli = _s3_head(present=True, pnl_csv="date,x\n2026-08-07 00:00:00,1\n")
        assert index._eod_pnl_row_state(cli, "bucket", "2026-08-07") == "PRESENT"


class TestPageAbsentEodPnlRow:
    def test_row_absent_message_names_backfill_and_severity_error(self):
        with patch("index.alerts.publish") as mock_publish:
            mock_publish.return_value = MagicMock(sns=MagicMock(ok=True))
            index._page_absent_eod_pnl_row("2026-08-07", "ROW_ABSENT")
        kwargs = mock_publish.call_args.kwargs
        assert kwargs["severity"] == "error"
        assert "NAV continuity" in kwargs["message"]
        assert "backfill_eod_pnl.py --date 2026-08-07" in kwargs["message"]
        assert "alpha-engine-config-I6733" in kwargs["message"]

    def test_csv_absent_message_names_the_missing_export(self):
        with patch("index.alerts.publish") as mock_publish:
            mock_publish.return_value = MagicMock(sns=MagicMock(ok=True))
            index._page_absent_eod_pnl_row("2026-08-07", "CSV_ABSENT")
        assert "MISSING" in mock_publish.call_args.kwargs["message"]


class TestHandlerEodPnl:
    _frozen_now = TestHandler._frozen_now

    def test_pages_when_row_absent_even_with_snapshot_present(self):
        """The closes-when demonstration (I6733): a synthetic missing-row
        trading day takes the PAGE path while the snapshot check stays
        quiet — the two artifacts page independently."""
        cli = _s3_head(present=True, pnl_csv=_CSV_WITHOUT_ROW)
        with patch("index.datetime", self._frozen_now("2026-08-07T20:30:00")), \
             patch("index.is_trading_day", return_value=True), \
             patch("index.boto3.client", return_value=cli), \
             patch("index._page_absent_eod_pnl_row") as mock_page:
            result = index.handler({}, None)
        assert result["action"] == "paged"
        assert result["reason"] == "eod_pnl_row_absent"
        mock_page.assert_called_once_with("2026-08-07", "ROW_ABSENT")

    def test_pages_both_when_snapshot_and_row_absent(self):
        cli = _s3_head(present=False, error_code="NoSuchKey", pnl_error_code="NoSuchKey")
        with patch("index.datetime", self._frozen_now("2026-08-07T20:30:00")), \
             patch("index.is_trading_day", return_value=True), \
             patch("index.boto3.client", return_value=cli), \
             patch("index._page_absent_snapshot") as mock_snap, \
             patch("index._page_absent_eod_pnl_row") as mock_pnl:
            result = index.handler({}, None)
        assert result["reason"] == "snapshot_absent+eod_pnl_csv_absent"
        mock_snap.assert_called_once()
        mock_pnl.assert_called_once_with("2026-08-07", "CSV_ABSENT")

    def test_raises_on_pnl_probe_infra_failure(self):
        cli = _s3_head(present=True, pnl_error_code="AccessDenied")
        with patch("index.datetime", self._frozen_now("2026-08-07T20:30:00")), \
             patch("index.is_trading_day", return_value=True), \
             patch("index.boto3.client", return_value=cli):
            with pytest.raises(ClientError):
                index.handler({}, None)
