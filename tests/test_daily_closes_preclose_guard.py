"""Regression tests for the daily_closes pre-close skip guard.

Contract: ``collect()`` must only short-circuit with ``skipped=True`` when
the existing ``{run_date}.parquet`` was written AFTER the NYSE close for
``run_date``. A pre-close write is a morning-side stale fetch (polygon
returns T-1's aggregate stamped under today's key) and must be overwritten
by the authoritative post-close collection.

Incident that forced this: 2026-04-20. The predictor's morning DailyData
Step Function wrote the parquet at 06:07 PT with Friday's closes stamped
as Monday. The 16:14 PT post-close rerun hit the old ``head_object →
skip`` short-circuit and propagated the stale data through daily_append
into ArcticDB for every ticker, producing a false α = −1.33% on the EOD
email vs the real +0.08%.
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from collectors import daily_closes


def _head_object_response(last_modified: datetime) -> dict:
    return {
        "LastModified": last_modified,
        "ContentLength": 12345,
        "ContentType": "application/octet-stream",
    }


def test_post_close_write_is_skipped():
    """If the parquet exists and was written after NYSE close, skip."""
    # 2026-04-20 is EDT; NYSE close = 20:00 UTC. Post-close write = 23:14 UTC.
    post_close = datetime(2026, 4, 20, 23, 14, 0, tzinfo=timezone.utc)

    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = _head_object_response(post_close)

    with patch("collectors.daily_closes.boto3.client", return_value=mock_s3):
        result = daily_closes.collect(
            bucket="test-bucket",
            tickers=["AAPL"],
            run_date="2026-04-20",
        )

    assert result["skipped"] is True
    assert result["status"] == "ok"
    # No write attempted — we trusted the post-close file.
    mock_s3.put_object.assert_not_called()


def test_pre_close_write_forces_refetch():
    """If the parquet exists but was written pre-close, refuse to skip."""
    # 2026-04-20 morning write at 13:07 UTC (06:07 PT) — well before 20:00 UTC close.
    pre_close = datetime(2026, 4, 20, 13, 7, 0, tzinfo=timezone.utc)

    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = _head_object_response(pre_close)

    # Force polygon+yfinance paths to return nothing so collect() bails with
    # "no data fetched" — we only need to prove the early-skip was NOT taken.
    with patch("collectors.daily_closes.boto3.client", return_value=mock_s3), \
         patch("collectors.daily_closes._fetch_yfinance_closes", return_value=0):
        result = daily_closes.collect(
            bucket="test-bucket",
            tickers=["AAPL"],
            run_date="2026-04-20",
        )

    # Early-skip path would have returned {"skipped": True, "status": "ok"}.
    # We should have fallen through to the fetch path instead.
    assert result.get("skipped") is not True
    assert "skipped" not in result or result["skipped"] is not True


def test_is_post_close_write_edt():
    """2026-04-20 is in EDT. NYSE close = 20:00 UTC."""
    run_date = "2026-04-20"
    # 1s before close → pre-close
    assert not daily_closes._is_post_close_write(
        datetime(2026, 4, 20, 19, 59, 59, tzinfo=timezone.utc), run_date
    )
    # Exactly at close → post-close (boundary is inclusive)
    assert daily_closes._is_post_close_write(
        datetime(2026, 4, 20, 20, 0, 0, tzinfo=timezone.utc), run_date
    )
    # 1h after close → post-close
    assert daily_closes._is_post_close_write(
        datetime(2026, 4, 20, 21, 0, 0, tzinfo=timezone.utc), run_date
    )


def test_is_post_close_write_est():
    """2026-01-15 is in EST. NYSE close = 21:00 UTC."""
    run_date = "2026-01-15"
    # 20:00 UTC in EST is 15:00 ET — still pre-close
    assert not daily_closes._is_post_close_write(
        datetime(2026, 1, 15, 20, 0, 0, tzinfo=timezone.utc), run_date
    )
    # 21:00 UTC = 16:00 ET → at close
    assert daily_closes._is_post_close_write(
        datetime(2026, 1, 15, 21, 0, 0, tzinfo=timezone.utc), run_date
    )


def test_missing_object_proceeds_to_fetch():
    """404 on head_object is the expected fresh-day case — proceed to write."""
    from botocore.exceptions import ClientError

    mock_s3 = MagicMock()
    mock_s3.head_object.side_effect = ClientError(
        {"Error": {"Code": "404", "Message": "Not Found"}},
        "HeadObject",
    )

    with patch("collectors.daily_closes.boto3.client", return_value=mock_s3), \
         patch("collectors.daily_closes._fetch_yfinance_closes", return_value=0):
        result = daily_closes.collect(
            bucket="test-bucket",
            tickers=["AAPL"],
            run_date="2026-04-20",
        )

    # Should not have short-circuited as skipped; fell through to fetch path.
    assert result.get("skipped") is not True


def test_head_object_auth_failure_propagates():
    """Non-404 errors on head_object must not silently fall through."""
    from botocore.exceptions import ClientError

    mock_s3 = MagicMock()
    mock_s3.head_object.side_effect = ClientError(
        {"Error": {"Code": "403", "Message": "Forbidden"}},
        "HeadObject",
    )

    with patch("collectors.daily_closes.boto3.client", return_value=mock_s3):
        with pytest.raises(ClientError):
            daily_closes.collect(
                bucket="test-bucket",
                tickers=["AAPL"],
                run_date="2026-04-20",
            )
