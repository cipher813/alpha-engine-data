"""Unit tests for eod_artifact_verification.py (alpha-engine-config#5289)."""

from __future__ import annotations

import io
from unittest.mock import MagicMock

import eod_artifact_verification as m
from eod_artifact_verification import (
    COMPLETION_MARKER_KEY_TEMPLATE,
    EOD_PNL_CSV_KEY,
    EodArtifactStatus,
    S3_BUCKET,
    format_eod_artifact_lines,
    verify_eod_artifacts,
)


class _NotFound(Exception):
    def __init__(self):
        self.response = {
            "Error": {"Code": "404"},
            "ResponseMetadata": {"HTTPStatusCode": 404},
        }


class _Throttled(Exception):
    def __init__(self):
        self.response = {
            "Error": {"Code": "ThrottlingException"},
            "ResponseMetadata": {"HTTPStatusCode": 400},
        }


def _s3_with(*, head_ok=True, csv_body: bytes | None = None, get_error: Exception | None = None):
    s3 = MagicMock()
    if head_ok:
        s3.head_object.return_value = {}
    else:
        s3.head_object.side_effect = _NotFound()
    if get_error is not None:
        s3.get_object.side_effect = get_error
    else:
        s3.get_object.return_value = {"Body": io.BytesIO(csv_body or b"date\n")}
    return s3


def test_verify_returns_none_when_run_date_unresolved():
    assert verify_eod_artifacts(MagicMock(), None) is None


def test_both_artifacts_present():
    s3 = _s3_with(head_ok=True, csv_body=b"date,nav\n2026-08-08,100000\n")
    status = verify_eod_artifacts(s3, "2026-08-08")
    assert status.all_present is True
    s3.head_object.assert_called_once_with(
        Bucket=S3_BUCKET,
        Key=COMPLETION_MARKER_KEY_TEMPLATE.format(run_date="2026-08-08"),
    )
    s3.get_object.assert_called_once_with(Bucket=S3_BUCKET, Key=EOD_PNL_CSV_KEY)


def test_marker_missing_pnl_present():
    s3 = _s3_with(head_ok=False, csv_body=b"date,nav\n2026-08-08,100000\n")
    status = verify_eod_artifacts(s3, "2026-08-08")
    assert status.completion_marker_present is False
    assert status.pnl_row_present is True
    assert status.all_present is False


def test_pnl_row_missing_for_this_date_even_though_csv_has_other_dates():
    """The core failure mode alpha-engine-config#5289 exists to catch: a
    SUCCEEDED terminal whose eod_pnl.csv has rows for OTHER days but not this
    one must report the row absent, not merely "the file exists"."""
    s3 = _s3_with(head_ok=True, csv_body=b"date,nav\n2026-08-07,100000\n2026-08-06,99000\n")
    status = verify_eod_artifacts(s3, "2026-08-08")
    assert status.completion_marker_present is True
    assert status.pnl_row_present is False


def test_csv_missing_date_column_reports_absent():
    s3 = _s3_with(head_ok=True, csv_body=b"nav,cash\n100000,5000\n")
    status = verify_eod_artifacts(s3, "2026-08-08")
    assert status.pnl_row_present is False


def test_get_object_not_found_reports_absent():
    s3 = _s3_with(head_ok=True, get_error=_NotFound())
    status = verify_eod_artifacts(s3, "2026-08-08")
    assert status.pnl_row_present is False


def test_get_object_non_404_error_fails_toward_absent_not_raise():
    s3 = _s3_with(head_ok=True, get_error=_Throttled())
    status = verify_eod_artifacts(s3, "2026-08-08")
    assert status.pnl_row_present is False


def test_head_object_non_404_error_fails_toward_absent_not_raise():
    s3 = MagicMock()
    s3.head_object.side_effect = _Throttled()
    s3.get_object.return_value = {"Body": io.BytesIO(b"date\n2026-08-08\n")}
    status = verify_eod_artifacts(s3, "2026-08-08")
    assert status.completion_marker_present is False


def test_oversized_csv_reports_absent_without_parsing(monkeypatch):
    s3 = MagicMock()
    s3.head_object.return_value = {}
    body = b"date\n2026-08-08\n"
    s3.get_object.return_value = {"Body": io.BytesIO(body)}
    monkeypatch.setattr(m, "_MAX_CSV_BYTES", len(body) - 1)
    status = verify_eod_artifacts(s3, "2026-08-08")
    assert status.pnl_row_present is False


def test_format_lines_healthy_is_one_line():
    status = EodArtifactStatus("2026-08-08", True, True)
    lines = format_eod_artifact_lines(status)
    assert len(lines) == 1
    assert "✓" in lines[0]
    assert "2026-08-08" in lines[0]


def test_format_lines_missing_pnl_row_is_loud_and_names_it():
    status = EodArtifactStatus("2026-08-08", True, False)
    lines = format_eod_artifact_lines(status)
    assert any("ARTIFACT(S) MISSING" in line for line in lines)
    assert any("eod_pnl.csv row for 2026-08-08" in line for line in lines)
    assert not any("_sf_completion marker" in line for line in lines)


def test_format_lines_missing_marker_names_it():
    status = EodArtifactStatus("2026-08-08", False, True)
    lines = format_eod_artifact_lines(status)
    assert any("_sf_completion marker for 2026-08-08" in line for line in lines)
    assert not any("eod_pnl.csv row" in line for line in lines)


def test_format_lines_both_missing_names_both():
    status = EodArtifactStatus("2026-08-08", False, False)
    lines = format_eod_artifact_lines(status)
    joined = "\n".join(lines)
    assert "_sf_completion marker for 2026-08-08" in joined
    assert "eod_pnl.csv row for 2026-08-08" in joined


def test_format_lines_none_status_is_unverified():
    lines = format_eod_artifact_lines(None)
    assert any("ARTIFACTS UNVERIFIED" in line for line in lines)
