"""
Tests for DataPreflight (2026-04-17).

Preflight runs at the START of DataPhase1 to fail-fast on credential drift,
external-API outages, and S3/ArcticDB misconfiguration — before any
collector burns spot-EC2 time. Each check raises ``PreflightError`` with a
specific named message; these tests mock requests + boto3 + arcticdb to
exercise each failure mode deterministically.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from validators.preflight import DataPreflight, PreflightError


BUCKET = "test-bucket"


def _make_preflight() -> DataPreflight:
    return DataPreflight(bucket=BUCKET, phase=1)


# ── Phase gating ─────────────────────────────────────────────────────────────

class TestPhaseGating:
    def test_phase_2_skips(self):
        """Only Phase 1 is gated today."""
        pf = DataPreflight(bucket=BUCKET, phase=2)
        # Should return without raising even with no env vars set
        with patch.dict("os.environ", {}, clear=True):
            pf.run()  # no exception


# ── Required env vars ────────────────────────────────────────────────────────

class TestRequiredEnvVars:
    def test_polygon_missing_raises(self):
        pf = _make_preflight()
        with patch.dict("os.environ", {"FRED_API_KEY": "x"}, clear=True):
            with pytest.raises(PreflightError, match="POLYGON_API_KEY"):
                pf._check_required_env_vars()

    def test_fred_missing_raises(self):
        pf = _make_preflight()
        with patch.dict("os.environ", {"POLYGON_API_KEY": "x"}, clear=True):
            with pytest.raises(PreflightError, match="FRED_API_KEY"):
                pf._check_required_env_vars()

    def test_empty_string_treated_as_missing(self):
        pf = _make_preflight()
        with patch.dict(
            "os.environ",
            {"POLYGON_API_KEY": "  ", "FRED_API_KEY": "y"},
            clear=True,
        ):
            with pytest.raises(PreflightError, match="POLYGON_API_KEY"):
                pf._check_required_env_vars()

    def test_both_present_passes(self):
        pf = _make_preflight()
        with patch.dict(
            "os.environ",
            {"POLYGON_API_KEY": "x", "FRED_API_KEY": "y"},
            clear=True,
        ):
            pf._check_required_env_vars()  # no exception


# ── Polygon reachability ─────────────────────────────────────────────────────

class TestPolygonReachable:
    def _setup(self):
        pf = _make_preflight()
        env_patch = patch.dict("os.environ", {"POLYGON_API_KEY": "fake_key"}, clear=False)
        env_patch.start()
        self.addCleanup_target = env_patch
        return pf

    def teardown_method(self):
        if hasattr(self, "addCleanup_target"):
            self.addCleanup_target.stop()

    def test_200_passes(self):
        pf = self._setup()
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(status_code=200, text='{"ok": true}')
            pf._check_polygon_reachable()  # no exception

    def test_401_auth_error(self):
        pf = self._setup()
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(status_code=401, text="unauthorized")
            with pytest.raises(PreflightError, match="auth failed.*invalid or revoked"):
                pf._check_polygon_reachable()

    def test_403_auth_error(self):
        pf = self._setup()
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(status_code=403, text="forbidden")
            with pytest.raises(PreflightError, match="auth failed"):
                pf._check_polygon_reachable()

    def test_500_outage(self):
        pf = self._setup()
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(status_code=503, text="unavailable")
            with pytest.raises(PreflightError, match="upstream outage"):
                pf._check_polygon_reachable()

    def test_network_error(self):
        pf = self._setup()
        with patch("requests.get") as mock_get:
            mock_get.side_effect = requests.ConnectionError("DNS failure")
            with pytest.raises(PreflightError, match="unreachable"):
                pf._check_polygon_reachable()


# ── FRED reachability ────────────────────────────────────────────────────────

class TestFredReachable:
    def _setup(self):
        pf = _make_preflight()
        env_patch = patch.dict("os.environ", {"FRED_API_KEY": "fake_key"}, clear=False)
        env_patch.start()
        self.addCleanup_target = env_patch
        return pf

    def teardown_method(self):
        if hasattr(self, "addCleanup_target"):
            self.addCleanup_target.stop()

    def test_200_passes(self):
        pf = self._setup()
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(status_code=200, text='{"observations": []}')
            pf._check_fred_reachable()

    def test_400_invalid_api_key(self):
        pf = self._setup()
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(
                status_code=400, text="Bad Request: api_key is invalid"
            )
            with pytest.raises(PreflightError, match="auth failed.*invalid"):
                pf._check_fred_reachable()

    def test_500_outage(self):
        pf = self._setup()
        with patch("requests.get") as mock_get:
            mock_get.return_value = MagicMock(status_code=502, text="bad gateway")
            with pytest.raises(PreflightError, match="upstream outage"):
                pf._check_fred_reachable()

    def test_network_error(self):
        pf = self._setup()
        with patch("requests.get") as mock_get:
            mock_get.side_effect = requests.Timeout("timed out")
            with pytest.raises(PreflightError, match="unreachable"):
                pf._check_fred_reachable()


# ── S3 writeability ──────────────────────────────────────────────────────────

class TestS3Writeable:
    def test_all_ops_succeed(self):
        pf = _make_preflight()
        pf._s3 = MagicMock()
        pf._s3.head_bucket.return_value = {}
        pf._s3.put_object.return_value = {}
        pf._s3.delete_object.return_value = {}
        pf._check_s3_writeable()  # no exception
        # Confirm the sentinel key was both PUT and DELETE'd
        assert pf._s3.put_object.called
        assert pf._s3.delete_object.called

    def test_head_bucket_fails(self):
        pf = _make_preflight()
        pf._s3 = MagicMock()
        pf._s3.head_bucket.side_effect = Exception("AccessDenied")
        with pytest.raises(PreflightError, match="HEAD failed"):
            pf._check_s3_writeable()

    def test_put_object_fails(self):
        pf = _make_preflight()
        pf._s3 = MagicMock()
        pf._s3.head_bucket.return_value = {}
        pf._s3.put_object.side_effect = Exception("PutObject denied")
        with pytest.raises(PreflightError, match="PUT.*failed"):
            pf._check_s3_writeable()

    def test_delete_failure_is_warning_not_fatal(self):
        """DELETE failure logs a WARNING but does not raise — sentinel
        accumulation is benign, and PUT succeeded which proves the
        critical write path works."""
        pf = _make_preflight()
        pf._s3 = MagicMock()
        pf._s3.head_bucket.return_value = {}
        pf._s3.put_object.return_value = {}
        pf._s3.delete_object.side_effect = Exception("DeleteObject denied")
        pf._check_s3_writeable()  # no exception


# ── ArcticDB connectability ──────────────────────────────────────────────────

class TestArcticDbConnectable:
    def test_connection_ok_with_required_libs(self):
        pf = _make_preflight()
        mock_arctic = MagicMock()
        mock_arctic.list_libraries.return_value = ["universe", "macro", "extra"]
        with patch("arcticdb.Arctic", return_value=mock_arctic):
            pf._check_arcticdb_connectable()  # no exception

    def test_connection_failure(self):
        pf = _make_preflight()
        with patch("arcticdb.Arctic", side_effect=Exception("S3 timeout")):
            with pytest.raises(PreflightError, match="connection failed"):
                pf._check_arcticdb_connectable()

    def test_missing_universe_lib(self):
        pf = _make_preflight()
        mock_arctic = MagicMock()
        mock_arctic.list_libraries.return_value = ["macro"]
        with patch("arcticdb.Arctic", return_value=mock_arctic):
            with pytest.raises(PreflightError, match="missing expected libraries.*universe"):
                pf._check_arcticdb_connectable()

    def test_missing_macro_lib(self):
        pf = _make_preflight()
        mock_arctic = MagicMock()
        mock_arctic.list_libraries.return_value = ["universe"]
        with patch("arcticdb.Arctic", return_value=mock_arctic):
            with pytest.raises(PreflightError, match="missing expected libraries.*macro"):
                pf._check_arcticdb_connectable()


# ── End-to-end run() ─────────────────────────────────────────────────────────

class TestRunEndToEnd:
    def test_all_pass(self):
        pf = _make_preflight()
        pf._s3 = MagicMock()
        pf._s3.head_bucket.return_value = {}
        pf._s3.put_object.return_value = {}
        pf._s3.delete_object.return_value = {}

        mock_arctic = MagicMock()
        mock_arctic.list_libraries.return_value = ["universe", "macro"]

        with patch.dict(
            "os.environ",
            {"POLYGON_API_KEY": "k1", "FRED_API_KEY": "k2"},
            clear=False,
        ), patch("arcticdb.Arctic", return_value=mock_arctic), patch(
            "requests.get"
        ) as mock_http:
            mock_http.return_value = MagicMock(status_code=200, text='{"ok": true}')
            pf.run()  # no exception

    def test_first_failure_short_circuits(self):
        """If env vars fail, no HTTP/S3/ArcticDB calls happen."""
        pf = _make_preflight()
        pf._s3 = MagicMock()  # would raise if called

        with patch.dict("os.environ", {}, clear=True), patch(
            "requests.get"
        ) as mock_http, patch("arcticdb.Arctic") as mock_arctic:
            with pytest.raises(PreflightError):
                pf.run()
            mock_http.assert_not_called()
            mock_arctic.assert_not_called()
            pf._s3.head_bucket.assert_not_called()
