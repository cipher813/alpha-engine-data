"""Tests for emailer._extract_details."""

from emailer import _extract_details


class TestExtractDetails:
    def test_prices_collector(self):
        info = {
            "status": "ok",
            "refreshed": 150,
            "stale": 160,
            "failed": 10,
            "total": 900,
            "validation": {
                "total_validated": 150,
                "anomalies": 3,
                "clean": 147,
            },
        }
        details = _extract_details("prices", info)
        assert "150 refreshed" in details
        assert "160 stale" in details
        assert "10 failed" in details
        assert "900 total" in details
        assert "3/150 anomalies" in details

    def test_slim_cache_with_validation(self):
        info = {
            "status": "ok",
            "written": 450,
            "failed": 0,
            "validation": {
                "total_validated": 450,
                "anomalies": 0,
                "clean": 450,
            },
        }
        details = _extract_details("slim_cache", info)
        assert "450 written" in details
        assert "450 validated" in details
        assert "failed" not in details  # 0 failures omitted

    def test_slim_cache_with_anomalies(self):
        info = {
            "status": "partial",
            "written": 448,
            "failed": 2,
            "validation": {
                "total_validated": 448,
                "anomalies": 5,
                "clean": 443,
            },
        }
        details = _extract_details("slim_cache", info)
        assert "448 written" in details
        assert "2 failed" in details
        assert "5/448 anomalies" in details

    def test_empty_info(self):
        assert _extract_details("unknown", {}) == ""

    def test_no_validation(self):
        info = {"status": "ok", "written": 10, "failed": 0}
        details = _extract_details("slim_cache", info)
        assert "10 written" in details
        assert "validated" not in details
