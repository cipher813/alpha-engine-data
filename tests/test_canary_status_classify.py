"""tests/test_canary_status_classify.py

Covers infrastructure/canary_status.py — the classifier deploy.sh's
post-deploy canary uses to decide roll-forward vs. roll-back.

Root incident (alpha-engine-config-I7855): alpha-engine-data-collector's
v341 canary invoke crashed at Lambda cold-start (a ConfigError raised at
module import). The invoke response was Lambda's OWN native error envelope
(errorMessage/errorType/stackTrace) — not this handler's status/statusCode
contract at all — and the old inline parser fell through every branch to
the literal "UNKNOWN", discarding the real cause. deploy.sh's operator (and
any unattended reader of its output) saw "Canary returned 'UNKNOWN'" instead
of the actual ConfigError text that was sitting in CloudWatch the whole time.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "infrastructure"))

from canary_status import classify  # noqa: E402


class TestHandlerContract:
    """The handler's own status/statusCode contract, unchanged from before."""

    def test_status_ok_passes(self):
        assert classify({"status": "OK"}) == "OK"

    def test_status_skipped_passes(self):
        assert classify({"status": "SKIPPED"}) == "SKIPPED"

    def test_status_code_500_is_env_error(self):
        assert classify({"statusCode": 500, "body": "boom"}) == "ENV_ERROR"

    def test_free_form_error_field_surfaced(self):
        assert classify({"status": "FAILED", "error": "quota exceeded"}) == "quota exceeded"

    def test_no_recognizable_shape_is_unknown(self):
        assert classify({"status": "FAILED"}) == "UNKNOWN"

    def test_empty_payload_is_unknown(self):
        assert classify({}) == "UNKNOWN"


class TestLambdaNativeErrorEnvelope:
    """The class of bug this file exists to close: an init-time crash
    returns Lambda's own error shape, which the handler's contract never
    defines and the old parser silently swallowed as 'UNKNOWN'.
    """

    def test_init_time_config_error_surfaced_verbatim(self):
        # Mirrors the actual v341 payload shape (errorType/errorMessage,
        # stackTrace present but irrelevant to classification).
        payload = {
            "errorMessage": (
                "diagnosis.enabled=True requires diagnosis.provider to be "
                "set explicitly — flow-doctor has no default LLM vendor."
            ),
            "errorType": "ConfigError",
            "stackTrace": ["  File \"/var/task/handler.py\", line 43, in <module>"],
        }
        result = classify(payload)
        assert result != "UNKNOWN"
        assert "ConfigError" in result
        assert "diagnosis.provider" in result

    def test_error_type_present_without_message(self):
        result = classify({"errorType": "Runtime.ExitError"})
        assert result.startswith("Runtime.ExitError")

    def test_error_message_present_without_type(self):
        result = classify({"errorMessage": "boom"})
        assert "UnknownError" in result
        assert "boom" in result

    def test_native_envelope_takes_priority_over_free_form_error(self):
        # A response can't legally carry both in practice, but the native
        # envelope is Lambda's own signal that init/execution crashed
        # outside the handler's control — it must win if somehow present
        # alongside a stale/unrelated 'error' key.
        payload = {"errorType": "Runtime.ExitError", "errorMessage": "crash", "error": "stale"}
        assert classify(payload) == "Runtime.ExitError: crash"


class TestCLIEntrypoint:
    def test_main_parses_file_and_prints_classification(self, tmp_path, capsys):
        import json
        from canary_status import main

        f = tmp_path / "response.json"
        f.write_text(json.dumps({"status": "OK"}))
        sys.argv = ["canary_status.py", str(f)]
        rc = main()
        assert rc == 0
        out = capsys.readouterr().out.strip()
        assert out == "OK"

    def test_main_unparseable_file_prints_parse_error(self, tmp_path, capsys):
        from canary_status import main

        f = tmp_path / "bad.json"
        f.write_text("not json")
        sys.argv = ["canary_status.py", str(f)]
        rc = main()
        assert rc == 0
        out = capsys.readouterr().out.strip()
        assert out == "PARSE_ERROR"
