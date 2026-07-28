"""Tests for the overseer backstop responder (alpha-engine-config-I4480).

Tests run WITHOUT live AWS or Telegram — they stub boto3 and urllib at the
function level. The hermetic-import guard applies (no nousergon_lib, no krepis,
no flow_doctor_telegram) — the backstop must stay independent.
"""

from __future__ import annotations

import json
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

# Ensure no nousergon_lib/krepis imports at module level
import index  # noqa: E402


# ═══════════════════════════════════════════════════════════════════════════════
# Unit tests — pure functions
# ═══════════════════════════════════════════════════════════════════════════════

class TestEscapeMarkdown(unittest.TestCase):
    def test_escapes_special_chars(self):
        result = index._escape_markdown("hello_world [test] (yep)")
        self.assertEqual(result, r"hello\_world \[test\] \(yep\)")

    def test_noop_on_clean_text(self):
        result = index._escape_markdown("just plain text 123")
        self.assertEqual(result, "just plain text 123")


class TestAlarmSlug(unittest.TestCase):
    def test_deterministic(self):
        a = index._alarm_slug("test-alarm")
        b = index._alarm_slug("test-alarm")
        self.assertEqual(a, b)
        self.assertEqual(len(a), 16)

    def test_different_alarms_different_slugs(self):
        a = index._alarm_slug("alarm-a")
        b = index._alarm_slug("alarm-b")
        self.assertNotEqual(a, b)


class TestWithinCooldown(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 7, 28, 12, 0, 0, tzinfo=timezone.utc)

    def test_no_prior(self):
        self.assertFalse(index._within_cooldown(None, self.now))

    def test_outside_window(self):
        old = (self.now - timedelta(minutes=61)).isoformat()
        self.assertFalse(index._within_cooldown(old, self.now))

    def test_inside_window(self):
        recent = (self.now - timedelta(minutes=30)).isoformat()
        self.assertTrue(index._within_cooldown(recent, self.now))

    def test_bad_timestamp(self):
        self.assertFalse(index._within_cooldown("not-a-timestamp", self.now))


class TestRecoveryActionsForAlarm(unittest.TestCase):
    def test_intake_age_maps_to_probe_and_redispatch(self):
        actions = index._recovery_actions_for_alarm(
            "alpha-engine-watch-plane-overseer-intake-age"
        )
        self.assertIn("invoke_liveness_probe", actions)
        self.assertIn("redispatch_alert_drain", actions)

    def test_liveness_probe_errors_maps_to_probe_only(self):
        actions = index._recovery_actions_for_alarm(
            "alpha-engine-watch-plane-overseer-liveness-probe-errors"
        )
        self.assertIn("invoke_liveness_probe", actions)
        self.assertNotIn("redispatch_alert_drain", actions)

    def test_unknown_alarm_gets_default(self):
        actions = index._recovery_actions_for_alarm("some-unknown-alarm")
        self.assertEqual(actions, index.DEFAULT_RECOVERY_ACTIONS)


class TestFormatDurationMinutes(unittest.TestCase):
    def test_minutes_only(self):
        self.assertEqual(index._format_duration_minutes(30), "30m")

    def test_hours_only(self):
        self.assertEqual(index._format_duration_minutes(120), "2h")

    def test_hours_and_minutes(self):
        self.assertEqual(index._format_duration_minutes(150), "2h30m")


# ═══════════════════════════════════════════════════════════════════════════════
# Format tests
# ═══════════════════════════════════════════════════════════════════════════════

class TestFormatRecoverySection(unittest.TestCase):
    def test_empty_results(self):
        text = index._format_recovery_section([])
        self.assertEqual(text, "")

    def test_probe_clean(self):
        results = [{
            "action": "invoke_liveness_probe",
            "ok": True,
            "verdict": {
                "problems": [],
                "clean": True,
                "kill_switches": {"ALERT_DRAIN_DISPATCH_ENABLED": "true"},
                "checks_run": 25,
                "checks_failed": 0,
            },
        }]
        text = index._format_recovery_section(results)
        self.assertIn("clean", text)
        self.assertIn("25 checks", text)

    def test_probe_with_problems(self):
        results = [{
            "action": "invoke_liveness_probe",
            "ok": True,
            "verdict": {
                "problems": ["intake queue backed up"],
                "clean": False,
                "kill_switches": {},
                "checks_run": 25,
                "checks_failed": 2,
            },
        }]
        text = index._format_recovery_section(results)
        self.assertIn("problem", text)
        self.assertIn("2 failed", text)

    def test_redispatch_launched(self):
        results = [{
            "action": "redispatch_alert_drain",
            "ok": True,
            "verdict": {
                "routed": True,
                "verdict": {"launched": True, "reason": "dispatched"},
            },
        }]
        text = index._format_recovery_section(results)
        self.assertIn("launched", text.lower())

    def test_redispatch_not_launched(self):
        results = [{
            "action": "redispatch_alert_drain",
            "ok": True,
            "verdict": {
                "routed": True,
                "verdict": {"launched": False, "reason": "concurrent_skip"},
            },
        }]
        text = index._format_recovery_section(results)
        self.assertIn("NOT launched", text)

    def test_action_failed(self):
        results = [{
            "action": "invoke_liveness_probe",
            "ok": False,
            "error": "AccessDenied",
        }]
        text = index._format_recovery_section(results)
        self.assertIn("AccessDenied", text)


class TestFormatStateSection(unittest.TestCase):
    def test_queue_metrics(self):
        state = {
            "intake_queue": {
                "queue": "nousergon-overseer-intake",
                "depth": 362,
                "in_flight": 0,
                "oldest_message_age_minutes": 4320,
                "ok": True,
            },
            "intake_dlq": {"queue": "dlq", "depth": 0, "ok": True},
            "dispatch_ledger": {"prefix": "overseer/dispatch_ledger", "key": None, "ok": True},
            "drain_ledger": {
                "prefix": "overseer/drain_ledger",
                "key": "overseer/drain_ledger/2026-07-23/160000-drain-abc.json",
                "last_modified": "2026-07-23T16:05:00Z",
                "run_start": "2026-07-23T16:00:00Z",
                "ok": True,
            },
            "probe_state": {"ok": True, "healthy": False, "fingerprint": "abc123", "updated_at": "2026-07-27T00:00:00Z"},
        }
        text = index._format_state_section(state)
        self.assertIn("362", text)
        self.assertIn("72h", text)  # 4320 minutes = 72 hours
        self.assertIn("drain", text.lower())

    def test_queue_unreadable(self):
        state = {
            "intake_queue": {"ok": False, "error": "AccessDenied"},
            "intake_dlq": {"ok": False, "error": "AccessDenied"},
            "dispatch_ledger": {"prefix": "x", "ok": False, "error": "blah"},
            "drain_ledger": {"prefix": "x", "ok": False, "error": "blah"},
            "probe_state": {"ok": False, "error": "blah"},
        }
        text = index._format_state_section(state)
        self.assertIn("UNREADABLE", text)

    def test_probe_healthy(self):
        state = {
            "intake_queue": {"depth": 0, "in_flight": 0, "oldest_message_age_minutes": 0, "ok": True},
            "intake_dlq": {"depth": 0, "ok": True},
            "dispatch_ledger": {"prefix": "x", "key": "x/date/file.json", "last_modified": "t", "ok": True},
            "drain_ledger": {"prefix": "x", "key": "x/date/file.json", "last_modified": "t", "ok": True},
            "probe_state": {"ok": True, "healthy": True},
        }
        text = index._format_state_section(state)
        self.assertIn("healthy", text)


class TestFormatEscalationNote(unittest.TestCase):
    def test_escalation(self):
        text = index._format_escalation_note(True)
        self.assertIn("ESCALATED", text)

    def test_no_escalation(self):
        text = index._format_escalation_note(False)
        self.assertEqual(text, "")


class TestFormatPage(unittest.TestCase):
    def setUp(self):
        self.alarm = {
            "AlarmName": "test-alarm",
            "OldStateValue": "OK",
            "NewStateValue": "ALARM",
            "NewStateReason": "Threshold Crossed: 1 >= 1",
            "Region": "us-east-1",
            "Trigger": {
                "MetricName": "ApproximateAgeOfOldestMessage",
                "Namespace": "AWS/SQS",
            },
        }
        self.state = {
            "intake_queue": {"depth": 10, "in_flight": 0, "oldest_message_age_minutes": 120, "ok": True},
            "intake_dlq": {"depth": 0, "ok": True},
            "dispatch_ledger": {"prefix": "x", "key": None, "ok": True},
            "drain_ledger": {"prefix": "x", "key": "x/file.json", "last_modified": "t", "ok": True},
            "probe_state": {"ok": True, "healthy": True},
        }
        self.recovery = [{
            "action": "invoke_liveness_probe",
            "ok": True,
            "verdict": {"problems": [], "clean": True, "kill_switches": {}, "checks_run": 25, "checks_failed": 0},
        }]

    def test_full_page(self):
        text = index._format_page(self.alarm, self.state, self.recovery, False, "_cooldown info_")
        self.assertIn("BACKSTOP", text)
        self.assertIn("test-alarm", text)
        self.assertIn("Fleet state", text)
        self.assertIn("Recovery attempt", text)
        self.assertIn("AWS Console", text)

    def test_escalation_page(self):
        text = index._format_page(self.alarm, self.state, self.recovery, True, "_cooldown info_")
        self.assertIn("ESCALATION", text)

    def test_long_reason_truncated(self):
        alarm = dict(self.alarm)
        alarm["NewStateReason"] = "x" * 1000
        text = index._format_page(alarm, self.state, [], False, "_ok_")
        self.assertLess(len(text), 3000)  # well under Telegram's 4096

    def test_no_recovery_actions(self):
        text = index._format_page(self.alarm, self.state, [], False, "_ok_")
        self.assertIn("No recovery actions configured", text)


# ═══════════════════════════════════════════════════════════════════════════════
# Handler integration tests (stubbed AWS + Telegram)
# ═══════════════════════════════════════════════════════════════════════════════

class TestHandler(unittest.TestCase):
    """Full handler tests with stubbed boto3 + urllib."""

    def _make_alarm_event(self, alarm_name="test-alarm", state="ALARM"):
        return {
            "Records": [{
                "Sns": {
                    "MessageId": "test-msg-1",
                    "Message": json.dumps({
                        "AlarmName": alarm_name,
                        "OldStateValue": "OK",
                        "NewStateValue": state,
                        "NewStateReason": "Threshold crossed",
                        "Region": "us-east-1",
                        "Trigger": {"MetricName": "Errors", "Namespace": "AWS/Lambda"},
                    }),
                }
            }]
        }

    @patch("index.urllib.request.urlopen")
    @patch("index.boto3.client")
    def test_handler_sends_enhanced_alarm(self, mock_boto, mock_urlopen):
        # SSM returns Telegram creds
        ssm_mock = MagicMock()
        ssm_mock.get_parameter.side_effect = [
            {"Parameter": {"Value": "fake-token"}},
            {"Parameter": {"Value": "12345"}},
        ]
        # S3 returns no cooldown state (NoSuchKey)
        s3_mock = MagicMock()
        s3_mock.get_object.side_effect = Exception("NoSuchKey")
        # SQS returns queue metrics
        sqs_mock = MagicMock()
        sqs_mock.get_queue_url.return_value = {"QueueUrl": "https://sqs.test/q"}
        sqs_mock.get_queue_attributes.return_value = {
            "Attributes": {
                "ApproximateNumberOfMessages": "362",
                "ApproximateNumberOfMessagesNotVisible": "0",
                "ApproximateAgeOfOldestMessage": "259200",
            }
        }
        # Lambda invoke returns probe verdict
        lam_mock = MagicMock()
        probe_payload = MagicMock()
        probe_payload.read.return_value = json.dumps({
            "problems": [],
            "clean": True,
            "kill_switches": {"ALERT_DRAIN_DISPATCH_ENABLED": "true"},
            "checks_run": 25,
            "checks_failed": 0,
        }).encode("utf-8")
        lam_mock.invoke.return_value = {
            "Payload": probe_payload,
        }

        def boto_client(service, **kwargs):
            if service == "ssm":
                return ssm_mock
            if service == "s3":
                return s3_mock
            if service == "sqs":
                return sqs_mock
            if service == "lambda":
                return lam_mock
            return MagicMock()

        mock_boto.side_effect = boto_client

        # Telegram send succeeds
        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"ok": true, "result": {"message_id": 1}}'
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        event = self._make_alarm_event()
        result = index.handler(event, None)

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["sent"], 1)
        self.assertEqual(result["results"][0]["status"], "sent")
        # Recovery should have been attempted
        self.assertTrue(result["results"][0]["recovery_attempted"])

    @patch("index.urllib.request.urlopen")
    @patch("index.boto3.client")
    def test_handler_skips_non_alarm(self, mock_boto, mock_urlopen):
        ssm_mock = MagicMock()
        ssm_mock.get_parameter.side_effect = [
            {"Parameter": {"Value": "fake-token"}},
            {"Parameter": {"Value": "12345"}},
        ]
        mock_boto.return_value = ssm_mock

        event = {
            "Records": [{
                "Sns": {
                    "MessageId": "test-2",
                    "Message": json.dumps({"notification": "something-else"}),
                }
            }]
        }
        result = index.handler(event, None)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["sent"], 0)
        self.assertEqual(result["results"][0]["status"], "skipped")

    @patch("index.urllib.request.urlopen")
    @patch("index.boto3.client")
    def test_handler_ok_state_skips_recovery(self, mock_boto, mock_urlopen):
        ssm_mock = MagicMock()
        ssm_mock.get_parameter.side_effect = [
            {"Parameter": {"Value": "fake-token"}},
            {"Parameter": {"Value": "12345"}},
        ]
        # S3 returns prior cooldown state
        s3_mock = MagicMock()
        s3_mock.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps({
                "last_fired_at": "2026-07-28T11:00:00+00:00",
                "recovery_attempted": True,
            }).encode("utf-8")))
        }
        mock_boto.side_effect = lambda service, **kw: {
            "ssm": ssm_mock,
            "s3": s3_mock,
        }.get(service, MagicMock())

        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"ok": true}'
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        # OK state — should send but NOT attempt recovery
        event = self._make_alarm_event(state="OK")
        result = index.handler(event, None)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["sent"], 1)
        self.assertFalse(result["results"][0]["recovery_attempted"])

    @patch("index.urllib.request.urlopen")
    @patch("index.boto3.client")
    def test_handler_ssm_failure(self, mock_boto, mock_urlopen):
        ssm_mock = MagicMock()
        ssm_mock.get_parameter.side_effect = Exception("SSM down")
        mock_boto.return_value = ssm_mock

        event = self._make_alarm_event()
        result = index.handler(event, None)
        self.assertEqual(result["status"], "error")
        self.assertIn("SSM", result["reason"])

    @patch("index.urllib.request.urlopen")
    @patch("index.boto3.client")
    def test_handler_second_occurrence_escalates(self, mock_boto, mock_urlopen):
        """Second firing within cooldown with prior recovery → escalated, no retry."""
        ssm_mock = MagicMock()
        ssm_mock.get_parameter.side_effect = [
            {"Parameter": {"Value": "fake-token"}},
            {"Parameter": {"Value": "12345"}},
        ]

        # S3: prior state shows recovery already attempted within cooldown
        s3_mock = MagicMock()
        recent = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        s3_mock.get_object.return_value = {
            "Body": MagicMock(read=MagicMock(return_value=json.dumps({
                "last_fired_at": recent,
                "recovery_attempted": True,
                "occurrence": 1,
            }).encode("utf-8")))
        }

        # SQS for state gathering
        sqs_mock = MagicMock()
        sqs_mock.get_queue_url.return_value = {"QueueUrl": "https://sqs.test/q"}
        sqs_mock.get_queue_attributes.return_value = {
            "Attributes": {
                "ApproximateNumberOfMessages": "100",
                "ApproximateNumberOfMessagesNotVisible": "0",
                "ApproximateAgeOfOldestMessage": "3600",
            }
        }

        def boto_client(service, **kwargs):
            return {"ssm": ssm_mock, "s3": s3_mock, "sqs": sqs_mock}.get(
                service, MagicMock()
            )

        mock_boto.side_effect = boto_client

        mock_resp = MagicMock()
        mock_resp.read.return_value = b'{"ok": true}'
        mock_urlopen.return_value.__enter__.return_value = mock_resp

        event = self._make_alarm_event()
        result = index.handler(event, None)

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["sent"], 1)
        # Should be escalated, NOT recovered
        self.assertTrue(result["results"][0]["escalated"])
        self.assertFalse(result["results"][0]["recovery_attempted"])


# ═══════════════════════════════════════════════════════════════════════════════
# Hermetic import guard
# ═══════════════════════════════════════════════════════════════════════════════

class TestHermeticImportGuard(unittest.TestCase):
    """The handler must NOT import nousergon_lib, krepis, or flow_doctor_telegram
    — sharing code with the smart path would violate backstop independence
    (overseer-policy §4 invariant 3)."""

    def test_no_forbidden_imports_in_index(self):
        import ast

        with open(os.path.join(os.path.dirname(__file__), "index.py")) as f:
            tree = ast.parse(f.read())

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    name = alias.name.split(".")[0]
                    self.assertNotIn(
                        name,
                        ["nousergon_lib", "krepis", "flow_doctor_telegram"],
                        f"index.py must not import {name} (violates backstop independence)",
                    )
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    base = node.module.split(".")[0]
                    self.assertNotIn(
                        base,
                        ["nousergon_lib", "krepis", "flow_doctor_telegram"],
                        f"index.py must not import from {node.module} (violates backstop independence)",
                    )


if __name__ == "__main__":
    unittest.main()
