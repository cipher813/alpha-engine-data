"""Unit tests for infrastructure/reactivate_paused_lane.py
(alpha-engine-config-I8111 deliverable 1 / alpha-engine-config-I8120 ruling).

`infrastructure/` is put on sys.path so `reactivate_paused_lane.py`'s own
`from dlq_redrive import ...` resolves to the real module — both live in
that directory and are not a package.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

REPO_ROOT = Path(__file__).resolve().parents[1]
INFRA = REPO_ROOT / "infrastructure"
if str(INFRA) not in sys.path:
    sys.path.insert(0, str(INFRA))


def _load_module():
    spec = importlib.util.spec_from_file_location(
        "reactivate_paused_lane", INFRA / "reactivate_paused_lane.py"
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules["reactivate_paused_lane"] = mod
    spec.loader.exec_module(mod)
    return mod


rpl = _load_module()


def _client_error(op: str, code: str = "SomeError", msg: str = "boom") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": msg}}, op)


def _make_sqs(dlq_count=0):
    sqs = MagicMock()
    queue_url = "https://sqs.us-east-1.amazonaws.com/711398986525/nousergon-overseer-intake"
    dlq_url = "https://sqs.us-east-1.amazonaws.com/711398986525/nousergon-overseer-intake-dlq"

    def get_queue_url(QueueName):  # noqa: N803
        return {"QueueUrl": dlq_url if "dlq" in QueueName else queue_url}

    def get_queue_attributes(QueueUrl, AttributeNames):  # noqa: N803
        if "QueueArn" in AttributeNames:
            arn = "arn:aws:sqs:us-east-1:711398986525:" + (
                "nousergon-overseer-intake-dlq" if QueueUrl == dlq_url else "nousergon-overseer-intake"
            )
            return {"Attributes": {"QueueArn": arn}}
        return {"Attributes": {"ApproximateNumberOfMessages": str(dlq_count)}}

    sqs.get_queue_url.side_effect = get_queue_url
    sqs.get_queue_attributes.side_effect = get_queue_attributes
    sqs.list_message_move_tasks.return_value = {"Results": []}
    sqs.start_message_move_task.return_value = {"TaskHandle": "task-1"}
    return sqs


def _make_scheduler(spec_by_name=None):
    scheduler = MagicMock()
    spec_by_name = spec_by_name or {}

    def get_schedule(Name):  # noqa: N803
        if Name not in spec_by_name:
            raise _client_error("GetSchedule", code="ResourceNotFoundException")
        return dict(spec_by_name[Name])

    scheduler.get_schedule.side_effect = get_schedule
    return scheduler


DEFAULT_SPEC = {
    "GroupName": "default",
    "ScheduleExpression": "cron(0 4 * * ? *)",
    "State": "DISABLED",
    "FlexibleTimeWindow": {"Mode": "OFF"},
    "Target": {"Arn": "arn:aws:lambda:us-east-1:711398986525:function:x", "RoleArn": "arn:role"},
    "Arn": "arn:aws:scheduler:us-east-1:711398986525:schedule/default/x",
    "CreationDate": "2026-01-01T00:00:00Z",
    "LastModificationDate": "2026-01-01T00:00:00Z",
}


def test_reactivate_redrives_before_enabling_triggers():
    sqs = _make_sqs(dlq_count=93)
    scheduler = _make_scheduler({"alpha-engine-alert-drain-0400utc": DEFAULT_SPEC})
    events = MagicMock()
    calls = []
    sqs.start_message_move_task.side_effect = lambda **kw: (calls.append("redrive"), {"TaskHandle": "t"})[1]
    scheduler.update_schedule.side_effect = lambda **kw: calls.append("enable")

    result = rpl.reactivate(
        sqs, scheduler, events,
        schedules=("alpha-engine-alert-drain-0400utc",),
    )
    assert calls == ["redrive", "enable"], "redrive must happen BEFORE any trigger is enabled"
    assert result["redrive"]["action"] == "started"
    assert result["enabled"] == ["alpha-engine-alert-drain-0400utc"]


def test_reactivate_round_trips_the_full_schedule_spec():
    sqs = _make_sqs(dlq_count=0)
    scheduler = _make_scheduler({"alpha-engine-alert-drain-1000utc": DEFAULT_SPEC})
    events = MagicMock()

    rpl.reactivate(sqs, scheduler, events, schedules=("alpha-engine-alert-drain-1000utc",))

    scheduler.update_schedule.assert_called_once()
    kwargs = scheduler.update_schedule.call_args.kwargs
    assert kwargs["State"] == "ENABLED"
    assert kwargs["ScheduleExpression"] == DEFAULT_SPEC["ScheduleExpression"]
    assert kwargs["Target"] == DEFAULT_SPEC["Target"]
    # Derived/read-only fields must not be sent back to UpdateSchedule.
    for derived in ("Arn", "CreationDate", "LastModificationDate", "ResponseMetadata"):
        assert derived not in kwargs


def test_reactivate_enables_events_rules_too():
    sqs = _make_sqs(dlq_count=0)
    scheduler = _make_scheduler()
    events = MagicMock()

    result = rpl.reactivate(sqs, scheduler, events, events_rules=("some-events-rule",))

    events.enable_rule.assert_called_once_with(Name="some-events-rule")
    assert result["enabled"] == ["some-events-rule"]


def test_reactivate_all_four_default_alert_drain_schedules():
    sqs = _make_sqs(dlq_count=93)
    scheduler = _make_scheduler({n: DEFAULT_SPEC for n in rpl.DEFAULT_SCHEDULES})
    events = MagicMock()

    result = rpl.reactivate(sqs, scheduler, events, schedules=rpl.DEFAULT_SCHEDULES)

    assert sorted(result["enabled"]) == sorted(rpl.DEFAULT_SCHEDULES)
    assert scheduler.update_schedule.call_count == 4


def test_reactivate_raises_and_touches_no_trigger_when_redrive_fails():
    sqs = _make_sqs(dlq_count=5)
    sqs.list_message_move_tasks.side_effect = _client_error("ListMessageMoveTasks")
    scheduler = _make_scheduler({"alpha-engine-alert-drain-0400utc": DEFAULT_SPEC})
    events = MagicMock()

    with pytest.raises(rpl.ReactivationError, match="DLQ redrive failed"):
        rpl.reactivate(sqs, scheduler, events, schedules=("alpha-engine-alert-drain-0400utc",))
    scheduler.get_schedule.assert_not_called()
    scheduler.update_schedule.assert_not_called()


def test_reactivate_collects_per_trigger_failures_and_still_raises():
    sqs = _make_sqs(dlq_count=0)
    scheduler = _make_scheduler({
        "alpha-engine-alert-drain-0400utc": DEFAULT_SPEC,
        # 1000utc deliberately absent -> get_schedule raises ResourceNotFoundException
    })
    events = MagicMock()

    with pytest.raises(rpl.ReactivationError, match="alpha-engine-alert-drain-1000utc"):
        rpl.reactivate(
            sqs, scheduler, events,
            schedules=("alpha-engine-alert-drain-0400utc", "alpha-engine-alert-drain-1000utc"),
        )
    # The good one still got enabled — one bad name does not block the rest.
    scheduler.update_schedule.assert_called_once()


def test_reactivate_raises_when_given_no_triggers_at_all():
    sqs = _make_sqs(dlq_count=0)
    scheduler = _make_scheduler()
    events = MagicMock()
    with pytest.raises(rpl.ReactivationError, match="nothing to reactivate"):
        rpl.reactivate(sqs, scheduler, events, schedules=(), events_rules=())


def test_main_cli_exits_nonzero_on_failure(monkeypatch, capsys):
    sqs = _make_sqs(dlq_count=0)
    sqs.list_message_move_tasks.side_effect = _client_error("ListMessageMoveTasks")
    scheduler = _make_scheduler()
    events = MagicMock()
    monkeypatch.setattr(
        rpl.boto3, "client",
        lambda name, region_name=None: {"sqs": sqs, "scheduler": scheduler, "events": events}[name],
    )
    rc = rpl.main(["--schedules", "alpha-engine-alert-drain-0400utc"])
    assert rc == 1
    assert "REACTIVATION FAILED" in capsys.readouterr().err
