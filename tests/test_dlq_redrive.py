"""Unit tests for infrastructure/dlq_redrive.py (alpha-engine-config-I8111).

Mocks the boto3 SQS client directly (no moto, no real AWS calls) — mirrors
the fleet's existing lambda test convention. Module is loaded by path, same
as tests/test_automation_pause.py does for infrastructure/automation_pause.py.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "infrastructure" / "dlq_redrive.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("dlq_redrive", MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules["dlq_redrive"] = mod
    spec.loader.exec_module(mod)
    return mod


dlq_redrive = _load_module()

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/711398986525/nousergon-overseer-intake"
DLQ_URL = "https://sqs.us-east-1.amazonaws.com/711398986525/nousergon-overseer-intake-dlq"
QUEUE_ARN = "arn:aws:sqs:us-east-1:711398986525:nousergon-overseer-intake"
DLQ_ARN = "arn:aws:sqs:us-east-1:711398986525:nousergon-overseer-intake-dlq"


def _client_error(op: str, code: str = "SomeError", msg: str = "boom") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": msg}}, op)


def _make_sqs(*, dlq_count=0, active_tasks=None, start_side_effect=None):
    sqs = MagicMock()

    def get_queue_url(QueueName):  # noqa: N803
        return {"QueueUrl": DLQ_URL if "dlq" in QueueName else QUEUE_URL}

    def get_queue_attributes(QueueUrl, AttributeNames):  # noqa: N803
        if "QueueArn" in AttributeNames:
            arn = DLQ_ARN if QueueUrl == DLQ_URL else QUEUE_ARN
            return {"Attributes": {"QueueArn": arn}}
        return {"Attributes": {"ApproximateNumberOfMessages": str(dlq_count)}}

    sqs.get_queue_url.side_effect = get_queue_url
    sqs.get_queue_attributes.side_effect = get_queue_attributes
    sqs.list_message_move_tasks.return_value = {"Results": active_tasks or []}
    if start_side_effect is not None:
        sqs.start_message_move_task.side_effect = start_side_effect
    else:
        sqs.start_message_move_task.return_value = {"TaskHandle": "task-1"}
    return sqs


def test_redrive_starts_move_task_when_dlq_nonempty():
    sqs = _make_sqs(dlq_count=93)
    result = dlq_redrive.redrive(sqs)
    assert result["action"] == "started"
    assert result["dlq_depth_at_start"] == 93
    sqs.start_message_move_task.assert_called_once()
    call_kwargs = sqs.start_message_move_task.call_args.kwargs
    assert call_kwargs["SourceArn"] == DLQ_ARN
    assert call_kwargs["DestinationArn"] == QUEUE_ARN


def test_redrive_skips_when_dlq_empty():
    sqs = _make_sqs(dlq_count=0)
    result = dlq_redrive.redrive(sqs)
    assert result == {"action": "skip", "reason": "dlq-empty", "dlq_depth": 0}
    sqs.start_message_move_task.assert_not_called()


def test_redrive_is_idempotent_when_task_already_running():
    active = [{
        "Status": "RUNNING",
        "ApproximateNumberOfMessagesMoved": 40,
        "ApproximateNumberOfMessagesToMove": 93,
    }]
    sqs = _make_sqs(dlq_count=93, active_tasks=active)
    result = dlq_redrive.redrive(sqs)
    assert result["action"] == "poll"
    assert result["status"] == "RUNNING"
    assert result["messages_moved"] == 40
    assert result["messages_to_move"] == 93
    sqs.start_message_move_task.assert_not_called()


def test_redrive_ignores_completed_tasks_and_starts_a_new_one():
    completed = [{"Status": "COMPLETED", "ApproximateNumberOfMessagesMoved": 93}]
    sqs = _make_sqs(dlq_count=5, active_tasks=completed)
    result = dlq_redrive.redrive(sqs)
    assert result["action"] == "started"
    sqs.start_message_move_task.assert_called_once()


def test_redrive_treats_concurrent_start_race_as_running():
    sqs = _make_sqs(dlq_count=5, start_side_effect=_client_error(
        "StartMessageMoveTask", msg="A message move task is already in progress"
    ))
    result = dlq_redrive.redrive(sqs)
    assert result["action"] == "started"
    assert result["task_handle"] is None


def test_redrive_raises_on_unexpected_start_failure():
    sqs = _make_sqs(dlq_count=5, start_side_effect=_client_error(
        "StartMessageMoveTask", code="AccessDenied", msg="denied"
    ))
    with pytest.raises(dlq_redrive.DlqRedriveError):
        dlq_redrive.redrive(sqs)


def test_redrive_raises_on_list_move_tasks_failure():
    sqs = _make_sqs(dlq_count=5)
    sqs.list_message_move_tasks.side_effect = _client_error("ListMessageMoveTasks")
    with pytest.raises(dlq_redrive.DlqRedriveError):
        dlq_redrive.redrive(sqs)


def test_redrive_respects_max_per_second_arg():
    sqs = _make_sqs(dlq_count=5)
    dlq_redrive.redrive(sqs, max_per_second=10)
    call_kwargs = sqs.start_message_move_task.call_args.kwargs
    assert call_kwargs["MaxNumberOfMessagesPerSecond"] == 10


def test_redrive_uses_custom_queue_names():
    sqs = _make_sqs(dlq_count=5)
    dlq_redrive.redrive(sqs, dlq_name="custom-dlq", dest_name="custom-dest")
    calls = [c.kwargs["QueueName"] for c in sqs.get_queue_url.call_args_list]
    assert "custom-dlq" in calls
    assert "custom-dest" in calls


def test_main_cli_invokes_redrive(monkeypatch, capsys):
    sqs = _make_sqs(dlq_count=0)
    monkeypatch.setattr(dlq_redrive.boto3, "client", lambda *a, **k: sqs)
    rc = dlq_redrive.main(["--dlq-name", "nousergon-overseer-intake-dlq"])
    assert rc == 0
    out = capsys.readouterr().out
    assert '"action": "skip"' in out
