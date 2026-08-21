"""Unit tests for dlq-redrive-monitor/index.py (alpha-engine-config-I8111).

Mocks the boto3 SQS client directly (no moto — mirrors the fleet's existing
lambda test convention, e.g. alert-drain-liveness-probe/test_handler.py) and
stubs krepis.alerts.publish so no real SNS/Telegram call is attempted.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).parent))
import index  # noqa: E402

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/711398986525/nousergon-overseer-intake"
DLQ_URL = "https://sqs.us-east-1.amazonaws.com/711398986525/nousergon-overseer-intake-dlq"
QUEUE_ARN = "arn:aws:sqs:us-east-1:711398986525:nousergon-overseer-intake"
DLQ_ARN = "arn:aws:sqs:us-east-1:711398986525:nousergon-overseer-intake-dlq"


def _client_error(op: str, code: str = "SomeError", msg: str = "boom") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": msg}}, op)


def _make_sqs(*, dlq_count=0, dlq_age=0, active_tasks=None, start_side_effect=None):
    sqs = MagicMock()

    def get_queue_url(QueueName):  # noqa: N803
        return {"QueueUrl": DLQ_URL if "dlq" in QueueName else QUEUE_URL}

    def get_queue_attributes(QueueUrl, AttributeNames):  # noqa: N803
        if "QueueArn" in AttributeNames:
            arn = DLQ_ARN if QueueUrl == DLQ_URL else QUEUE_ARN
            return {"Attributes": {"QueueArn": arn}}
        attrs = {"ApproximateNumberOfMessages": str(dlq_count)}
        if dlq_count > 0:
            attrs["ApproximateAgeOfOldestMessage"] = str(dlq_age)
        return {"Attributes": attrs}

    sqs.get_queue_url.side_effect = get_queue_url
    sqs.get_queue_attributes.side_effect = get_queue_attributes
    sqs.list_message_move_tasks.return_value = {"Results": active_tasks or []}
    if start_side_effect is not None:
        sqs.start_message_move_task.side_effect = start_side_effect
    else:
        sqs.start_message_move_task.return_value = {"TaskHandle": "task-1"}
    return sqs


@pytest.fixture(autouse=True)
def stub_publish():
    with patch("index.publish") as mock_publish:
        yield mock_publish


# ── run_redrive ──────────────────────────────────────────────────────────


def test_redrive_starts_move_task_when_dlq_nonempty():
    sqs = _make_sqs(dlq_count=93, dlq_age=786240)
    result = index.run_redrive(sqs)
    assert result["action"] == "started"
    assert result["dlq_depth_at_start"] == 93
    sqs.start_message_move_task.assert_called_once()
    call_kwargs = sqs.start_message_move_task.call_args.kwargs
    assert call_kwargs["SourceArn"] == DLQ_ARN
    assert call_kwargs["DestinationArn"] == QUEUE_ARN


def test_redrive_skips_when_dlq_empty():
    sqs = _make_sqs(dlq_count=0)
    result = index.run_redrive(sqs)
    assert result == {"action": "skip", "reason": "dlq-empty", "dlq_depth": 0}
    sqs.start_message_move_task.assert_not_called()


def test_redrive_is_idempotent_when_task_already_running():
    active = [{
        "Status": "RUNNING",
        "ApproximateNumberOfMessagesMoved": 40,
        "ApproximateNumberOfMessagesToMove": 93,
    }]
    sqs = _make_sqs(dlq_count=93, active_tasks=active)
    result = index.run_redrive(sqs)
    assert result["action"] == "poll"
    assert result["status"] == "RUNNING"
    assert result["messages_moved"] == 40
    assert result["messages_to_move"] == 93
    sqs.start_message_move_task.assert_not_called()


def test_redrive_ignores_completed_tasks_and_starts_a_new_one():
    completed = [{"Status": "COMPLETED", "ApproximateNumberOfMessagesMoved": 93}]
    sqs = _make_sqs(dlq_count=5, active_tasks=completed)
    result = index.run_redrive(sqs)
    assert result["action"] == "started"
    sqs.start_message_move_task.assert_called_once()


def test_redrive_treats_concurrent_start_race_as_running():
    sqs = _make_sqs(dlq_count=5, start_side_effect=_client_error(
        "StartMessageMoveTask", msg="A message move task is already in progress"
    ))
    result = index.run_redrive(sqs)
    assert result["action"] == "started"
    assert result["task_handle"] is None


def test_redrive_raises_on_unexpected_start_failure():
    sqs = _make_sqs(dlq_count=5, start_side_effect=_client_error(
        "StartMessageMoveTask", code="AccessDenied", msg="denied"
    ))
    with pytest.raises(index.DlqRedriveError):
        index.run_redrive(sqs)


def test_redrive_raises_on_list_move_tasks_failure():
    sqs = _make_sqs(dlq_count=5)
    sqs.list_message_move_tasks.side_effect = _client_error("ListMessageMoveTasks")
    with pytest.raises(index.DlqRedriveError):
        index.run_redrive(sqs)


def test_redrive_respects_max_per_second_env(monkeypatch):
    monkeypatch.setattr(index, "MAX_MOVE_MESSAGES_PER_SECOND", "10")
    sqs = _make_sqs(dlq_count=5)
    index.run_redrive(sqs)
    call_kwargs = sqs.start_message_move_task.call_args.kwargs
    assert call_kwargs["MaxNumberOfMessagesPerSecond"] == 10


# ── run_age_check ────────────────────────────────────────────────────────


def test_age_check_no_breach_below_threshold(stub_publish):
    sqs = _make_sqs(dlq_count=93, dlq_age=index.AGE_THRESHOLD_SECONDS - 1)
    result = index.run_age_check(sqs)
    assert result["breach"] is False
    stub_publish.assert_not_called()


def test_age_check_breach_pages_at_error_severity(stub_publish):
    # Measured fact: 93 msgs, oldest 218h24m = 786,240s (~9.1d) — below the
    # 10d/864000s threshold at that moment, so bump slightly past threshold
    # to exercise the breach path deterministically.
    sqs = _make_sqs(dlq_count=93, dlq_age=index.AGE_THRESHOLD_SECONDS + 3600)
    result = index.run_age_check(sqs)
    assert result["breach"] is True
    assert result["dlq_count"] == 93
    stub_publish.assert_called_once()
    _, kwargs = stub_publish.call_args
    assert kwargs["severity"] == "error"
    assert kwargs["dedup_key"] == f"dlq-age-breach-{index.DLQ_NAME}"
    assert "93" in stub_publish.call_args.args[0]


def test_age_check_empty_queue_never_breaches_regardless_of_reported_age():
    # SQS can report a stale ApproximateAgeOfOldestMessage attribute value
    # even at count=0; an empty queue must never be treated as a breach.
    sqs = _make_sqs(dlq_count=0)
    result = index.run_age_check(sqs)
    assert result["breach"] is False
    assert result["oldest_age_seconds"] == 0


def test_age_check_raises_on_get_attributes_failure():
    sqs = _make_sqs(dlq_count=5)
    sqs.get_queue_attributes.side_effect = _client_error("GetQueueAttributes")
    with pytest.raises(index.DlqRedriveError):
        index.run_age_check(sqs)


# ── handler ──────────────────────────────────────────────────────────────


def test_handler_runs_both_steps(monkeypatch):
    sqs = _make_sqs(dlq_count=93, dlq_age=index.AGE_THRESHOLD_SECONDS + 1)
    with patch("index.boto3") as mock_boto3:
        mock_boto3.client.return_value = sqs
        result = index.handler({}, None)
    assert result["redrive"]["action"] == "started"
    assert result["age_check"]["breach"] is True
