"""Unit tests for alert-drain-liveness-probe/index.handler (config#3173).

Mirrors ci-watch-liveness-probe's test shape — same mid-run spot-reclaim
pattern, simpler payload (a relaunch needs no reconstructed fields, just
"run the drain again").
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))
import index  # noqa: E402

RUN_ID = "drain-2026-07-22T1200Z"
COMPLETION_KEY = f"overseer/_control/completed/alert-drain-{RUN_ID}.json"
RELAUNCH_KEY = f"overseer/_control/relaunch/alert-drain-{RUN_ID}.json"

WATCH_TAGS = {"Name": "alpha-engine-alert-drain-spot", "alert-drain-run-id": RUN_ID}


class FakeClientError(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


@pytest.fixture(autouse=True)
def reset_notify(monkeypatch):
    mock = MagicMock(return_value=True)
    monkeypatch.setattr(index, "notify_via_flow_doctor", mock)
    yield mock


def _reclaim_event(detail_type="EC2 Spot Instance Interruption Warning",
                   instance_id="i-dead", **detail_overrides):
    detail = {"instance-id": instance_id}
    detail.update(detail_overrides)
    return {"source": "aws.ec2", "detail-type": detail_type, "detail": detail}


def _make_ec2(tags=WATCH_TAGS, instance_id="i-dead"):
    ec2 = MagicMock()
    ec2.describe_tags.return_value = {
        "Tags": [{"Key": k, "Value": v, "ResourceId": instance_id} for k, v in tags.items()]
    }
    return ec2


def _make_s3(*, marker_exists=False, relaunch=None):
    s3 = MagicMock()
    if marker_exists:
        s3.head_object.return_value = {}
    else:
        s3.head_object.side_effect = FakeClientError("404")

    docs = {}
    if relaunch is not None:
        docs[RELAUNCH_KEY] = relaunch

    def get_object(Bucket, Key):  # noqa: N803 — boto3 kwarg names
        if Key not in docs:
            raise FakeClientError("NoSuchKey")
        body = MagicMock()
        body.read.return_value = json.dumps(docs[Key]).encode()
        return {"Body": body}

    s3.get_object.side_effect = get_object
    return s3


def _clients_factory(ec2, s3, lam):
    def factory(name, region_name=None):
        return {"ec2": ec2, "s3": s3, "lambda": lam}[name]
    return factory


def _run(event, *, ec2=None, s3=None, lam=None):
    ec2 = ec2 if ec2 is not None else _make_ec2()
    s3 = s3 if s3 is not None else _make_s3()
    lam = lam if lam is not None else MagicMock()
    factory = _clients_factory(ec2, s3, lam)
    with patch("index.boto3.client", side_effect=factory):
        result = index.handler(event, None)
    return result, ec2, s3, lam


def test_scheduled_probe_event_is_a_documented_noop():
    result, _, s3, lam = _run({})
    assert result == {"reclaim_event": False, "noop": True}
    s3.head_object.assert_not_called()
    lam.invoke.assert_not_called()


def test_non_terminated_state_change_is_ignored():
    result, ec2, s3, lam = _run(
        _reclaim_event(detail_type="EC2 Instance State-change Notification", state="stopping")
    )
    assert result["handled"] is False
    assert result["reason"] == "not_terminated"
    ec2.describe_tags.assert_not_called()


def test_reclaim_event_without_instance_id_raises():
    with pytest.raises(ValueError, match="instance-id"):
        index.handler({"source": "aws.ec2", "detail-type": "EC2 Spot Instance Interruption Warning",
                       "detail": {}}, None)


def test_reclaim_event_for_non_drain_box_exits_quietly():
    ec2 = _make_ec2(tags={"Name": "alpha-engine-data-spot"})
    result, _, s3, lam = _run(_reclaim_event(), ec2=ec2)
    assert result["watch_box"] is False
    s3.head_object.assert_not_called()
    lam.invoke.assert_not_called()
    index.notify_via_flow_doctor.assert_not_called()


def test_reclaim_with_completion_marker_is_clean_exit():
    s3 = _make_s3(marker_exists=True)
    result, _, s3, lam = _run(_reclaim_event(), s3=s3)
    assert result["watch_box"] is True
    assert result["completed"] is True
    assert s3.head_object.call_args.kwargs["Key"] == COMPLETION_KEY
    lam.invoke.assert_not_called()
    index.notify_via_flow_doctor.assert_not_called()


def test_reclaim_with_missing_run_id_tag_escalates_loud():
    ec2 = _make_ec2(tags={"Name": "alpha-engine-alert-drain-spot"})
    result, _, s3, lam = _run(_reclaim_event(), ec2=ec2)
    assert result["reason"] == "missing_discriminator_tag"
    assert result["escalated"] is True
    lam.invoke.assert_not_called()
    kwargs = index.notify_via_flow_doctor.call_args.kwargs
    assert kwargs["silent"] is False
    assert kwargs["severity"] == "error"


def test_first_mid_run_death_relaunches_once_with_record_before_invoke():
    result, _, s3, lam = _run(_reclaim_event(instance_id="i-dead"))

    assert result["completed"] is False
    assert result["relaunched"] is True

    put_call = s3.put_object.call_args
    assert put_call.kwargs["Key"] == RELAUNCH_KEY
    ledger = json.loads(put_call.kwargs["Body"])
    assert ledger["dead_instance_id"] == "i-dead"

    lam.invoke.assert_called_once()
    kwargs = lam.invoke.call_args.kwargs
    assert kwargs["FunctionName"] == index.ALERT_DRAIN_DISPATCHER_FUNCTION
    assert kwargs["InvocationType"] == "Event"
    payload = json.loads(kwargs["Payload"])
    assert payload == {
        "is_drill": "false",
        "trigger": "reclaim-relaunch",
        # config-I7400: the relaunch MUST inherit the lineage, or the fresh
        # run_id the dispatcher mints resets the bound it is supposed to spend.
        "lineage_id": RUN_ID,
    }
    index.notify_via_flow_doctor.assert_called_once()
    assert index.notify_via_flow_doctor.call_args.kwargs["silent"] is True


def test_duplicate_notification_for_same_dead_instance_is_a_noop():
    s3 = _make_s3(relaunch={"dead_instance_id": "i-dead"})
    result, _, s3, lam = _run(_reclaim_event(instance_id="i-dead"), s3=s3)
    assert result["duplicate_notification"] is True
    lam.invoke.assert_not_called()
    s3.put_object.assert_not_called()
    index.notify_via_flow_doctor.assert_not_called()


def test_second_death_for_different_instance_escalates_loud_not_relaunch():
    s3 = _make_s3(relaunch={"dead_instance_id": "i-first-relaunch"})
    result, _, s3, lam = _run(_reclaim_event(instance_id="i-second-dead"), s3=s3)
    assert result["reason"] == "second_death"
    assert result["escalated"] is True
    lam.invoke.assert_not_called()
    s3.put_object.assert_not_called()
    kwargs = index.notify_via_flow_doctor.call_args.kwargs
    assert kwargs["silent"] is False
    assert "SECOND watch-box death" in index.notify_via_flow_doctor.call_args.args[0]


def test_invoke_failure_still_records_ledger_and_escalates():
    lam = MagicMock()
    lam.invoke.side_effect = RuntimeError("boom")
    result, _, s3, lam = _run(_reclaim_event(), lam=lam)
    assert result["relaunched"] is False
    assert result["reason"] == "invoke_failed"
    assert result["escalated"] is True
    s3.put_object.assert_called_once()
    assert index.notify_via_flow_doctor.call_args.kwargs["severity"] == "error"


def test_dead_drill_box_never_relaunches_or_escalates():
    tags = {"Name": "alpha-engine-alert-drain-spot", "alert-drain-run-id": "drill-2026-07-22T1200Z"}
    ec2 = _make_ec2(tags=tags)
    s3 = _make_s3()
    result, _, s3, lam = _run(_reclaim_event(), ec2=ec2, s3=s3)
    assert result["drill"] is True
    assert result["completed"] is False
    assert result["relaunched"] is False
    s3.put_object.assert_not_called()
    lam.invoke.assert_not_called()
    index.notify_via_flow_doctor.assert_not_called()


def test_completed_drill_box_is_clean_no_relaunch_no_page():
    tags = {"Name": "alpha-engine-alert-drain-spot", "alert-drain-run-id": "drill-2026-07-22T1200Z"}
    ec2 = _make_ec2(tags=tags)
    s3 = _make_s3(marker_exists=True)
    result, _, s3, lam = _run(_reclaim_event(), ec2=ec2, s3=s3)
    assert result["drill"] is True
    assert result["completed"] is True
    lam.invoke.assert_not_called()
    index.notify_via_flow_doctor.assert_not_called()


def test_keys_derived_from_run_id():
    assert index._completion_key(RUN_ID) == COMPLETION_KEY
    assert index._relaunch_key(RUN_ID) == RELAUNCH_KEY


# ── config-I7400 ────────────────────────────────────────────────────────────
# Two defects that together turned one deterministic failure into 17 spot
# boxes in 70 minutes on 2026-08-15, every page reading "attempt 1/1".

LINEAGE_ID = "drain-2026-07-22T1100Z"
LINEAGE_RELAUNCH_KEY = f"overseer/_control/relaunch/alert-drain-{LINEAGE_ID}.json"

_STATE_CHANGE = "EC2 Instance State-change Notification"


def _terminated_event(instance_id="i-dead"):
    """The event a SELF-SHUTDOWN produces — indistinguishable, at the event
    level, from the one a reclaim produces after the warning."""
    return _reclaim_event(detail_type=_STATE_CHANGE, instance_id=instance_id,
                          state="terminated")


class TestOnlyAReclaimMayRelaunch:
    def test_bare_termination_without_a_warning_escalates_and_does_not_relaunch(self):
        result, _, s3, lam = _run(_terminated_event())
        assert result["relaunched"] is False
        assert result["reason"] == "workload_failure_not_reclaim"
        assert result["escalated"] is True
        lam.invoke.assert_not_called(), "a deterministic failure was retried unchanged"
        s3.put_object.assert_not_called()

    def test_that_escalation_is_loud(self):
        _run(_terminated_event())
        kwargs = index.notify_via_flow_doctor.call_args.kwargs
        assert kwargs["silent"] is False
        assert kwargs["severity"] == "error"
        assert "NOT reclaimed" in index.notify_via_flow_doctor.call_args.args[0]

    def test_a_spot_interruption_warning_still_relaunches(self):
        """The fix must not disarm the case the probe exists for."""
        result, _, _, lam = _run(_reclaim_event())
        assert result["relaunched"] is True
        lam.invoke.assert_called_once()

    def test_a_completed_box_is_still_clean_on_a_bare_termination(self):
        """The normal end of every healthy drain: it finishes, writes its
        marker, and shuts itself down. That must not page."""
        result, _, _, lam = _run(_terminated_event(), s3=_make_s3(marker_exists=True))
        assert result["completed"] is True
        assert "reason" not in result
        lam.invoke.assert_not_called()
        index.notify_via_flow_doctor.assert_not_called()

    def test_the_workload_failure_page_dedups_per_lineage(self):
        """A stuck lane pages once per chain, not once per box."""
        _run(_terminated_event())
        key = index.notify_via_flow_doctor.call_args.kwargs["dedup_key"]
        assert key.endswith(f":workload_failure:{RUN_ID}")


class TestTheBoundKeysOnLineage:
    def test_ledger_key_uses_the_lineage_tag_when_present(self):
        tags = {**WATCH_TAGS, "alert-drain-lineage-id": LINEAGE_ID}
        result, _, s3, _ = _run(_reclaim_event(), ec2=_make_ec2(tags=tags))
        assert result["lineage_id"] == LINEAGE_ID
        assert s3.put_object.call_args.kwargs["Key"] == LINEAGE_RELAUNCH_KEY

    def test_ledger_key_falls_back_to_run_id_for_a_pre_lineage_box(self):
        """A box launched before this shipped carries no lineage tag. Its own
        run_id IS the lineage root, so nothing is stranded."""
        result, _, s3, _ = _run(_reclaim_event())
        assert result["lineage_id"] == RUN_ID
        assert s3.put_object.call_args.kwargs["Key"] == RELAUNCH_KEY

    def test_the_relaunched_box_spends_the_bound_instead_of_resetting_it(self):
        """THE REGRESSION. Box A (run_id=RUN_ID) is reclaimed and relaunched.
        Box B inherits the lineage but gets a NEW run_id. Box B is then also
        reclaimed. Before this fix B's ledger key was derived from B's own
        run_id, which had no record, so B relaunched too -- and so on without
        end. Keyed on lineage, B's death finds A's record and escalates."""
        b_tags = {
            "Name": "alpha-engine-alert-drain-spot",
            "alert-drain-run-id": "drain-2026-07-22T1204Z",   # freshly minted
            "alert-drain-lineage-id": LINEAGE_ID,             # inherited
        }
        s3 = _make_s3()
        docs = {LINEAGE_RELAUNCH_KEY: {"dead_instance_id": "i-box-a"}}

        def get_object(Bucket, Key):  # noqa: N803 — boto3 kwarg names
            if Key not in docs:
                raise FakeClientError("NoSuchKey")
            body = MagicMock()
            body.read.return_value = json.dumps(docs[Key]).encode()
            return {"Body": body}

        s3.get_object.side_effect = get_object

        result, _, s3, lam = _run(
            _reclaim_event(instance_id="i-box-b"), ec2=_make_ec2(tags=b_tags), s3=s3,
        )
        assert result["reason"] == "second_death"
        assert result["escalated"] is True
        lam.invoke.assert_not_called()

    def test_second_death_page_dedups_per_lineage(self):
        s3 = _make_s3(relaunch={"dead_instance_id": "i-first"})
        _run(_reclaim_event(instance_id="i-second"), s3=s3)
        key = index.notify_via_flow_doctor.call_args.kwargs["dedup_key"]
        assert key.endswith(f":second_death:{RUN_ID}")

    def test_the_ledger_records_both_identities(self):
        _, _, s3, _ = _run(_reclaim_event(instance_id="i-dead"))
        ledger = json.loads(s3.put_object.call_args.kwargs["Body"])
        assert ledger["dead_instance_id"] == "i-dead"
        assert ledger["dead_run_id"] == RUN_ID
        assert ledger["lineage_id"] == RUN_ID
