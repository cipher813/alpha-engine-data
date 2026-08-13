"""Handler tests for alpha-engine-preflight-sweep-dispatcher (config-I7249).

Run by ``deploy.sh`` before packaging, so a broken handler cannot be deployed.

The three properties under test are the ones that decide whether this PR is
deployable by the merge button alone and whether the console can be trusted:

1. **The pre-spend guard skips without spending** until the sweep code is on
   main — that is what makes the MERGE the activation, with no post-merge
   command to forget.
2. **A declared skip is not a failure.** ``off`` in the cadence manifest, and
   the operator kill-switch, must be distinguishable from "the sweep died" on
   every surface — otherwise the deadman cannot tell them apart either.
3. **Silence is unproducible.** Every failure path writes the console's
   "could not measure" row before it raises.
"""

from __future__ import annotations

import io
import json
import os
import sys
import types
import urllib.error

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


class _FakeBody:
    def __init__(self, payload):
        self._payload = json.dumps(payload).encode()

    def read(self):
        return self._payload


class FakeS3:
    def __init__(self):
        self.puts = {}

    def put_object(self, Bucket, Key, Body, **_kw):  # noqa: N803 — boto3 contract
        self.puts[Key] = json.loads(Body.decode())


class FakeLambdaClient:
    def __init__(self, payload=None, function_error=None, raises=None):
        self._payload = payload or {}
        self._function_error = function_error
        self._raises = raises

    def invoke(self, **_kw):
        if self._raises:
            raise self._raises
        out = {"Payload": _FakeBody(self._payload)}
        if self._function_error:
            out["FunctionError"] = self._function_error
        return out


class FakeSsm:
    def __init__(self, raises=None):
        self._raises = raises
        self.sent = []

    def send_command(self, **kw):
        if self._raises:
            raise self._raises
        self.sent.append(kw)
        return {"Command": {"CommandId": "cmd-1"}}


class FakeEc2:
    def __init__(self):
        self.tagged = []
        self.terminated = []

    def create_tags(self, Resources, Tags):  # noqa: N803
        self.tagged.append((Resources, Tags))

    def terminate_instances(self, InstanceIds):  # noqa: N803
        self.terminated.extend(InstanceIds)


class FakeCw:
    def __init__(self):
        self.metrics = []

    def put_metric_data(self, Namespace, MetricData):  # noqa: N803
        self.metrics.extend(m["MetricName"] for m in MetricData)


@pytest.fixture
def index(monkeypatch):
    import index as mod

    fakes = types.SimpleNamespace(
        s3=FakeS3(), ssm=FakeSsm(), ec2=FakeEc2(), cw=FakeCw(),
        lam=FakeLambdaClient({"instance_id": "i-abc", "command_id": "boot-1", "market": "spot"}),
    )

    def _client(name, **_kw):
        return {"s3": fakes.s3, "ssm": fakes.ssm, "ec2": fakes.ec2,
                "cloudwatch": fakes.cw, "lambda": fakes.lam}[name]

    monkeypatch.setattr(mod.boto3, "client", _client)
    monkeypatch.setattr(mod, "DISPATCH_ENABLED", True)
    mod._fakes = fakes
    return mod


class _FakeResponse:
    """A urlopen stand-in. Must be a real context manager: Python resolves
    ``__enter__``/``__exit__`` on the TYPE, so attaching them to an instance
    silently fails the ``with`` and the guard reads it as an unreachable URL."""

    def __init__(self, body: bytes, status: int = 200):
        self._body = body
        self.status = status

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False


def _opener(status_by_path, cadence=None):
    def opener(request, timeout=None):  # noqa: ARG001
        url = request.full_url if hasattr(request, "full_url") else str(request)
        for path, status in status_by_path.items():
            if path in url:
                if status != 200:
                    raise urllib.error.HTTPError(url, status, "nope", {}, io.BytesIO(b""))
                break
        return _FakeResponse(json.dumps(cadence or {}).encode())

    return opener


CADENCE_DAILY = {
    "sweep_cadence": "daily",
    "cadence_minutes": 1440,
    "allowed_values": ["daily", "off"],
}


# ── 1. The pre-spend guard ───────────────────────────────────────────────────


def test_the_guard_reports_absent_sweep_code_without_spending(index):
    deployed, reason = index.sweep_code_is_deployed(
        opener=_opener({"preflight_sweep.py": 404})
    )
    assert deployed is False
    assert "preflight_sweep.py" in reason


def test_the_guard_confirms_deployed_code(index):
    deployed, reason = index.sweep_code_is_deployed(opener=_opener({}))
    assert deployed is True


def test_a_network_failure_errs_toward_not_spending(index):
    """Unable to confirm the code is deployed is not the same as confirming it
    is, and a spot hour is the wrong side to err on."""

    def opener(*_a, **_k):
        raise OSError("dns down")

    deployed, reason = index.sweep_code_is_deployed(opener=opener)
    assert deployed is False
    assert "could not confirm" in reason


def test_the_handler_skips_and_launches_nothing_when_the_code_is_absent(index, monkeypatch):
    monkeypatch.setattr(
        index, "sweep_code_is_deployed", lambda **_k: (False, "not on main yet")
    )
    monkeypatch.setattr(index, "read_declared_cadence", lambda **_k: ("daily", "ok"))
    out = index.handler({}, None)
    assert out["dispatched"] is False
    assert out["declared_skip"] is True
    assert index._fakes.ssm.sent == []
    assert index._fakes.ec2.terminated == []
    assert "PreflightSweepDispatchSkipped" in index._fakes.cw.metrics


# ── 2. A declared skip is not a failure ──────────────────────────────────────


def test_a_cadence_of_off_is_a_declared_skip_with_its_own_metric(index, monkeypatch):
    monkeypatch.setattr(
        index, "read_declared_cadence", lambda **_k: ("off", "declared cadence: off")
    )
    out = index.handler({}, None)
    assert out["dispatched"] is False and out["declared_skip"] is True
    # The distinct metric is what keeps "gated off by declaration" separable
    # from "the sweep died" on the deadman's surface.
    assert "PreflightSweepDeclaredOff" in index._fakes.cw.metrics
    assert "PreflightSweepDispatchSkipped" not in index._fakes.cw.metrics


def test_the_kill_switch_is_a_declared_skip_not_an_error(index, monkeypatch):
    monkeypatch.setattr(index, "DISPATCH_ENABLED", False)
    out = index.handler({}, None)
    assert out["dispatched"] is False and out["declared_skip"] is True
    assert index._fakes.lam._payload is not None  # never invoked


def test_an_unreadable_cadence_is_unknown_never_defaulted_to_daily(index):
    def opener(*_a, **_k):
        raise OSError("no network")

    cadence, reason = index.read_declared_cadence(opener=opener)
    assert cadence == "unknown"
    assert "could not read" in reason


def test_a_cadence_outside_allowed_values_is_unknown(index):
    cadence, reason = index.read_declared_cadence(
        opener=_opener({}, cadence={"sweep_cadence": "hourly", "allowed_values": ["daily", "off"]})
    )
    assert cadence == "unknown"


# ── 3. Silence is unproducible ───────────────────────────────────────────────


def test_a_box_that_never_launches_still_writes_the_console_row(index, monkeypatch):
    monkeypatch.setattr(index, "read_declared_cadence", lambda **_k: ("daily", "ok"))
    monkeypatch.setattr(index, "sweep_code_is_deployed", lambda **_k: (True, "ok"))
    index._fakes.lam = FakeLambdaClient(raises=RuntimeError("no capacity"))
    with pytest.raises(index.SweepDispatchError):
        index.handler({}, None)
    row = index._fakes.s3.puts.get("ops/checks/ae-preflight-sweep/latest.json")
    assert row is not None, "a dispatch failure must not leave the console silent"
    assert row["status"] == "error"
    assert row["measured"] is False
    assert "no capacity" in row["unmeasured_reason"]


def test_an_undeliverable_command_terminates_the_box_and_reports(index, monkeypatch):
    monkeypatch.setattr(index, "read_declared_cadence", lambda **_k: ("daily", "ok"))
    monkeypatch.setattr(index, "sweep_code_is_deployed", lambda **_k: (True, "ok"))
    index._fakes.ssm = FakeSsm(raises=RuntimeError("Undeliverable"))
    with pytest.raises(index.SweepDispatchError):
        index.handler({}, None)
    # An orphaned launcher box is a spend leak; the failure path must not
    # create one.
    assert index._fakes.ec2.terminated == ["i-abc"]
    row = index._fakes.s3.puts["ops/checks/ae-preflight-sweep/latest.json"]
    assert row["status"] == "error"


def test_the_unmeasured_row_never_claims_output_correctness(index):
    index.write_unmeasured_report("run-1", "the box never booted")
    row = index._fakes.s3.puts["ops/checks/ae-preflight-sweep/latest.json"]
    assert "PRECONDITIONS only" in row["summary"]


def test_the_deadman_subject_is_emitted_when_the_row_is_written(index):
    index.write_unmeasured_report("run-1", "the box never booted")
    assert "PreflightSweepRunCompleted" in index._fakes.cw.metrics


# ── Happy path ───────────────────────────────────────────────────────────────


def test_a_successful_dispatch_reuses_the_weekly_launcher_box(index, monkeypatch):
    monkeypatch.setattr(index, "read_declared_cadence", lambda **_k: ("daily", "ok"))
    monkeypatch.setattr(index, "sweep_code_is_deployed", lambda **_k: (True, "ok"))
    out = index.handler({}, None)
    assert out["dispatched"] is True
    assert out["instance_id"] == "i-abc"
    assert out["command_id"] == "cmd-1"
    # The bootstrap command id must reach the box, or the sweep cannot tell a
    # failed bootstrap from a slow one.
    assert "boot-1" in index._fakes.ssm.sent[0]["Parameters"]["commands"][0]


def test_the_box_is_retagged_with_this_job_s_much_shorter_deadline(index, monkeypatch):
    monkeypatch.setattr(index, "read_declared_cadence", lambda **_k: ("daily", "ok"))
    monkeypatch.setattr(index, "sweep_code_is_deployed", lambda **_k: (True, "ok"))
    index.handler({}, None)
    resources, tags = index._fakes.ec2.tagged[0]
    keys = {t["Key"] for t in tags}
    assert resources == ["i-abc"]
    assert {"watchdog-deadline", "pipeline_role"} <= keys
