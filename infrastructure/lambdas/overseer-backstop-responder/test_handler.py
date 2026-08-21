"""Tests for the overseer-backstop-responder (alpha-engine-config-I4480).

The properties that matter are behavioural, not cosmetic:

  1. an alarm NOT in the reviewed allowlist never triggers an action;
  2. a second firing inside the cooldown window escalates instead of retrying;
  3. the page still goes out when evidence-gathering partially fails — a
     backstop that crashes while reporting an outage is worse than one that
     reports "could not read X";
  4. the responder never raises, whatever AWS does to it.

Every boto3 client is faked. Nothing here touches AWS, SNS, or Telegram.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import index  # noqa: E402


class NoSuchKey(Exception):
    """Named to match botocore's real exception — _is_missing_key matches on
    the class NAME, so a differently-named fake would not exercise the path."""


class FakeS3:
    def __init__(self, existing: dict | None = None, raise_on_get: bool = False):
        self.objects = existing or {}
        self.puts: list[str] = []
        self.raise_on_get = raise_on_get
        self.exceptions = type("E", (), {"NoSuchKey": NoSuchKey})

    def get_object(self, Bucket, Key):  # noqa: N803 — boto3 kwarg casing
        if self.raise_on_get:
            raise RuntimeError("s3 down")
        if Key not in self.objects:
            raise NoSuchKey()
        body = json.dumps(self.objects[Key]).encode()
        return {"Body": type("B", (), {"read": lambda self, b=body: b})()}

    def put_object(self, Bucket, Key, Body, ContentType=None):  # noqa: N803
        self.puts.append(Key)
        self.objects[Key] = json.loads(Body)

    def list_objects_v2(self, Bucket, Prefix):  # noqa: N803
        return {"Contents": []}


class FakeLambda:
    def __init__(self):
        self.invocations: list[dict] = []

    def get_function_configuration(self, FunctionName):  # noqa: N803
        return {"Environment": {"Variables": {}}}

    def invoke(self, FunctionName, InvocationType, Payload):  # noqa: N803
        self.invocations.append(
            {"function": FunctionName, "payload": json.loads(Payload or b"{}")}
        )
        body = json.dumps({"routed": True, "verdict": {"launched": True}}).encode()
        return {"Payload": type("P", (), {"read": lambda self, b=body: b})()}


class FakeSqs:
    """Validates attribute names the way SQS actually does.

    The first version of this fake returned whatever it was asked for,
    including `ApproximateAgeOfOldestMessage` — which is an AWS/SQS *metric*,
    not a queue attribute. Real SQS answers `InvalidAttributeName` and fails
    the WHOLE call, taking the depth reading with it. The permissive fake
    turned an absent guarantee into a believed one, which is the exact class
    overseer-policy §4 inv. 13 exists to prevent; the defect surfaced only on
    a live invoke of the deployed function.
    """

    VALID = {
        "ApproximateNumberOfMessages",
        "ApproximateNumberOfMessagesNotVisible",
        "ApproximateNumberOfMessagesDelayed",
        "All",
    }

    def get_queue_url(self, QueueName):  # noqa: N803
        return {"QueueUrl": f"https://sqs/{QueueName}"}

    def get_queue_attributes(self, QueueUrl, AttributeNames):  # noqa: N803
        bad = set(AttributeNames) - self.VALID
        if bad:
            raise type("InvalidAttributeName", (Exception,), {})(
                f"Unknown Attribute {sorted(bad)[0]}."
            )
        return {"Attributes": {
            "ApproximateNumberOfMessages": "99",
            "ApproximateNumberOfMessagesNotVisible": "0",
        }}


class FakeCw:
    def get_metric_statistics(self, **kw):
        from datetime import datetime as _dt
        return {"Datapoints": [
            {"Sum": 6.0, "Maximum": 7200.0,
             "Timestamp": _dt(2026, 7, 28, 16, 0, tzinfo=timezone.utc)}
        ]}


@pytest.fixture
def wired(monkeypatch):
    """Wire fakes in and capture the page instead of sending it."""
    state = {
        "s3": FakeS3(), "lambda": FakeLambda(), "sqs": FakeSqs(),
        "cloudwatch": FakeCw(), "pages": [],
    }
    monkeypatch.setattr(index, "boto3", type("B", (), {
        "client": staticmethod(lambda name, region_name=None: state[name])
    }))
    monkeypatch.setattr(index, "_telegram",
                        lambda text: (state["pages"].append(text), True)[1])
    monkeypatch.setattr(index, "RECOVERY_ENABLED", True)
    # Nothing paused, stated explicitly. In the source tree the bundled manifest
    # does not sit next to index.py (deploy.sh copies it in), so without this the
    # every-action test would pass because the manifest was UNREADABLE rather
    # than because nothing was paused — a green that proves the wrong thing.
    monkeypatch.setattr(index, "_paused_trigger_names", lambda: set())
    return state


def _sns(alarm: str, reason: str = "Threshold crossed") -> dict:
    return {"Records": [{"Sns": {"Message": json.dumps(
        {"AlarmName": alarm, "NewStateReason": reason})}}]}


INTAKE_AGE = "alpha-engine-watch-plane-overseer-intake-age"
PROBE_ERRORS = "alpha-engine-watch-plane-overseer-liveness-probe-errors"


# ── Property 1: the allowlist is the entire authority ───────────────────────


def test_unmapped_alarm_takes_no_action(wired):
    out = index.handler(_sns("some-unrelated-billing-alarm"), None)
    assert wired["lambda"].invocations == [], "an unmapped alarm must never act"
    assert "allowlist" in out["outcome"]["skipped"]
    assert wired["pages"], "it must still page — reporting is unconditional"


def test_intake_age_alarm_redispatches_the_drain_once(wired):
    index.handler(_sns(INTAKE_AGE), None)
    invs = wired["lambda"].invocations
    assert len(invs) == 1
    assert invs[0]["function"] == index.ROUTER_FUNCTION
    assert invs[0]["payload"]["playbook"] == "alert-drain"
    assert invs[0]["payload"]["payload"]["is_drill"] == "false", (
        "a drill would prove the pipe works while leaving the backlog untouched"
    )


def test_probe_errors_alarm_reinvokes_the_probe(wired):
    index.handler(_sns(PROBE_ERRORS), None)
    invs = wired["lambda"].invocations
    assert len(invs) == 1
    assert invs[0]["function"] == index.PROBE_FUNCTION


def test_kill_switch_disables_action_but_not_the_page(wired, monkeypatch):
    monkeypatch.setattr(index, "RECOVERY_ENABLED", False)
    out = index.handler(_sns(INTAKE_AGE), None)
    assert wired["lambda"].invocations == []
    assert "kill switch" in out["outcome"]["skipped"]
    assert wired["pages"]


def test_every_allowlist_entry_is_a_known_action():
    """Guards against a typo'd action name silently becoming a no-op."""
    for alarm, spec in index.ALARM_ACTIONS.items():
        assert spec["action"] in ("redispatch", "invoke_probe"), alarm
        assert spec.get("rationale"), f"{alarm} must carry a rationale"
        assert spec.get("component"), f"{alarm} must name its target component"
        assert spec.get("triggers"), f"{alarm} must name its component's triggers"
        if spec["action"] == "redispatch":
            assert spec.get("playbook") and isinstance(spec.get("payload"), dict)


# ── Property 5: never restart automation the operator paused (I7330) ────────


REPO_MANIFEST = (
    Path(__file__).resolve().parents[2] / "automation_pause.json"
)


def _manifest_names() -> set[str]:
    m = json.loads(REPO_MANIFEST.read_text(encoding="utf-8"))
    names = set()
    for surface in ("events_rules", "scheduler_schedules"):
        names |= set(m["paused"][surface])
    names |= {k for k in m.get("pending", {}) if not k.startswith("_")}
    return names


def test_declared_triggers_all_exist_in_the_repo_manifest():
    """A typo in `triggers` would silently disable the gate, not fail it.

    `_pause_verdict` asks whether every declared name is IN the paused set. A
    misspelled name can never be in it, so the component reads as live forever
    and the guard is gone with nothing red. This test is the only thing that
    notices — it compares against the real repo manifest, not a fixture.
    """
    known = _manifest_names()
    assert known, "the repo manifest parsed to zero paused triggers"
    for alarm, spec in index.ALARM_ACTIONS.items():
        for trigger in spec["triggers"]:
            assert trigger in known, (
                f"{alarm}: declared trigger {trigger!r} appears nowhere in "
                f"automation_pause.json. Either it was renamed there, or it is "
                f"a typo — both make the pause gate a permanent no-op."
            )


def test_action_is_skipped_when_every_trigger_of_its_component_is_paused(
        wired, monkeypatch):
    for alarm, spec in index.ALARM_ACTIONS.items():
        wired["lambda"].invocations.clear()
        monkeypatch.setattr(index, "_paused_trigger_names",
                            lambda s=spec: set(s["triggers"]))
        out = index.handler(_sns(alarm), None)
        assert wired["lambda"].invocations == [], (
            f"{alarm}: acted on a component whose every trigger is paused"
        )
        assert "paused in automation_pause.json" in out["outcome"]["skipped"]
        assert wired["pages"], "reporting stays unconditional"


def test_a_partially_paused_component_is_still_recovered(wired, monkeypatch):
    """One live trigger means the component still runs on its own cadence, so
    its alarm is a real failure and the bounded recovery is still correct."""
    spec = index.ALARM_ACTIONS[INTAKE_AGE]
    monkeypatch.setattr(index, "_paused_trigger_names",
                        lambda: set(spec["triggers"][1:]))
    index.handler(_sns(INTAKE_AGE), None)
    assert len(wired["lambda"].invocations) == 1


def test_a_paused_component_does_not_consume_its_cooldown_window(wired, monkeypatch):
    """The skip must happen BEFORE the attempt is claimed.

    Otherwise the first firing after the pause lifts would escalate as a
    "second firing" while no attempt was ever made — the escalation path would
    fire having never tried anything.
    """
    spec = index.ALARM_ACTIONS[PROBE_ERRORS]
    monkeypatch.setattr(index, "_paused_trigger_names",
                        lambda: set(spec["triggers"]))
    index.handler(_sns(PROBE_ERRORS), None)
    assert wired["s3"].puts == [], "a paused component must not claim a window"

    monkeypatch.setattr(index, "_paused_trigger_names", lambda: set())
    out = index.handler(_sns(PROBE_ERRORS), None)
    assert len(wired["lambda"].invocations) == 1
    assert not out["outcome"].get("escalated"), (
        "the first real attempt must not be treated as a retry"
    )


def test_an_unreadable_manifest_reports_unknown_rather_than_not_paused(
        monkeypatch, capsys):
    """UNKNOWN is not "nothing is paused", but it must not stop the backstop.

    A backstop that silently declines to act because it could not read a file is
    the failure mode this Lambda exists to survive, so `_pause_verdict` returns
    no skip and the read failure is printed. Deliberately does NOT use the
    `wired` fixture, which stubs the function under test.
    """
    monkeypatch.setattr(index, "PAUSE_MANIFEST", Path("/nonexistent/pause.json"))
    assert index._paused_trigger_names() is None
    assert "pause manifest unreadable" in capsys.readouterr().out
    assert index._pause_verdict(index.ALARM_ACTIONS[PROBE_ERRORS]) is None


def test_every_bundled_file_is_in_the_deploy_workflow_path_filter():
    """A file in the zip but not in the path filter merges green and deploys nothing.

    `deploy.sh` copies three files into the package. The workflow only runs when
    a path in its filter changes, so a bundled file missing from that list means
    edits to it never reach the deployed Lambda — the responder keeps deciding
    from a stale copy and no check goes red. `automation_pause.json` was exactly
    that gap when it was first bundled. This asserts the general property rather
    than the one instance, so the next bundled file cannot repeat it.
    """
    import re

    lambda_dir = Path(__file__).resolve().parent
    repo_root = lambda_dir.parents[2]
    deploy = (lambda_dir / "deploy.sh").read_text(encoding="utf-8")
    workflow = (
        repo_root / ".github/workflows/deploy-overseer-backstop-responder.yml"
    ).read_text(encoding="utf-8")

    # Resolve the handful of path variables deploy.sh uses as `cp` sources.
    variables = {"SCRIPT_DIR": str(lambda_dir), "REPO_ROOT": str(repo_root)}
    for name in ("REGISTRY_SRC", "PAUSE_SRC"):
        m = re.search(rf'^{name}="([^"]+)"', deploy, re.M)
        assert m, f"{name} is no longer assigned in deploy.sh — update this test"
        variables[name] = m.group(1)

    def _expand(raw: str) -> str:
        for _ in range(3):  # variables reference each other one level deep
            raw = re.sub(r"\$\{(\w+)\}",
                         lambda mo: variables.get(mo.group(1), mo.group(0)), raw)
        return raw

    sources = [_expand(m) for m in
               re.findall(r'^\s*cp "([^"]+)" "\$\{PKG\}/', deploy, re.M)]
    assert len(sources) >= 3, f"expected 3+ bundled files, parsed {sources}"

    filters = re.findall(r"^\s+- '([^']+)'$", workflow, re.M)
    for source in sources:
        assert "${" not in source, f"unresolved variable in {source!r}"
        rel = Path(source).resolve().relative_to(repo_root).as_posix()
        covered = any(
            rel == f or (f.endswith("/**") and rel.startswith(f[:-2]))
            for f in filters
        )
        assert covered, (
            f"{rel} is bundled into the zip by deploy.sh but matches no path "
            f"filter in deploy-overseer-backstop-responder.yml. Editing it "
            f"would merge green and deploy nothing. Filters: {filters}"
        )


# ── The dispatch-flag block reports state whose value agrees with its label ──


def test_dispatch_flags_never_render_a_bare_boolean(wired):
    """`router: true` under a heading reading "kill switches" says the opposite
    of what it means — every variable in DISPATCH_FLAGS is an *_ENABLED flag.
    Read as "the router is stopped", it cost a P1 filed against the wrong
    component on 2026-08-14 (alpha-engine-config-I7330).
    """
    out = index.handler(_sns(INTAKE_AGE), None)
    page = wired["pages"][-1]
    assert "kill switches:" not in page
    assert "dispatch flags:" in page
    for label, value in out["evidence"]["dispatch"].items():
        assert value.split(" ")[0] in ("ENABLED", "STOPPED", "UNREADABLE:"), (
            f"{label}: {value!r} must lead with the semantic state"
        )
        assert f"    {label}: true" not in page
        assert f"    {label}: false" not in page


def test_dispatch_flag_polarity_matches_the_variable_value(wired, monkeypatch):
    cases = [({"OVERSEER_DISPATCH_ENABLED": "true"}, "ENABLED"),
             ({"OVERSEER_DISPATCH_ENABLED": "false"}, "STOPPED"),
             ({}, "ENABLED")]  # unset defaults to enabled
    for variables, expected in cases:
        monkeypatch.setattr(
            wired["lambda"], "get_function_configuration",
            lambda FunctionName, v=variables: {"Environment": {"Variables": v}},
        )
        assert index._dispatch_state()["router"].startswith(expected), (
            f"{variables} should render as {expected}"
        )


# ── Property 2: one attempt per window, then escalate ───────────────────────


def test_second_firing_in_window_escalates_instead_of_retrying(wired):
    index.handler(_sns(INTAKE_AGE), None)
    assert len(wired["lambda"].invocations) == 1
    out = index.handler(_sns(INTAKE_AGE), None)
    assert len(wired["lambda"].invocations) == 1, "must NOT retry in-window"
    assert out["outcome"]["escalated"] is True
    assert "SECOND firing" in wired["pages"][-1]


def test_a_different_alarm_is_not_blocked_by_anothers_cooldown(wired):
    index.handler(_sns(INTAKE_AGE), None)
    index.handler(_sns(PROBE_ERRORS), None)
    assert len(wired["lambda"].invocations) == 2


def test_window_key_is_stable_within_and_changes_across_windows(monkeypatch):
    monkeypatch.setattr(index, "COOLDOWN_HOURS", 6)
    at = lambda h: datetime(2026, 7, 28, h, 30, tzinfo=timezone.utc)  # noqa: E731
    assert index._window_start(at(1)) == index._window_start(at(5))
    assert index._window_start(at(5)) != index._window_start(at(7))


def test_cooldown_state_unreadable_fails_open(wired, monkeypatch):
    """If S3 cannot be read we make ONE extra bounded attempt rather than none —
    and the alternative (fail closed) would silently disable recovery exactly
    when the plane is least healthy."""
    monkeypatch.setattr(index, "boto3", type("B", (), {
        "client": staticmethod(lambda name, region_name=None: (
            FakeS3(raise_on_get=True) if name == "s3" else wired[name]
        ))
    }))
    index.handler(_sns(INTAKE_AGE), None)
    assert len(wired["lambda"].invocations) == 1


# ── Property 3+4: the page survives partial blindness; nothing raises ───────


def test_page_still_sent_when_every_evidence_call_fails(monkeypatch):
    pages: list[str] = []

    class Dead:
        def __getattr__(self, _name):
            def _boom(*a, **kw):
                raise RuntimeError("aws down")
            return _boom

    monkeypatch.setattr(index, "boto3", type("B", (), {
        "client": staticmethod(lambda name, region_name=None: Dead())
    }))
    monkeypatch.setattr(index, "_telegram",
                        lambda text: (pages.append(text), True)[1])
    out = index.handler(_sns(INTAKE_AGE), None)
    assert pages, "the page is the primary deliverable and must survive"
    assert "UNREADABLE" in pages[0], "blindness must be named, not hidden"
    assert out["alarm"] == INTAKE_AGE


def test_malformed_sns_event_still_pages(wired):
    out = index.handler({"Records": [{"Sns": {"Message": "not json"}}]}, None)
    assert wired["pages"]
    assert out["alarm"] == "(unknown)"


def test_empty_event_does_not_raise(wired):
    assert index.handler({}, None)["alarm"] == "(unknown)"


def test_router_returning_a_decline_is_reported_as_failure(wired, monkeypatch):
    class Declining(FakeLambda):
        def invoke(self, FunctionName, InvocationType, Payload):  # noqa: N803
            self.invocations.append({"function": FunctionName, "payload": {}})
            body = json.dumps({"routed": False, "reason": "playbook_disabled"}).encode()
            return {"Payload": type("P", (), {"read": lambda self, b=body: b})()}

    declining = Declining()
    monkeypatch.setattr(index, "boto3", type("B", (), {
        "client": staticmethod(lambda name, region_name=None: (
            declining if name == "lambda" else wired[name]
        ))
    }))
    out = index.handler(_sns(INTAKE_AGE), None)
    assert out["outcome"]["result"]["ok"] is False
    assert "FAILED" in wired["pages"][-1]


# ── The dumbness invariant (§4 inv. 3) ──────────────────────────────────────


def test_no_agent_bus_or_queue_dependency_in_the_source():
    """§4 inv. 3: the backstop stays dumb forever.

    Asserted over the parsed AST, not the raw text — the module docstring
    NAMES the forbidden dependencies in order to explain why they are absent,
    so a substring scan would flag its own rationale. Imports and attribute
    calls are the things that can actually erode the invariant.
    """
    import ast

    tree = ast.parse(Path(index.__file__).read_text())

    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])

    forbidden_imports = {"krepis", "flow_doctor", "nousergon_lib", "anthropic",
                         "openai", "requests"}
    assert not (imported & forbidden_imports), (
        f"{sorted(imported & forbidden_imports)} imported by the backstop "
        f"responder — it must have no agent or bus dependency "
        f"(overseer-policy §4 inv. 3)"
    )
    # One list, referenced twice — the assertion and its message drifted apart
    # once already, and a message naming a stale set sends the reader looking
    # for an import that is not the one that failed.
    allowed = {"__future__", "json", "os", "urllib", "datetime", "pathlib",
               "boto3"}
    assert imported <= allowed, (
        f"unexpected import(s) {sorted(imported - allowed)} "
        f"— the backstop's dependency set is boto3 + stdlib, by design"
    )

    called = {n.func.attr for n in ast.walk(tree)
              if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    forbidden_calls = {"receive_message", "delete_message", "publish"}
    assert not (called & forbidden_calls), (
        f"{sorted(called & forbidden_calls)} called — the backstop must never "
        f"consume the intake queue or re-publish onto the bus it may be rescuing"
    )


def test_unreadable_state_still_claims_so_the_second_firing_escalates(wired, monkeypatch):
    """Regression anchor for the unbounded-retry loop.

    The first version returned early when the state read failed, so the claim
    was never written — every subsequent firing also read-failed and also
    acted. Unbounded retries in the one component whose whole contract is
    'one bounded attempt'.
    """
    broken = FakeS3(raise_on_get=True)
    monkeypatch.setattr(index, "boto3", type("B", (), {
        "client": staticmethod(lambda name, region_name=None: (
            broken if name == "s3" else wired[name]
        ))
    }))
    index.handler(_sns(INTAKE_AGE), None)
    assert broken.puts, "the claim must be written even when the read failed"

    # Second firing: the claim is now present, so the read succeeds and escalates.
    broken.raise_on_get = False
    out = index.handler(_sns(INTAKE_AGE), None)
    assert out["outcome"].get("escalated") is True
    assert len(wired["lambda"].invocations) == 1, "must not act twice in-window"


def test_queue_state_reports_depth_and_age_without_an_invalid_attribute(wired):
    """Regression anchor: age comes from CloudWatch, depth from SQS. Asking SQS
    for the age attribute fails the whole call and loses the depth too."""
    out = index.handler(_sns("unmapped-for-this-test"), None)
    intake = out["evidence"]["queues"]["intake"]
    assert "UNREADABLE" not in intake, intake
    assert "99 visible" in intake
    assert "oldest 2h00m" in intake


OWN_ALARM = "alpha-engine-watch-plane-backstop-responder-errors"


def test_the_responders_own_alarm_is_never_actionable(wired):
    """The responder's own error alarm publishes to the topic it subscribes to,
    so it invokes itself. That is safe ONLY because its own alarm has no
    allowlist entry — it reports and stops. If someone ever adds one, this
    fails. (The human leg of that topic is an email subscription sharing no
    component with the responder, which is what satisfies inv. 1; the
    self-invoke is a harmless second reader, not the terminating path.)"""
    assert OWN_ALARM not in index.ALARM_ACTIONS
    out = index.handler(_sns(OWN_ALARM), None)
    assert wired["lambda"].invocations == []
    assert "allowlist" in out["outcome"]["skipped"]
    assert wired["pages"], "it must still page with full plane state"


# ── Property 5: only ALARM is an incident (alpha-engine-config-I8109) ───────
#
# The responder read `AlarmName` and `NewStateReason` and never
# `NewStateValue`, so a recovery rendered identically to an outage: the
# "🚨 OVERSEER BACKSTOP" header, the full plane-state dump, and a RECOVERY
# block. 45 alarms in this account carry `OKActions` pointing at the backstop
# topic, so this was a class over all of them.
#
# Measured instance: `alpha-engine-pipeline-deadman-weekly-freshness` was
# created 2026-08-21 14:32 PDT, sat INSUFFICIENT_DATA, and transitioned to OK
# at 15:10 PDT — firing OKActions and paging Brian as an emergency about a
# pipeline that was fine. CloudWatch's OK-reason text reuses the phrase
# "Threshold Crossed", which is why the reason string cannot substitute for
# the state field.


def _sns_state(alarm: str, state: str, reason: str = "Threshold Crossed") -> dict:
    return {"Records": [{"Sns": {"Message": json.dumps(
        {"AlarmName": alarm, "NewStateReason": reason,
         "NewStateValue": state})}}]}


def test_ok_transition_does_not_page_as_an_incident(wired):
    index.handler(_sns_state(INTAKE_AGE, "OK"), None)
    page = wired["pages"][0]
    assert "🚨" not in page, "a recovery must not carry the incident siren"
    assert "RECOVERED" in page
    assert "PLANE STATE" not in page, (
        "the evidence dump exists to help someone act; on a green transition "
        "there is nothing to act on"
    )


def test_insufficient_data_does_not_page_as_an_incident(wired):
    index.handler(_sns_state(INTAKE_AGE, "INSUFFICIENT_DATA"), None)
    page = wired["pages"][0]
    assert "🚨" not in page
    assert "no data" in page


def test_a_non_alarm_state_never_attempts_recovery(wired):
    """INTAKE_AGE is in the allowlist and WOULD dispatch on ALARM."""
    out = index.handler(_sns_state(INTAKE_AGE, "OK"), None)
    assert wired["lambda"].invocations == [], (
        "a recovery notice must not trigger a recovery action"
    )
    assert out["outcome"]["skipped"].startswith("state=OK")


def test_an_ok_transition_does_not_consume_the_cooldown_window(wired):
    """A recovery must not spend the incident budget of the incident it ends.

    The cooldown is what makes a second ALARM inside the window escalate as
    "the earlier recovery did NOT fix this". If an OK transition claimed the
    window, the next real firing would be misread as a second attempt.
    """
    index.handler(_sns_state(INTAKE_AGE, "OK"), None)
    index.handler(_sns(INTAKE_AGE), None)
    assert wired["lambda"].invocations, (
        "the ALARM after the OK must still be treated as a FIRST attempt"
    )


def test_alarm_state_is_unchanged_by_the_new_branch(wired):
    """Regression guard: the incident path keeps every property above."""
    out = index.handler(_sns_state(INTAKE_AGE, "ALARM"), None)
    page = wired["pages"][0]
    assert "🚨 OVERSEER BACKSTOP" in page
    assert "PLANE STATE" in page
    assert "RECOVERY" in page
    assert out["state"] == "ALARM"


def test_an_unparseable_event_is_treated_as_an_incident(wired):
    """Fail toward the loud side: a malformed event is never downgraded."""
    out = index.handler({"Records": [{"Sns": {"Message": "{not json"}}]}, None)
    assert out["state"] == "ALARM"
    assert "🚨" in wired["pages"][0]


def test_a_missing_state_field_is_treated_as_an_incident(wired):
    """Older SNS shapes, and every existing test's payload, omit the field."""
    out = index.handler(_sns(INTAKE_AGE), None)
    assert out["state"] == "ALARM"


# ── Property 6: a paused probe is state, not blindness ─────────────────────
#
# `_probe_health()` read only `AWS/Lambda Invocations`, so the liveness probe
# Brian disabled on 2026-08-07 (alpha-engine-config-I6617) was reported in
# every page as "the probe is not running at all" — which reads as plane-wide
# blindness rather than as his own ruling. overseer-policy §7: a deliberate
# operator disable is state, surfaced in every assurance report, never paged.


def _silence_probe_metrics(wired):
    """Zero Invocations and zero Errors — the shape a switched-off probe has."""
    wired["cloudwatch"].get_metric_statistics = (
        lambda **kwargs: {"Datapoints": []}
    )


def test_a_paused_probe_reports_as_paused_not_as_dead(wired, monkeypatch):
    monkeypatch.setattr(index, "_paused_trigger_names",
                        lambda: set(index.PROBE_TRIGGERS))
    _silence_probe_metrics(wired)
    health = index._probe_health()
    assert "PAUSED by declaration" in health
    assert "BY DESIGN" in health


def test_a_probe_with_a_live_trigger_still_reports_as_dead(wired, monkeypatch):
    monkeypatch.setattr(index, "_paused_trigger_names",
                        lambda: {index.PROBE_TRIGGERS[0]})
    _silence_probe_metrics(wired)
    health = index._probe_health()
    assert "not running at all" in health


def test_an_unreadable_manifest_does_not_claim_the_probe_is_paused(
        wired, monkeypatch):
    monkeypatch.setattr(index, "_paused_trigger_names", lambda: None)
    _silence_probe_metrics(wired)
    health = index._probe_health()
    assert "UNREADABLE" in health
    assert "PAUSED by declaration" not in health
