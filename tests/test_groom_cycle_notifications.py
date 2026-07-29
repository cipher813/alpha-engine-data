"""tests/test_groom_cycle_notifications.py — cycle-level groom lifecycle pings.

Guards the 2026-07-28 operator ruling: the groom must emit ONE buzzing STARTED
ping and ONE buzzing COMPLETE roll-up per trigger cycle, from the ORCHESTRATOR
rather than the box.

Why it exists. Before this, terminal reporting lived exclusively in
``groom_run.sh``'s §7, at ``severity=info`` — which ``FleetTelegramTopic.GROOM``
delivers with ``disable_notification=True``. Two independent consequences, both
observed live on 2026-07-28:

  * a FINISHED message was delivered but never buzzed, so the operator's lived
    experience was "start + errors only"; and
  * a per-box trap structurally cannot report a box that never reached it. On
    the 12:00Z cycle all three lanes were reclaimed
    ``instance-terminated-no-capacity`` two minutes after launch. Zero
    notifications fired on any rail and the SF sat in ``TaskSubmitted`` for four
    hours (config-I4987/I4989). ``GroomDispatchComplete`` was a bare ``Succeed``.

The tests below pin the three things that make the fix real rather than
cosmetic: the SF actually routes through the notifier on BOTH terminal paths,
the notifier is total (no lane silently omitted from the roll-up), and the
buzz-vs-silent posture is explicit rather than inherited.

The schedule-lockstep test is a separate concern riding the same PR: the
Overseer's ``run_window`` liveness check enumerates expected triggers from
``playbooks.yaml``, and that list had drifted three retirements behind the live
crons (config-I4988), so the one check that should have paged on 2026-07-27/28
structurally could not fire. A mirrored schedule with nothing enforcing the
mirror is the §2.7 derive-once defect; this test is the enforcement.
"""

import importlib.util
import json
import re
import sys
import types
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
SF_FILE = REPO_ROOT / "infrastructure" / "step_function_groom.json"
DISPATCHER_DIR = REPO_ROOT / "infrastructure" / "lambdas" / "scheduled-groom-dispatcher"
DISPATCHER_DEPLOY = DISPATCHER_DIR / "deploy.sh"
PLAYBOOKS = REPO_ROOT / "infrastructure" / "overseer" / "playbooks.yaml"
NOTIFIER_DEPLOY = (
    REPO_ROOT / "infrastructure" / "lambdas" / "sf-telegram-notifier" / "deploy.sh"
)


@pytest.fixture(scope="module")
def states():
    return json.loads(SF_FILE.read_text())["States"]


@pytest.fixture(scope="module")
def dispatcher():
    """Import the dispatcher Lambda with its AWS/lib imports stubbed.

    The module is a Lambda entrypoint, not a package — it imports boto3 and
    nousergon_lib at module scope. Only the pure notification-shaping helpers
    are under test here, so the heavy imports are satisfied with stubs rather
    than pulled in for real.
    """
    def stub(name, **attrs):
        """Install a stub ONLY if the real module is genuinely unavailable."""
        try:
            importlib.import_module(name)
            return
        except ImportError:
            pass
        mod = types.ModuleType(name)
        for k, v in attrs.items():
            setattr(mod, k, v)
        sys.modules[name] = mod

    stub("boto3", client=lambda *a, **k: None)  # noqa: ARG005
    stub("flow_doctor_telegram", notify_via_flow_doctor=lambda *a, **k: True)  # noqa: ARG005
    # nousergon_lib.spot_dispatch imports `from krepis import alerts` at module
    # scope; krepis is a runtime dep of the Lambda bundle, not of this test
    # suite's env.
    stub("krepis")
    stub("krepis.alerts", publish=lambda *a, **k: None)  # noqa: ARG005
    if "krepis" in sys.modules and not hasattr(sys.modules["krepis"], "alerts"):
        sys.modules["krepis"].alerts = sys.modules["krepis.alerts"]

    sys.path.insert(0, str(REPO_ROOT))
    spec = importlib.util.spec_from_file_location(
        "groom_dispatcher_under_test", DISPATCHER_DIR / "index.py"
    )
    module = importlib.util.module_from_spec(spec)
    # Deliberately NOT wrapped in try/pytest.skip. An unimportable dispatcher
    # must FAIL this suite, not quietly skip 9 assertions — a skipped guard is
    # indistinguishable from a passing one in CI output, which is the same
    # absence-of-signal defect these tests exist to prevent (policy §2.4).
    spec.loader.exec_module(module)
    return module


# ── SF wiring ────────────────────────────────────────────────────────────────


def test_notify_cycle_complete_exists_and_invokes_the_dispatcher(states):
    st = states["NotifyCycleComplete"]
    assert st["Type"] == "Task"
    assert st["Resource"] == "arn:aws:states:::lambda:invoke"
    payload = st["Parameters"]["Payload"]
    assert payload["mode"] == "cycle_complete"
    assert (
        st["Parameters"]["FunctionName"] == "alpha-engine-scheduled-groom-dispatcher"
    )


def test_cycle_notify_passes_the_state_root_not_named_fields(states):
    """$.mapFailure is ABSENT on the two healthy paths.

    A ``Parameters`` reference to an absent field raises ``States.Runtime``,
    which no ``Catch`` can intercept — groom-sweep-policy §2.2 requires that
    class be eliminated by construction. Passing the root and reading with
    ``.get()`` in Python is that construction; a future edit that "helpfully"
    cherry-picks $.mapFailure here would reintroduce an uncatchable failure on
    the SUCCESS path only, which is exactly the shape that survives review.
    """
    payload = states["NotifyCycleComplete"]["Parameters"]["Payload"]
    assert payload["cycle.$"] == "$"
    flat = json.dumps(payload)
    assert "$.mapFailure" not in flat
    assert "$.sweep" not in flat


def test_both_terminal_paths_route_through_the_cycle_notification(states):
    """Success AND failure must both be reported.

    A cycle ending FAILED is the one whose roll-up the operator most needs, so
    the notifier sits ahead of CheckMapLaunchOutcome rather than on either
    branch of it.
    """
    check = states["CheckMapLaunchOutcome"]
    assert check["Choices"][0]["Next"] == "GroomMapLaunchFailed"
    assert check["Default"] == "GroomDispatchComplete"

    feeders = [
        name for name, st in states.items()
        if st.get("Next") == "CheckMapLaunchOutcome"
        or any(c.get("Next") == "CheckMapLaunchOutcome" for c in st.get("Catch", []))
    ]
    # Only the notifier itself (success Next + best-effort Catch) may feed the
    # outcome check. Anything else means a path bypasses the roll-up.
    assert feeders == ["NotifyCycleComplete"], (
        f"{feeders} reach CheckMapLaunchOutcome directly — every converging "
        "path must pass through NotifyCycleComplete first, or that cycle "
        "terminates with no operator notification (the 12:00Z silence)"
    )


def test_cycle_notification_is_best_effort_and_cannot_change_the_outcome(states):
    """Policy §8 carve-out: a notify hiccup must never fail the execution."""
    st = states["NotifyCycleComplete"]
    catch = st["Catch"][0]
    assert catch["ErrorEquals"] == ["States.ALL"]
    assert catch["Next"] == "CheckMapLaunchOutcome"
    assert catch["ResultPath"] == "$.cycleNotifyError"
    assert st["ResultPath"] == "$.cycleNotify"
    # ResultPath must not clobber the state root — $.mapFailure has to survive
    # this state or a failed cycle would report SUCCEEDED.
    assert st["ResultPath"] not in ("$", None)
    assert "TimeoutSeconds" in st, "no unbounded state (policy §2.1)"


# ── Notification shaping ─────────────────────────────────────────────────────


def test_every_lane_appears_in_the_rollup_including_unrecognized_shapes(dispatcher):
    """Totality, not prettiness (policy §2.4 no-silent-default).

    A lane whose outcome this code did not anticipate must still produce a row.
    A lane silently missing from the summary is precisely the failure the ping
    exists to surface.
    """
    rows, degraded = dispatcher._lane_rows([
        {"issue_filter": "low-only", "groomLaunch": {"launched": True,
                                                     "completion": "success"}},
        {"issue_filter": "mid-only", "laneOutcome": {"laneFailed": True,
                                                     "reason": "timeout"}},
        {"issue_filter": "high-only"},                      # no outcome at all
        {"issue_filter": "weird", "groomLaunch": {"completion": "invented_state"}},
        "not-even-a-dict",                                  # malformed
    ])
    assert len(rows) == 5, "a lane was dropped from the roll-up"
    assert degraded is True
    joined = "\n".join(rows)
    assert "low-only" in joined and "mid-only" in joined and "high-only" in joined
    assert "no outcome reported" in joined
    assert "invented_state" in joined, "an unknown terminal state must render as itself"
    assert "malformed outcome" in joined


def test_all_clean_lanes_report_not_degraded(dispatcher):
    rows, degraded = dispatcher._lane_rows([
        {"issue_filter": "low-only", "groomLaunch": {"completion": "success"}},
        {"issue_filter": "mid-only", "groomLaunch": {"completion": "success"}},
    ])
    assert degraded is False
    assert len(rows) == 2


def test_unknown_terminal_state_is_never_silently_treated_as_healthy(dispatcher):
    assert dispatcher._lane_glyph("success") == "✅"
    assert dispatcher._lane_glyph("a-state-invented-next-month") == "❔"


def test_cycle_pings_are_buzzing_not_silent(dispatcher):
    """The whole point of the ruling.

    The canonical GROOM topic spec is ``notify_on=("info",)`` +
    ``disable_notification=True``. Cycle-level pings override BOTH: severities
    above info are otherwise DROPPED, not merely silenced.
    """
    ov = dispatcher._CYCLE_NOTIFIER_OVERRIDES
    assert ov["disable_notification"] is False
    assert set(ov["notify_on"]) >= {"info", "warning", "error", "critical"}


def test_cycle_flow_name_is_distinct_from_the_silent_per_box_rail(dispatcher):
    """flow_doctor_telegram caches the built config by flow_name.

    Sharing one with the silent rail would serve whichever posture initialized
    first — a coin-flip between buzzing and silent, per cold start.
    """
    assert dispatcher._CYCLE_FLOW_NAME != dispatcher._FLOW_NAME
    assert dispatcher._CYCLE_DB_BASENAME != dispatcher._DB_BASENAME


def test_zero_lane_cycles_are_announced_not_skipped(dispatcher, monkeypatch):
    """"No ping today" must never be readable as either "nothing was due" or
    "the trigger never fired" — that ambiguity cost an operator session on
    2026-07-27 (policy §2.4)."""
    sent = []
    monkeypatch.setattr(dispatcher, "_notify_cycle",
                        lambda text, **kw: sent.append((text, kw)))
    dispatcher._notify_cycle_started({"launches": [], "reason": "below floor"},
                                     "0 12 * * *")
    assert len(sent) == 1
    assert "0 lanes" in sent[0][0]
    assert "sweep still runs" in sent[0][0]


def test_every_decide_response_flows_through_one_notification_chokepoint():
    """A per-branch notify would be forgotten on exactly the quiet branches."""
    src = (DISPATCHER_DIR / "index.py").read_text()
    # The chokepoint's own return is the ONE legitimate construction of this
    # shape; every other occurrence is a branch that skipped the ping.
    bypasses = [
        line for line in src.splitlines()
        if 'return {"decide"' in line and "decide_payload" not in line
    ]
    assert not bypasses, (
        f"decide_only response(s) bypass _decide_result: {bypasses} — their "
        "cycle STARTED ping would never fire"
    )
    assert src.count("return _decide_result(") >= 4


def test_cycle_complete_handler_is_total(dispatcher, monkeypatch):
    """It sits on the SF's path to BOTH terminal states.

    An exception here would convert a completed cycle into States.TaskFailed —
    the notification is the least important thing in the execution and must
    never be able to change its outcome.
    """
    def boom(*_a, **_k):
        raise RuntimeError("telegram down")

    monkeypatch.setattr(dispatcher, "_notify_cycle_complete", boom)
    out = dispatcher._handle_cycle_complete({"cycle": {}})
    assert out["cycleNotify"]["notified"] is False
    assert "telegram down" in out["cycleNotify"]["error"]


def test_cycle_complete_never_claims_the_sweep_finished(dispatcher, monkeypatch):
    """DispatchEndOfSfSweep is a plain lambda:invoke, not .waitForTaskToken.

    At roll-up time we can honestly report only that the sweep BOX launched.
    Claiming completion we did not observe is the §2.8 defect — a record
    asserting an action that never happened.
    """
    sent = []
    monkeypatch.setattr(dispatcher, "_notify_cycle",
                        lambda text, **kw: sent.append(text))
    dispatcher._notify_cycle_complete({
        "schedInput": {"schedule": "0 20 * * *"},
        "mapOutcome": [{"issue_filter": "low-only",
                        "groomLaunch": {"completion": "success"}}],
        "sweep": {"launched": True},
    })
    body = sent[0]
    assert "box launched" in body
    assert "quiescent" not in body.lower()


def test_a_failed_cycle_reports_degraded(dispatcher, monkeypatch):
    sent = []
    monkeypatch.setattr(dispatcher, "_notify_cycle",
                        lambda text, **kw: sent.append((text, kw)))
    dispatcher._notify_cycle_complete({
        "schedInput": {"schedule": "0 12 * * *"},
        "mapOutcome": [{"issue_filter": "low-only",
                        "laneOutcome": {"laneFailed": True, "reason": "timeout"}}],
        "sweep": {"dispatched": False, "reason": "sweep_dispatch_failed"},
        "mapFailure": {"failed": True, "reason": "one_or_more_lanes_failed"},
    })
    text, kw = sent[0]
    assert "degraded" in text
    assert kw["severity"] == "warning", "a degraded cycle must not ping as routine info"
    assert "not dispatched" in text


# ── Schedule lockstep (config-I4988) ─────────────────────────────────────────


def _deploy_sched_crons() -> list[tuple[int, int]]:
    """(hour, minute) of every cron in the dispatcher's SCHED_CRONS array."""
    src = DISPATCHER_DEPLOY.read_text()
    block = re.search(r"SCHED_CRONS=\((.*?)\n\)", src, re.S)
    assert block, "SCHED_CRONS array not found in scheduled-groom-dispatcher/deploy.sh"
    out = []
    for minute, hour in re.findall(r'"cron\((\d+)\s+(\d+)\s', block.group(1)):
        out.append((int(hour), int(minute)))
    assert out, "no cron expressions parsed from SCHED_CRONS"
    return sorted(out)


def _registry_run_window_schedule() -> list[tuple[int, int]]:
    doc = yaml.safe_load(PLAYBOOKS.read_text())
    groom = doc["playbooks"]["groom"] if "playbooks" in doc else doc["groom"]
    checks = groom["liveness"]["checks"]
    rw = [c for c in checks if c["type"] == "run_window"]
    assert len(rw) == 1
    return sorted((s["hour"], s["minute"]) for s in rw[0]["schedule"])


def test_liveness_run_window_schedule_matches_the_deployed_crons():
    """config-I4988: the mirror must be enforced, not merely asserted in a comment.

    ``_rw_expected_triggers`` enumerates from the registry, so a stale list
    means the check accounts for triggers that never fire and is blind to the
    ones that do — which is why the groom's own liveness check could not fire
    across four consecutive failed cycles.
    """
    assert _registry_run_window_schedule() == _deploy_sched_crons(), (
        "playbooks.yaml groom run_window schedule has drifted from "
        "scheduled-groom-dispatcher/deploy.sh SCHED_CRONS — the liveness check "
        "is enumerating triggers that do not fire (config-I4988)"
    )


def test_groom_sf_failures_reach_telegram():
    """The groom SF's own SNS publishes go to `alpha-engine-alerts`, which has
    NO Telegram subscriber — so a failed groom SF reached email and the intake
    bus but never Telegram. Successes are deliberately NOT in this rule: the SF
    emits its own richer cycle roll-up, and adding them here would double-report.
    """
    src = NOTIFIER_DEPLOY.read_text()
    assert "alpha-engine-groom-sf-failure" in src
    block = src[src.index("GROOM_EVENT_PATTERN=") :]
    pattern = json.loads(re.search(r"\{.*?\n\}\n", block, re.S).group(0))
    assert pattern["detail"]["stateMachineArn"] == [
        "arn:aws:states:${REGION}:${ACCOUNT_ID}:stateMachine:alpha-engine-groom-dispatch"
    ]
    assert set(pattern["detail"]["status"]) == {"FAILED", "TIMED_OUT", "ABORTED"}
    assert "SUCCEEDED" not in pattern["detail"]["status"]
    assert "RUNNING" not in pattern["detail"]["status"]
