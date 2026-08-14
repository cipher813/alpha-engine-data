# shellcheck shell=bash
#
# pause.sh — resolve a trigger's intended state from the automation-pause manifest.
#
# Source this from any deploy.sh that creates or updates an EventBridge rule or
# an EventBridge Scheduler schedule, and pass the result as --state:
#
#   source "${SCRIPT_DIR}/../_shared/pause.sh"
#   aws events put-rule --name "${RULE}" --state "$(pause_state "${RULE}")" ...
#
# WHY (alpha-engine-config-I6619). Brian's 2026-08-07 ruling paused 40 scheduled
# triggers; infrastructure/automation_pause.json records which. Neither
# `aws events put-rule` nor `aws scheduler create-schedule|update-schedule`
# accepts a "leave the state alone" option — both DEFAULT TO ENABLED when
# --state is omitted, and update-schedule is a full replace. So every deploy.sh
# that reconciles its own triggers silently un-paused them on the next redeploy.
# The pause survived only by detection: automation_pause.py --check goes red the
# next morning. This closes it at the write site instead.
#
# Fail-OPEN on purpose. If the manifest is missing or unreadable this returns
# ENABLED — the pre-pause behaviour — rather than disabling a trigger nobody
# asked to disable. The asymmetry is deliberate: a pause that silently SPREADS
# would stop the weekly SF with no signal that a config file was the cause,
# while a pause that silently LIFTS is caught by automation_pause.py --check the
# next morning. One failure mode has a detector; the other does not.

# Absolute path to the manifest, resolved from THIS file's location so a
# deploy.sh may be invoked from any cwd (they are, by CI and by operators).
_PAUSE_MANIFEST="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/automation_pause.json"

# pause_state <trigger-name> -> "DISABLED" if the manifest pauses it, else "ENABLED".
#
# Matches on the trigger NAME across both surfaces. EventBridge rules and
# Scheduler schedules are different APIs but share one namespace here, and
# tests/test_automation_pause.py asserts no name appears on both, so a single
# lookup is unambiguous.
pause_state() {
  local name="${1:?pause_state: trigger name required}"

  if [ ! -r "${_PAUSE_MANIFEST}" ]; then
    echo "ENABLED"
    return 0
  fi

  # python3 rather than jq: every deploy.sh in this repo already depends on
  # python3, none depends on jq, and adding a dependency to 26 scripts to read
  # one JSON file is not worth it.
  python3 - "${_PAUSE_MANIFEST}" "${name}" <<'PY'
import json
import sys

try:
    with open(sys.argv[1], encoding="utf-8") as fh:
        manifest = json.load(fh)
    paused = manifest["paused"]
    names = set(paused["events_rules"]) | set(paused["scheduler_schedules"])
    # `pending` = paused, but not created live yet. Same write-time behaviour;
    # it exists as a separate block only because automation_pause.py --check
    # requires every `paused` entry to exist live. Keys prefixed `_` are notes.
    names |= {k for k in manifest.get("pending", {}) if not k.startswith("_")}
except Exception:
    # Unreadable or malformed manifest -> pre-pause behaviour. See the
    # fail-open note at the top of this file.
    print("ENABLED")
else:
    print("DISABLED" if sys.argv[2] in names else "ENABLED")
PY
}

# alarm_actions_flag <alarm-name> -> the put-metric-alarm flag controlling paging.
#
# The alarm-side twin of pause_state, and it exists for the identical reason:
# `aws cloudwatch put-metric-alarm` is an UPSERT of the whole alarm with no
# "leave the actions alone" option, and it RESETS ActionsEnabled to true. So
# every setup_*_alarms.sh that reconciles its own alarms silently re-armed the
# ones the pause had deliberately silenced, on every run.
#
# Measured 2026-08-14 (alpha-engine-config-I7023): the class re-armed eleven
# paused-component alarms twice in one afternoon — once from
# setup_watch_plane_alarms.sh via its deploy workflow, and once from a different
# provisioner entirely, which is what showed this was never a one-script defect.
# Six scripts in this repo call put-metric-alarm.
#
# FAIL-LOUD, diverging from pause_state's documented fail-open. That asymmetry
# was argued on the grounds that one direction has a detector and the other does
# not. For alarms BOTH directions now have one — `alarm-unexpectedly-enabled`
# and `armed-but-silenced` in automation_pause.py --check — so refusing to guess
# costs nothing, and the thing being avoided is paging a human at 3am about a
# component that is off on purpose. A provisioner that cannot read the manifest
# should stop, not arm everything.
alarm_actions_flag() {
  local name="${1:?alarm_actions_flag: alarm name required}"

  if [ ! -r "${_PAUSE_MANIFEST}" ]; then
    echo "alarm_actions_flag: cannot read ${_PAUSE_MANIFEST}" >&2
    return 1
  fi

  python3 - "${_PAUSE_MANIFEST}" "${name}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as fh:
    manifest = json.load(fh)

paused = manifest["paused"]
paused_triggers = set(paused["events_rules"]) | set(paused["scheduler_schedules"])
paused_triggers |= {k for k in manifest.get("pending", {}) if not k.startswith("_")}

entry = manifest.get("paused_alarms", {}).get(sys.argv[2])
# Justification is computed here exactly as automation_pause.alarm_justified()
# computes it — every watched trigger paused, and an entry watching nothing is
# never justified. Duplicated in two languages, pinned by
# tests/test_automation_pause.py::test_shared_helper_matches_alarm_justified.
watches = (entry or {}).get("watches") or []
silenced = bool(watches) and all(w in paused_triggers for w in watches)
print("--no-actions-enabled" if silenced else "--actions-enabled")
PY
}
