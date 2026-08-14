#!/usr/bin/env bash
# setup_watch_plane_alarms.sh — RETIRED as an alarm-creation path.
#
# alpha-engine-config-I7359, executing the I7359 ownership ruling: an alarm is
# an account resource (infrastructure-ownership-policy.md §2), and this repo
# is PUBLIC while the alarm definitions and applier now live in the PRIVATE
# nous-ergon-ops repo. This script's `put-metric-alarm` calls are gone —
# nousergon-data/tests/test_no_imperative_alarm_authorship.py fails the build
# if they come back.
#
# Every alarm this script used to create is codified as a JSON file under
# nous-ergon-ops/infrastructure/cloudwatch/alarms/alpha-engine-watch-plane-*.json
# (alpha-engine-config-I7340/nous-ergon-ops-PR675). To change one, edit that
# file in nous-ergon-ops and open a PR there — never edit this script and run
# it. To apply a change immediately rather than waiting for the next merge
# touching that file, an operator with nous-ergon-ops checked out runs:
#
#   infrastructure/cloudwatch/apply.py --prefix alpha-engine-watch-plane-
#
# .github/workflows/deploy-watch-plane-alarms.yml, which used to invoke this
# script on every push touching it, is retired in the same PR — the merge
# trigger now lives in nous-ergon-ops's cloudwatch-alarm-apply-on-merge.yml,
# which fires on the alarm FILE, not on a copy of the bash that used to write
# it.
#
# This file is a pointer, not a stub with hidden behavior: it does nothing and
# exits 0 so a stale muscle-memory invocation is a no-op, not a failure.

set -euo pipefail
echo "setup_watch_plane_alarms.sh no longer creates alarms (alpha-engine-config-I7359)."
echo "Edit nous-ergon-ops/infrastructure/cloudwatch/alarms/alpha-engine-watch-plane-*.json instead."
echo "To apply immediately: nous-ergon-ops/infrastructure/cloudwatch/apply.py --prefix alpha-engine-watch-plane-"
exit 0
