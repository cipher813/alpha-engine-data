#!/usr/bin/env bash
# setup_research_runner_timeout_alarm.sh — RETIRED as an alarm-creation path.
#
# alpha-engine-config-I7359, executing the I7359 ownership ruling. The alarm
# this script used to create is codified in the PRIVATE nous-ergon-ops repo:
# infrastructure/cloudwatch/alarms/alpha-engine-research-runner-timeout.json.
# Edit it there; do not edit or run this script.
# nousergon-data/tests/test_no_imperative_alarm_authorship.py fails the build
# if `put-metric-alarm` reappears here.
#
#   infrastructure/cloudwatch/apply.py --name alpha-engine-research-runner-timeout
#
set -euo pipefail
echo "setup_research_runner_timeout_alarm.sh no longer creates alarms (alpha-engine-config-I7359)."
echo "Edit nous-ergon-ops/infrastructure/cloudwatch/alarms/alpha-engine-research-runner-timeout.json instead."
exit 0
