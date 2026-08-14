#!/usr/bin/env bash
# setup_substrate_alarms.sh — RETIRED as an alarm-creation path.
#
# alpha-engine-config-I7359, executing the I7359 ownership ruling. Every alarm
# this script used to create is codified in the PRIVATE nous-ergon-ops repo
# under infrastructure/cloudwatch/alarms/alpha-engine-substrate-*.json. Edit
# those files there; do not edit or run this script.
# nousergon-data/tests/test_no_imperative_alarm_authorship.py fails the build
# if `put-metric-alarm` reappears here.
#
#   infrastructure/cloudwatch/apply.py --prefix alpha-engine-substrate-
#
set -euo pipefail
echo "setup_substrate_alarms.sh no longer creates alarms (alpha-engine-config-I7359)."
echo "Edit nous-ergon-ops/infrastructure/cloudwatch/alarms/alpha-engine-substrate-*.json instead."
echo "To apply immediately: nous-ergon-ops/infrastructure/cloudwatch/apply.py --prefix alpha-engine-substrate-"
exit 0
