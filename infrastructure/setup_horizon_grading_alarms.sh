#!/usr/bin/env bash
# setup_horizon_grading_alarms.sh — RETIRED as an alarm-creation path.
#
# alpha-engine-config-I7359, executing the I7359 ownership ruling. Both alarms
# this script used to create are codified in the PRIVATE nous-ergon-ops repo:
# infrastructure/cloudwatch/alarms/alpha-engine-universe-returns-horizon-lag.json
# and alpha-engine-predictor-outcomes-grading-lag.json. Edit those files
# there; do not edit or run this script.
# nousergon-data/tests/test_no_imperative_alarm_authorship.py fails the build
# if `put-metric-alarm` reappears here.
#
#   infrastructure/cloudwatch/apply.py --name alpha-engine-universe-returns-horizon-lag alpha-engine-predictor-outcomes-grading-lag
#
set -euo pipefail
echo "setup_horizon_grading_alarms.sh no longer creates alarms (alpha-engine-config-I7359)."
echo "Edit the two alpha-engine-{universe-returns-horizon-lag,predictor-outcomes-grading-lag}.json files in nous-ergon-ops instead."
exit 0
