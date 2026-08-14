#!/usr/bin/env bash
# setup_changelog_observability_alarms.sh — RETIRED as an alarm-creation path.
#
# alpha-engine-config-I7359, executing the I7359 ownership ruling. All eight
# alarms this script used to create are newly codified in the PRIVATE
# nous-ergon-ops repo (same PR that retired this script) under
# infrastructure/cloudwatch/alarms/alpha-engine-lambda-errors-*.json. Edit
# those files there; do not edit or run this script.
# nousergon-data/tests/test_no_imperative_alarm_authorship.py fails the build
# if `put-metric-alarm` reappears here.
#
#   infrastructure/cloudwatch/apply.py --prefix alpha-engine-lambda-errors-
#
set -euo pipefail
echo "setup_changelog_observability_alarms.sh no longer creates alarms (alpha-engine-config-I7359)."
echo "Edit nous-ergon-ops/infrastructure/cloudwatch/alarms/alpha-engine-lambda-errors-*.json instead."
echo "To apply immediately: nous-ergon-ops/infrastructure/cloudwatch/apply.py --prefix alpha-engine-lambda-errors-"
exit 0
