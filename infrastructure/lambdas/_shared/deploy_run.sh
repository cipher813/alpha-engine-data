# shellcheck shell=bash
#
# deploy_run.sh — shared status-propagating command runner + post-upload
# code verification, sourced by every lambda's deploy.sh.
#
# WHY (alpha-engine-config-I8033): every deploy.sh in this repo defined its
# own run() as `if $DRY_RUN; then echo ...; else "$@"; fi` — a plain
# function whose failure is NOT reliably fatal even under `set -euo
# pipefail`:
#
#   1. bash's errexit is suppressed for a command inside an if/else BODY
#      when that same function has, earlier in the script, been invoked as
#      part of an if/while/until CONDITION or an && / || list — a
#      documented bash quirk, not a bug in any one script, and one every
#      copy of the old run() was exposed to identically.
#   2. Independent of (1): `aws lambda update-function-code` on a 25MB
#      artifact was measured 2026-08-21 to print
#      "aws: [ERROR]: Connection was closed before we received a valid
#      response..." to stderr and still return exit 0. No amount of shell-
#      side error propagation catches a CLI that misreports its own exit
#      status — set -e is necessary but not sufficient.
#
# run() below closes (1): it captures $? explicitly right after the command
# and calls `exit` itself rather than depending on errexit noticing a
# nonzero return somewhere up the call chain. verify_code_deployed() closes
# (2): it is the only check here that does not trust the CLI's reported
# exit code at all — it reads back the function's live CodeSha256 and
# compares it to a locally-computed digest of the artifact just built.
#
# Usage (matches the calling convention every deploy.sh already uses):
#   # shellcheck source=infrastructure/lambdas/_shared/deploy_run.sh
#   source "${SCRIPT_DIR}/../_shared/deploy_run.sh"
#   run aws lambda update-function-code --function-name "$FUNCTION_NAME" \
#     --zip-file "fileb://$ZIP" --region "$REGION" \
#     --query 'LastUpdateStatus' --output text
#   if ! $DRY_RUN; then
#     aws lambda wait function-updated --function-name "$FUNCTION_NAME" --region "$REGION"
#   fi
#   verify_code_deployed "$FUNCTION_NAME" "$REGION" "$ZIP"
#
# Reads $DRY_RUN if the caller defines it (as most deploy.sh scripts do);
# defaults to false (real run) when unset, so a caller with no --dry-run flag
# of its own (thinktank-spot-dispatcher) does not need to add one.
#
# TRANSPORT DECISION (alpha-engine-config-I8033 deliverable 3): every
# deploy.sh here uploads via a direct `--zip-file fileb://...` PUT, never
# via S3 (`--code S3Bucket=...`). Moving to S3 was evaluated and REJECTED for
# now: it would add an S3 write step plus an `s3:PutObject` grant to every
# one of the 43 lambdas' iam-policy.json (each independently reviewed —
# infrastructure/iam/README.md's single-writer rule), a materially larger
# IAM surface, for a failure that verify_code_deployed() above already turns
# loud regardless of transport. The connection-close measured 2026-08-21 was
# 1 failure in this repo's deploy history, not a recurring pattern; a bigger
# transport migration is disproportionate to that base rate. Tracked as a
# follow-up (alpha-engine-config-I8043) rather than silently dropped.
#
# What ships instead: retry envvars, set here so every deploy.sh gets them
# without an operator having to rediscover
# `AWS_RETRY_MODE=adaptive AWS_MAX_ATTEMPTS=10` (what actually cleared the
# measured failure) by hand. `:-` so an operator's own environment always
# wins. adaptive mode also backs off client-side request rate on repeated
# throttling/connection errors, not just retrying blindly.
export AWS_RETRY_MODE="${AWS_RETRY_MODE:-adaptive}"
export AWS_MAX_ATTEMPTS="${AWS_MAX_ATTEMPTS:-10}"

run() {
  if ${DRY_RUN:-false}; then
    echo "DRY: $*"
    return 0
  fi
  local status=0
  "$@" || status=$?
  if [ "${status}" -ne 0 ]; then
    echo "ERROR: command failed (exit ${status}): $*" >&2
    exit "${status}"
  fi
  return 0
}

# verify_code_deployed <function-name> <region> <zip-path>
#
# Reads back the LIVE CodeSha256 after an update-function-code /
# create-function call and asserts it matches the artifact just built —
# independent of whatever exit code the upload call reported or printed.
# Lambda's CodeSha256 is the base64-encoded SHA-256 digest of the deployed
# zip; computing the same digest locally and comparing is authoritative
# regardless of CLI behaviour.
#
# Call this AFTER `aws lambda wait function-updated` (when not DRY_RUN) so
# the read observes the settled state, not an in-flight update.
verify_code_deployed() {
  local function_name="${1:?verify_code_deployed: function name required}"
  local region="${2:?verify_code_deployed: region required}"
  local zip_path="${3:?verify_code_deployed: zip path required}"

  if ${DRY_RUN:-false}; then
    echo "DRY: would verify CodeSha256 for ${function_name} against ${zip_path}"
    return 0
  fi

  if [ ! -r "${zip_path}" ]; then
    echo "ERROR: verify_code_deployed: artifact not readable: ${zip_path}" >&2
    exit 1
  fi

  # python3 rather than openssl: every deploy.sh in this repo already depends
  # on python3 (see _shared/pause.sh's identical rationale for jq), so this
  # adds no new dependency, and it sidesteps any cross-platform difference in
  # how `openssl base64` wraps or trails its output between the OpenSSL and
  # LibreSSL builds operators and CI runners carry.
  local expected actual
  expected="$(python3 -c '
import base64, hashlib, sys
with open(sys.argv[1], "rb") as fh:
    digest = hashlib.sha256(fh.read()).digest()
print(base64.b64encode(digest).decode("ascii"))
' "${zip_path}")"

  if ! actual="$(aws lambda get-function --function-name "${function_name}" \
        --region "${region}" --query 'Configuration.CodeSha256' --output text 2>&1)"; then
    echo "ERROR: verify_code_deployed: could not read back live CodeSha256 for ${function_name}: ${actual}" >&2
    exit 1
  fi

  if [ "${expected}" != "${actual}" ]; then
    echo "ERROR: code verification FAILED for ${function_name} in ${region}." >&2
    echo "ERROR:   expected CodeSha256 (built artifact ${zip_path}): ${expected}" >&2
    echo "ERROR:   live CodeSha256:                                  ${actual}" >&2
    echo "ERROR: the upload call may have reported success while shipping nothing" >&2
    echo "ERROR: new — the function's live code is UNCHANGED (alpha-engine-config-I8033)." >&2
    exit 1
  fi

  echo "  ✓ verified live CodeSha256 matches ${zip_path}"
}
