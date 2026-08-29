# shellcheck shell=bash
#
# apply_iam_policy.sh — shared idempotent IAM role/policy apply, sourced by
# every lambda's deploy.sh (alpha-engine-config#2825).
#
# WHY this exists: every lambda deploy.sh applied its iam-policy.json ONLY
# inside its `--bootstrap` block (first-time creation). Ordinary merges to
# main re-deploy CODE only (`deploy.sh` flagless, the CI auto-deploy path
# and the documented default) and never re-ran `put-role-policy` — so a
# post-bootstrap iam-policy.json edit silently drifted from live until an
# operator happened to know to re-run --bootstrap (undocumented tribal
# knowledge; confirmed as the root cause of 9 of 10 findings in
# nousergon-data-PR784's extended drift-check coverage). Each deploy.sh now
# also exposes a standalone `--apply-iam` flag (same pattern
# changelog-incident-mirror/deploy.sh already used for this, config#865)
# that calls this function directly, so re-applying a changed policy no
# longer requires the full (slower, more side-effectful) --bootstrap path.
#
# Deliberately requires an operator to run --apply-iam by hand rather than
# wiring this into CI: the github-actions-lambda-deploy OIDC role
# intentionally lacks iam:CreateRole/iam:PutRolePolicy fleet-wide, a
# boundary adopted after 4 IAM-clobber incidents in 2 months (see
# infrastructure/iam/README.md "Single-writer rule"). check-drift.py
# (config#2340 surface 3) is the automated half of this pair: it now covers
# every lambda exec role and runs on every PR + daily, so a future
# iam-policy.json edit that isn't re-applied still gets caught quickly
# instead of drifting silently for weeks.
#
# Expects the caller to already define `run()` (the $DRY_RUN-aware command
# wrapper) and `$DRY_RUN`, exactly as every deploy.sh already does.
#
# probe_role_presence <role_name> -> prints "present" | "absent" | "unknown"
#
# WHY THIS EXISTS (the four-permanently-red-deploys defect, 2026-08-28).
#
# The old existence check was:
#
#   if ! aws iam get-role --role-name "$r" ... >/dev/null 2>&1; then  # CREATE
#
# `2>&1` to /dev/null makes AccessDenied and NoSuchEntity the same observation,
# and the `!` branch reads BOTH as "the role does not exist". The CI auto-deploy
# identity `github-actions-lambda-deploy` holds `iam:GetRole` on exactly two
# roles (alpha-engine-eventbridge-sfn-role, alpha-engine-step-functions-role)
# and on NO lambda execution role — by design, per the single-writer rule. So
# every code-only deploy that reached this helper probed a role it may not read,
# was denied, concluded "absent", printed `Creating IAM role: <name>` and called
# `aws iam create-role` — a grant it also does not hold and must not hold.
#
# A denied READ is not evidence of absence. Distinguishing the two is the whole
# job here, and it is why the stderr is captured instead of discarded.
probe_role_presence() {
  local role_name="${1:?probe_role_presence: role name required}"
  local err rc=0
  err="$(aws iam get-role --role-name "${role_name}" \
           --query 'Role.RoleName' --output text 2>&1 >/dev/null)" || rc=$?
  if [ "${rc}" -eq 0 ]; then
    echo present
    return 0
  fi
  # Only an explicit NoSuchEntity proves absence. Everything else — AccessDenied,
  # an expired token, a throttle, a network failure — is `unknown`.
  if printf '%s' "${err}" | grep -qiE 'NoSuchEntity|cannot be found|does not exist'; then
    echo absent
    return 0
  fi
  echo unknown
  return 0
}

# Usage: apply_iam_policy <role_name> <policy_name> <policy_file> <trust_policy_json> [may_create_role]
#
# `may_create_role` (default "true") governs the ROLE-CREATION half only. The
# policy apply below runs either way. See probe_role_presence and
# apply_iam_policy_on_deploy for why the auto-apply path passes "false".
apply_iam_policy() {
  local role_name="$1" policy_name="$2" policy_file="$3" trust_policy="$4"
  local may_create_role="${5:-true}"

  local presence
  presence="$(probe_role_presence "${role_name}")"
  case "${presence}" in
    present)
      echo "  IAM role exists: ${role_name}"
      ;;
    absent)
      if [ "${may_create_role}" != "true" ]; then
        # alpha-engine-config-I9045 / the four-red-deploys defect. Creating a
        # role is a BOOTSTRAP act, and the single-writer rule (infrastructure/
        # iam/README.md) reserves it for an operator. The auto-apply-on-deploy
        # path must never attempt it: it holds no iam:CreateRole by design, so
        # the attempt can only ever fail, and failing here aborts a deploy
        # whose CODE has already shipped.
        echo "  IAM role ABSENT: ${role_name} — not creating it from this path." >&2
        echo "  This is the code-only auto-apply path, which is deliberately not a" >&2
        echo "  role creator (single-writer rule). Run this deploy.sh --bootstrap as" >&2
        echo "  an operator to create it. Skipping the policy apply too: there is no" >&2
        echo "  role to put it on." >&2
        APPLY_IAM_POLICY_VERDICT="role-absent"
        return 0
      fi
      echo "  Creating IAM role: ${role_name}"
      run aws iam create-role \
        --role-name "${role_name}" \
        --assume-role-policy-document "${trust_policy}" \
        --query 'Role.RoleName' --output text
      if ! $DRY_RUN; then
        echo "  Waiting 10s for IAM role propagation..."
        sleep 10
      fi
      ;;
    *)
      # `unknown` — the probe was DENIED or failed for a reason that is not
      # NoSuchEntity. It is NOT evidence of absence, and the old code treated
      # it as exactly that (see probe_role_presence). Fall through to the
      # policy apply, which is the call whose own success or AccessDenied is
      # the real answer.
      echo "  IAM role presence UNKNOWN for ${role_name} (the get-role probe was" >&2
      echo "  denied or unreadable). Not creating anything on an unknown; the" >&2
      echo "  policy apply below is the authoritative attempt." >&2
      ;;
  esac

  # Report whether this apply actually CHANGES anything
  # (alpha-engine-config-I7444).
  #
  # put-role-policy is idempotent, so re-applying an unchanged document
  # succeeds exactly like a real change and the caller's "IAM applied" line
  # prints either way. The command is therefore indistinguishable from a
  # no-op by reading its output — which is precisely how, on 2026-08-16, an
  # operator ran this from a checkout on `main` while the changed
  # iam-policy.json lived on an unmerged PR branch, saw the success line,
  # and reasonably believed the grant had landed. It had not: the file that
  # was applied was main's, identical to what was already live. The
  # iam-policy-change-guard stayed red and the cause was invisible.
  #
  # Comparing live against the file first costs one read and converts an
  # unfalsifiable "applied" into a statement with content. Best-effort by
  # design: if the read or the comparison cannot be done, say so and apply
  # anyway — a diagnostic must never be able to block the apply itself.
  local live_doc="" verdict="unknown"
  if live_doc="$(aws iam get-role-policy \
        --role-name "${role_name}" \
        --policy-name "${policy_name}" \
        --query 'PolicyDocument' --output json 2>/dev/null)"; then
    if command -v python3 >/dev/null 2>&1; then
      verdict="$(python3 -c '
import json, sys
try:
    live = json.loads(sys.argv[1])
    disk = json.load(open(sys.argv[2]))
except Exception:
    print("unknown"); raise SystemExit(0)
# Key-order- and whitespace-insensitive: only the semantic document matters.
print("same" if json.dumps(live, sort_keys=True) == json.dumps(disk, sort_keys=True)
      else "different")
' "${live_doc}" "${policy_file}" 2>/dev/null || echo unknown)"
    fi
  else
    # No inline policy of that name yet — a first apply is by definition a change.
    verdict="absent"
  fi

  echo "  Applying inline policy: ${policy_name}"
  case "${verdict}" in
    same)
      echo "  NOTE: live policy already matches ${policy_file} — this apply is a NO-OP." >&2
      echo "  NOTE: If you expected a change, you are almost certainly running from the" >&2
      echo "  NOTE: wrong checkout: this reads iam-policy.json from the working tree, so" >&2
      echo "  NOTE: a policy edit that lives on an unmerged branch requires running from" >&2
      echo "  NOTE: THAT BRANCH (alpha-engine-config-I7444)." >&2
      ;;
    different) echo "  live policy DIFFERS from ${policy_file} — applying the change." ;;
    absent)    echo "  no inline policy ${policy_name} on the role yet — first apply." ;;
    *)         echo "  (could not compare live policy against ${policy_file}; applying anyway)" >&2 ;;
  esac

  run aws iam put-role-policy \
    --role-name "${role_name}" \
    --policy-name "${policy_name}" \
    --policy-document "file://${policy_file}"

  APPLY_IAM_POLICY_VERDICT="${verdict}"
}

# apply_iam_policy_on_deploy — the "#4472 auto-apply on merge" call site.
#
# Same arguments as apply_iam_policy. Tolerates EXACTLY ONE failure: the
# caller is an identity without iam:PutRolePolicy / iam:CreateRole. That is
# the CI auto-deploy role by design (see the single-writer note above), and
# check-drift.py is its backstop. EVERY OTHER failure is a broken applier and
# exits non-zero, which aborts the deploy under the callers' `set -euo
# pipefail`.
#
# WHY this exists (alpha-engine-config-I7338). The four #4472 call sites each
# wrote their own tolerance inline:
#
#   apply_iam_policy ... || echo "WARN: IAM auto-apply failed (expected in CI
#                                 — role lacks iam:PutRolePolicy)"
#
# That `||` swallowed every cause and asserted one. On 2026-08-14, running as
# `ne-admin` — an identity that DOES hold iam:PutRolePolicy —
# alert-drain-liveness-probe/deploy.sh printed:
#
#   deploy.sh: line 188: apply_iam_policy: command not found
#   WARN: IAM auto-apply failed (expected in CI — role lacks iam:PutRolePolicy)
#
# It had never sourced this file. The WARN named a cause that could not have
# been true, read as benign, and the auto-apply feature had therefore never
# executed on that lambda since #4472 shipped — which is the content-drift
# half of alpha-engine-config-I6299.
#
# Two properties close that class, and both depend on the tolerance living
# HERE rather than at the call site:
#
#   1. A `command not found` (rc 127) reaches the classifier as an unmatched
#      stderr string and is reported LOUDLY, not as a permission note.
#   2. If a deploy.sh forgets to source this file at all, the call site has no
#      `||` of its own, so `apply_iam_policy_on_deploy: command not found`
#      aborts the deploy under `set -e` instead of printing a reassurance.
#
# tests/test_deploy_shell_functions_are_defined.py is the derived guard for
# the sourcing itself, so a 35th deploy.sh cannot repeat this.
apply_iam_policy_on_deploy() {
  local role_name="${1:?apply_iam_policy_on_deploy: role name required}"
  local policy_name="${2:?apply_iam_policy_on_deploy: policy name required}"
  local policy_file="${3:?apply_iam_policy_on_deploy: policy file required}"
  local trust_policy="${4:?apply_iam_policy_on_deploy: trust policy required}"

  # stderr is captured to a file rather than streamed through a process
  # substitution: `tee` in a `>(...)` is not reaped by the `||`, so the
  # classifier can race the flush and read an EMPTY stderr for a genuine
  # AccessDenied. Captured stderr is replayed verbatim on both paths below,
  # so nothing is hidden — only deferred to the end of the command.
  local err_file rc=0 stderr_text
  err_file="$(mktemp)"

  # THE SUBSHELL IS LOAD-BEARING (the four-permanently-red-deploys defect).
  #
  # `run()` in _shared/deploy_run.sh calls `exit`, not `return` — deliberate,
  # and correct, per alpha-engine-config-I8033. But `exit` inside a function
  # terminates the SHELL, and `|| rc=$?` guards a non-zero RETURN. There is no
  # return to guard, so the moment run() started exiting (merged 2026-08-21),
  # the classifier below became unreachable: a denied `aws iam create-role`
  # killed the whole deploy at exit 254, the captured stderr in "${err_file}"
  # was never replayed, and the CI log ended at `Creating IAM role: <name>`
  # with no AWS error text at all. Four workflows went red on every run for a
  # week and the log could not say why.
  #
  # This is the SAME class alpha-engine-config-I8125 fixed for `run ... || true`
  # call sites with run_tolerating(). It fixed the call-site shape and missed
  # this one — a FUNCTION-level tolerance wrapper. A subshell closes it
  # generally: an `exit` inside `( ... )` sets the subshell's exit status, which
  # `||` does catch, so run()'s fail-loud semantics are preserved everywhere and
  # this one classifier still gets to run.
  #
  # Cost of the subshell: assignments inside it do not escape, so
  # APPLY_IAM_POLICY_VERDICT is round-tripped on stdout's last line below
  # rather than read from the variable.
  local out_file
  out_file="$(mktemp)"
  (
    apply_iam_policy "${role_name}" "${policy_name}" "${policy_file}" \
      "${trust_policy}" "false"
    printf 'APPLY_IAM_POLICY_VERDICT=%s\n' "${APPLY_IAM_POLICY_VERDICT:-unknown}"
  ) >"${out_file}" 2>"${err_file}" || rc=$?

  # Replay stdout minus the verdict marker, and lift the verdict out of the
  # subshell.
  APPLY_IAM_POLICY_VERDICT="$(sed -n 's/^APPLY_IAM_POLICY_VERDICT=//p' "${out_file}" | tail -1)"
  grep -v '^APPLY_IAM_POLICY_VERDICT=' "${out_file}" || true
  rm -f "${out_file}"

  stderr_text="$(cat "${err_file}" 2>/dev/null || true)"
  rm -f "${err_file}"

  if [ "${rc}" -eq 0 ]; then
    if [ -n "${stderr_text}" ]; then
      printf '%s\n' "${stderr_text}" >&2
    fi
    return 0
  fi

  # The ONLY tolerated cause. Matched against what the AWS CLI actually
  # emits for a denied IAM write: an explicit `AccessDenied`/
  # `AccessDeniedException` error code, the `is not authorized to perform`
  # sentence, or an SCP/boundary explicit deny.
  if printf '%s' "${stderr_text}" | grep -qiE \
      'AccessDenied|is not authorized to perform|explicit deny|ExpiredToken|InvalidClientTokenId|UnrecognizedClientException'; then
    echo "WARN: IAM auto-apply skipped — this caller lacks iam:PutRolePolicy/iam:CreateRole." >&2
    echo "WARN: role=${role_name} policy=${policy_name}. This is expected for the CI" >&2
    echo "WARN: auto-deploy OIDC role (single-writer rule). check-drift.py is the backstop;" >&2
    echo "WARN: an operator must run this deploy.sh --apply-iam to land the change." >&2
    # Replay the AWS error VERBATIM even though this failure is tolerated. The
    # header above has claimed since it was written that "captured stderr is
    # replayed verbatim on both paths below" — it was not, on this path, and
    # nothing noticed because the assertion lived only in a comment. A
    # tolerated failure whose cause never prints is how a WRONG tolerance
    # survives: `AccessDenied` matched on some OTHER operation than the one
    # assumed reads exactly like the expected boundary.
    printf 'WARN: --- captured stderr (tolerated) ---\n%s\nWARN: --- end stderr ---\n' \
      "${stderr_text}" >&2
    return 0
  fi

  # Anything else is a BROKEN APPLIER, not a permission boundary. Loud, and
  # non-zero so `set -e` aborts the deploy rather than shipping code whose
  # IAM policy silently did not move.
  echo "ERROR: IAM auto-apply FAILED for role=${role_name} policy=${policy_name} (exit ${rc})." >&2
  echo "ERROR: This is NOT the known CI permission boundary — the stderr below carries no" >&2
  echo "ERROR: AccessDenied. The apply mechanism itself is broken; live IAM is now unknown" >&2
  echo "ERROR: relative to ${policy_file}." >&2
  if [ -n "${stderr_text}" ]; then
    printf 'ERROR: --- captured stderr ---\n%s\nERROR: --- end stderr ---\n' "${stderr_text}" >&2
  else
    echo "ERROR: (the failing command produced no stderr — exit ${rc} alone)" >&2
  fi
  return "${rc}"
}
