#!/usr/bin/env bash
# deploy.sh — Build and deploy the Phase 2 alternative data Lambda.
#
# Container image deployment (ECR) — same pattern as research + predictor.
# Function: alpha-engine-data-collector
#   - Timeout: 900s (15 min, Lambda max standard — was 600s for ~30 tickers;
#     now servicing 900+ tickers via concurrent ThreadPoolExecutor)
#   - Memory: 1024 MB (see LAMBDA_MEMORY_MB below)
#   - Triggered by Step Functions (Saturday pipeline)
#
# Memory/timeout are applied on EVERY deploy, not just on create (2026-07-27).
# Previously they were passed only to `create-function`, so on an existing
# function the values were whatever someone last set by hand — un-versioned
# live state that silently drifts from this file.
#
# That drift caused the 2026-07-25 DataPhase2 failure. Memory had been raised
# to 1024 MB on $LATEST, but `publish-version` snapshots configuration at
# publish time and the `live` alias still pointed at version 269 — published
# at 512 MB. The Saturday SF invokes `alpha-engine-data-collector:live`, so it
# kept running at 512 MB and hit Runtime.OutOfMemory twice (REPORT lines show
# maxMemoryUsed == memorySize == 512 on two ~460s invocations). The raise was
# real, and completely invisible to the pipeline.
#
# Hence the ordering below: update code -> wait -> update CONFIG -> wait ->
# publish-version -> repoint alias. Configuration must land on $LATEST before
# the version is cut, or the alias serves a version that never saw it.
#
# Prerequisites:
#   1. AWS CLI configured with appropriate credentials
#   2. IAM role created (alpha-engine-data-role)
#   3. ECR repository: alpha-engine-data-collector
#   4. Docker installed and running
#   5. Secrets (FMP_API_KEY, FINNHUB_API_KEY, EDGAR_IDENTITY, …) present in SSM
#      Parameter Store under /alpha-engine/* — the Lambda reads them at runtime
#      via get_secret(); no .env is built into or sourced by this deploy
#      (config#890 .env→SSM arc).
#
# Usage: ./infrastructure/deploy.sh

set -euo pipefail

FUNCTION_NAME="alpha-engine-data-collector"
REGION="${AWS_REGION:-us-east-1}"
BUCKET="alpha-engine-research"

# Declared runtime envelope — the single source of truth for this function's
# memory/timeout. Overridable for one-off experiments, but the committed
# default is what every deploy converges the live function to.
#
# 1024 MB: phase-2 invocations OOM'd at 512 MB (2026-07-25). Non-phase-2 runs
# sit at 236-341 MB observed, so 512 was adequate for them and masked the
# phase-2 requirement. Sized at 2x the OOM ceiling rather than a hair above it
# because the OOM'd invocations were also running ~460s of a 600s timeout, and
# Lambda scales CPU with memory — the headroom buys margin on both axes.
LAMBDA_MEMORY_MB="${LAMBDA_MEMORY_MB:-1024}"
LAMBDA_TIMEOUT="${LAMBDA_TIMEOUT:-900}"

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "$REGION" 2>/dev/null || echo "ACCOUNT_ID")
ROLE_ARN="${LAMBDA_ROLE_ARN:-arn:aws:iam::${ACCOUNT_ID}:role/alpha-engine-data-role}"
ECR_REPO="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${FUNCTION_NAME}"

# ── Build and deploy ───────────────────────────────────────────────────────

echo "=== Building container image for $FUNCTION_NAME ==="

# alpha-engine-lib is installed inside the Dockerfile via pip from
# git+https://github.com/nousergon/nousergon-lib@v0.3.0 (public repo
# since 2026-05-03). No vendor staging needed.

docker build --platform linux/amd64 --provenance=false -t "$FUNCTION_NAME:latest" .

echo "Authenticating with ECR..."
aws ecr get-login-password --region "$REGION" | \
  docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

aws ecr describe-repositories --repository-names "$FUNCTION_NAME" --region "$REGION" &>/dev/null || \
  aws ecr create-repository --repository-name "$FUNCTION_NAME" --region "$REGION" > /dev/null

echo "Pushing image to ECR..."
docker tag "$FUNCTION_NAME:latest" "$ECR_REPO:latest"
docker push "$ECR_REPO:latest"
IMAGE_URI="$ECR_REPO:latest"

echo "Deploying $FUNCTION_NAME..."

if aws lambda get-function --function-name "$FUNCTION_NAME" --region "$REGION" &>/dev/null; then
  EXISTING_PKG=$(aws lambda get-function-configuration \
    --function-name "$FUNCTION_NAME" --region "$REGION" \
    --query "PackageType" --output text 2>/dev/null || echo "Zip")

  if [ "$EXISTING_PKG" = "Image" ]; then
    aws lambda update-function-code \
      --function-name "$FUNCTION_NAME" \
      --image-uri "$IMAGE_URI" \
      --region "$REGION" > /dev/null

    # Converge configuration to the declared envelope on every deploy. Must
    # complete BEFORE publish-version below, which snapshots $LATEST's config
    # into the version the `live` alias will serve.
    aws lambda wait function-updated \
      --function-name "$FUNCTION_NAME" --region "$REGION" 2>/dev/null || sleep 5
    echo "  Applying declared config: ${LAMBDA_MEMORY_MB}MB / ${LAMBDA_TIMEOUT}s..."
    aws lambda update-function-configuration \
      --function-name "$FUNCTION_NAME" \
      --timeout "$LAMBDA_TIMEOUT" \
      --memory-size "$LAMBDA_MEMORY_MB" \
      --region "$REGION" > /dev/null
  else
    echo "  Migrating from zip to container image..."
    aws lambda delete-function --function-name "$FUNCTION_NAME" --region "$REGION"
    sleep 2
    aws lambda create-function \
      --function-name "$FUNCTION_NAME" \
      --package-type Image \
      --code "ImageUri=$IMAGE_URI" \
      --role "$ROLE_ARN" \
      --timeout "$LAMBDA_TIMEOUT" \
      --memory-size "$LAMBDA_MEMORY_MB" \
      --region "$REGION" > /dev/null
  fi
else
  aws lambda create-function \
    --function-name "$FUNCTION_NAME" \
    --package-type Image \
    --code "ImageUri=$IMAGE_URI" \
    --role "$ROLE_ARN" \
    --timeout "$LAMBDA_TIMEOUT" \
    --memory-size "$LAMBDA_MEMORY_MB" \
    --region "$REGION" > /dev/null
fi
echo "  $FUNCTION_NAME deployed."

# ── Converge the Lambda environment (alpha-engine-config-I7925) ─────────────
# $FUNCTION_NAME was one of eleven fleet-wide Lambdas carrying a GITHUB_TOKEN
# set by hand and refreshed by nothing — no repo, IaC file or script here
# ever wrote this function's environment. The environment carried a STALE
# COPY of the credential — set from an older SSM parameter version and
# never re-derived on deploy — that GitHub rejected while the SSM
# parameter's own value remained valid the whole time
# (alpha-engine-config-I7968 tracks the mis-attribution). On 2026-08-21
# a first-party dependency picked it up out of site-packages,
# sent it to GitHub, got a 401, and halted the preopen trading pipeline
# (alpha-engine-config-I7924). alpha-engine-predictor-inference
# (crucible-predictor) was the one of eleven that broke and was fixed first
# (crucible-predictor-PR535) — this mirrors that same convergence.
#
# DENY-LIST, deliberately, not an allow-list: this function carries
# operator-set flags and provider keys codified nowhere (FMP_API_KEY,
# FINNHUB_API_KEY, EDGAR_IDENTITY, …), and asserting a complete key set
# would delete them. The allow-list end state is tracked separately
# (alpha-engine-config-I7958).
#
# Read-modify-write via the shared `krepis.aws remove-lambda-env` CLI — a
# bare `aws lambda update-function-configuration --environment` REPLACES the
# whole variable map.
#
# `--defer-publish`: this function is alias-pinned (the Saturday SF invokes
# `${FUNCTION_NAME}:live`, per the header note above) and `publish-version`
# below snapshots configuration into the version the alias will serve — so
# the removal must land on $LATEST BEFORE that publish, exactly like the
# memory/timeout convergence this script already performs. Placed after
# publish-version it would mutate $LATEST only and be a silent no-op on the
# alias (L4497).
# `--missing-ok`: every deploy after the first finds the key already gone.
LAMBDA_ENV_DENIED_KEYS=(GITHUB_TOKEN)

echo "  Converging Lambda environment (removing denied keys)..."
aws lambda wait function-updated --function-name "$FUNCTION_NAME" --region "$REGION" 2>/dev/null || sleep 5
python3 -m krepis.aws remove-lambda-env \
  --function-name "$FUNCTION_NAME" --region "$REGION" \
  --defer-publish --missing-ok \
  "${LAMBDA_ENV_DENIED_KEYS[@]/#/--unset=}"

# ── Router addressing (alpha-engine-config-I7409) ────────────────────────────
# `$FUNCTION_NAME` never declared KREPIS_EXEC_CONTEXT, so krepis.router
# silently defaulted every flow-doctor LLM-diagnosis call it makes to
# 'laptop' — the only environment where the loopback egress proxy this
# resolves to (127.0.0.1:8990/8980) actually exists — and every diagnosis
# attempt failed RouterUnresolvable (measured live 2026-08-31 against a SPY
# cross-source quarantine alert). `lambda` appears on no registry entry's
# `reachable_from`, deliberately (LLM_MODEL_REGISTRY.yaml): a Lambda has no
# local egress proxy, so the router edge (litellm_proxy) is its only path,
# which model-router-policy §3.4a R27a guarantees is offered in every
# context. Mirrors crucible-research/infrastructure/deploy.sh's
# `_apply_router_env`. Its OWN credential (ROUTER_CONSUMER_DATA), not
# LITELLM_MASTER_KEY — krepis.secrets resolves SSM before os.environ, so a
# shared secret NAME collapses this Lambda into another consumer's identity
# at the edge (nous-ergon-ops-PR951 registers it).
echo "  Applying router addressing (merge, not replace)..."
aws lambda wait function-updated --function-name "$FUNCTION_NAME" --region "$REGION" 2>/dev/null || sleep 5
python3 -m krepis.aws merge-lambda-env \
  --function-name "$FUNCTION_NAME" --region "$REGION" \
  --set "KREPIS_EXEC_CONTEXT=lambda" \
  --set "KREPIS_LITELLM_PROXY_URL=${ROUTER_URL:-https://router.nousergon.ai:8443}" \
  --set "KREPIS_ROUTER_CREDENTIAL_SECRET=${ROUTER_CREDENTIAL_SECRET:-ROUTER_CONSUMER_DATA}" \
  --set "KREPIS_APPCONFIG_APPLICATION=alpha-engine" \
  --set "KREPIS_APPCONFIG_CONFIG_PROFILE=llm-model-registry" \
  --set "KREPIS_APPCONFIG_ENVIRONMENT=production"

# Publish version and update 'live' alias
echo "  Publishing Lambda version..."
aws lambda wait function-updated --function-name "$FUNCTION_NAME" --region "$REGION" 2>/dev/null || sleep 5
VERSION=$(aws lambda publish-version \
  --function-name "$FUNCTION_NAME" \
  --query "Version" --output text \
  --region "$REGION")
echo "  Published version: $VERSION"
aws lambda update-alias \
  --function-name "$FUNCTION_NAME" \
  --name live \
  --function-version "$VERSION" \
  --region "$REGION" 2>/dev/null || \
aws lambda create-alias \
  --function-name "$FUNCTION_NAME" \
  --name live \
  --function-version "$VERSION" \
  --region "$REGION"
echo "  Alias 'live' -> version $VERSION"

# Canary
echo "  Running canary (dry_run=true)..."
CANARY_OUT=$(mktemp)
aws lambda invoke \
  --function-name "${FUNCTION_NAME}:live" \
  --payload '{"phase": 2, "dry_run": true}' \
  --cli-binary-format raw-in-base64-out \
  --region "$REGION" \
  "$CANARY_OUT" > /dev/null

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CANARY_STATUS=$(python3 "$SCRIPT_DIR/canary_status.py" "$CANARY_OUT" 2>/dev/null || echo "PARSE_ERROR")
rm -f "$CANARY_OUT"

if [ "$CANARY_STATUS" != "OK" ] && [ "$CANARY_STATUS" != "SKIPPED" ]; then
  echo "  WARNING: Canary returned '$CANARY_STATUS'"
  echo "  Check CloudWatch logs for details."
  echo "  Rolling back..."
  # Roll back to previous version
  PREV_VERSION=$((VERSION - 1))
  if [ "$PREV_VERSION" -gt 0 ]; then
    aws lambda update-alias \
      --function-name "$FUNCTION_NAME" \
      --name live \
      --function-version "$PREV_VERSION" \
      --region "$REGION" 2>/dev/null || true
    echo "  Rolled back to version $PREV_VERSION"
  fi
  # Independent-channel surveillance per ROADMAP L221 — this exact
  # rollback chain fired silently 10 consecutive times across 2 days
  # (alpha-engine-data #274 retrospective) before Brian noticed the
  # GitHub Actions red-icon. ``dedup_key`` collapses an image-wide
  # rebuild that breaks N Lambdas' canaries within the hour into one
  # alert per (Lambda, version) — lib v0.24.0 substrate (L221
  # retrofit 2026-05-22). Best-effort; trailing || true never
  # overrides the deploy's exit 1.
  # Target is krepis.alerts (config#1339/config#1545): alerts relocated to
  # krepis at nousergon-lib v0.66.0, and the runner-side install this call
  # depends on had drifted to a stale alpha-engine-lib@v0.26.0 pin whose
  # import name (nousergon_lib) never matched this nousergon_lib call —
  # the exact L221 alert this comment describes had been silently dead
  # (ModuleNotFoundError swallowed by || true) since that pin went stale.
  python3 -m krepis.alerts publish \
    --severity error \
    --source "alpha-engine-data/infrastructure/deploy.sh" \
    --dedup-key "canary-fail-${FUNCTION_NAME}-v${VERSION}" \
    --message "Canary rolled back: ${FUNCTION_NAME} canary returned status='${CANARY_STATUS}', live alias reverted v${VERSION}→v${PREV_VERSION}. See CloudWatch /aws/lambda/${FUNCTION_NAME} for payload." \
    || true
  exit 1
fi
echo "  Canary passed (status=$CANARY_STATUS)"

# NOTE: IAM role `alpha-engine-data-role` is a prerequisite (see header).
# It currently exists in AWS and is the execution role for the live
# alpha-engine-data-collector Lambda. A prior version of this script
# tried to `aws iam get-role` as a "create-if-missing" bootstrap and
# fell through to CreateRole when the GetRole call lacked permission —
# masking the permission error as "role not found" (silent fail) and
# then dying loudly on CreateRole. The github-actions-lambda-deploy
# role intentionally lacks iam:* permissions (principle of least
# privilege), so the bootstrap block had been dead code since day one
# of the auto-deploy path. Provisioning this role is a one-time
# operation — do it out of band with a privileged principal, ideally
# by extending infrastructure/iam/ the way #17 did for
# github-actions-lambda-deploy.

echo ""
echo "Deployment complete."
echo ""
