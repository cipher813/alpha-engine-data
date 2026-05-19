#!/usr/bin/env bash
# infrastructure/backfill_reference_price_cache.sh — Wave 3 PR1 one-shot seed.
#
# Copies every object under ``s3://alpha-engine-research/predictor/price_cache/``
# to ``s3://alpha-engine-research/reference/price_cache/`` byte-for-byte. Run
# ONCE as part of the PR1 deploy — the producer write-both in
# ``collectors/prices.py`` + ``collectors/fred_history.py`` +
# ``weekly_collector.py`` only mirrors writes for STALE tickers, so the new
# prefix needs this initial seed before the write-both soak clock starts.
#
# Idempotent. ``aws s3 sync`` skips objects that already match by size +
# last-modified, so re-running is safe and incremental.
#
# Usage:
#   bash infrastructure/backfill_reference_price_cache.sh              # real copy
#   bash infrastructure/backfill_reference_price_cache.sh --dry-run    # plan only

set -euo pipefail

BUCKET="${ALPHA_ENGINE_BUCKET:-alpha-engine-research}"
LEGACY_PREFIX="predictor/price_cache/"
NEW_PREFIX="reference/price_cache/"

DRY_RUN=""
if [[ "${1:-}" == "--dry-run" ]]; then
    DRY_RUN="--dryrun"
    echo "[backfill] DRY-RUN: planning copy without writing"
fi

echo "[backfill] Wave 3 PR1 seed: ${LEGACY_PREFIX} -> ${NEW_PREFIX} on s3://${BUCKET}"

# Pre-flight: legacy must exist + have objects
LEGACY_COUNT=$(aws s3 ls "s3://${BUCKET}/${LEGACY_PREFIX}" --recursive --summarize \
    | awk '/Total Objects:/ {print $3}')
if [[ -z "${LEGACY_COUNT}" || "${LEGACY_COUNT}" -lt 100 ]]; then
    echo "[backfill] ERROR: legacy prefix has ${LEGACY_COUNT:-0} objects (expected >100)." >&2
    echo "[backfill]   refusing to seed an empty/sparse mirror." >&2
    exit 1
fi
echo "[backfill] legacy: ${LEGACY_COUNT} objects"

# aws s3 sync: copy with delta-only semantics (skips objects matching by
# size + mtime). The first run copies everything; subsequent runs are
# fast no-ops unless the legacy prefix has been refreshed.
aws s3 sync \
    "s3://${BUCKET}/${LEGACY_PREFIX}" \
    "s3://${BUCKET}/${NEW_PREFIX}" \
    --only-show-errors \
    ${DRY_RUN}

if [[ -n "${DRY_RUN}" ]]; then
    echo "[backfill] DRY-RUN complete — no objects written."
    exit 0
fi

# Post-flight: parity check on object counts
NEW_COUNT=$(aws s3 ls "s3://${BUCKET}/${NEW_PREFIX}" --recursive --summarize \
    | awk '/Total Objects:/ {print $3}')
echo "[backfill] new prefix: ${NEW_COUNT} objects (legacy: ${LEGACY_COUNT})"

if [[ "${NEW_COUNT}" -lt "${LEGACY_COUNT}" ]]; then
    echo "[backfill] WARN: new prefix has fewer objects than legacy." >&2
    echo "[backfill]   delta=${LEGACY_COUNT} - ${NEW_COUNT}. Re-run to converge." >&2
    exit 2
fi

echo "[backfill] OK — reference/price_cache/ seeded. Soak clock starts on the"
echo "[backfill]   next Saturday SF firing's first write to both prefixes."
