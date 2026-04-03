#!/usr/bin/env bash
# rag/pipelines/run_weekly_ingestion.sh — Weekly RAG ingestion pipeline.
#
# Runs all ingestion pipelines in sequence:
#   1. SEC filings (10-K/10-Q) — from signals universe, 2y lookback
#   2. 8-K material events — from signals universe, 1y lookback
#   3. Earnings transcripts (Finnhub) — from signals universe, latest 8
#   4. Thesis history — from research.db (incremental)
#   5. Filing change detection — analyze consecutive filings
#
# Intended to run on the Saturday spot instance alongside the backtester,
# or as a standalone cron job on the always-on EC2 instance.
#
# Usage:
#   bash rag/pipelines/run_weekly_ingestion.sh              # full run
#   bash rag/pipelines/run_weekly_ingestion.sh --dry-run    # preview only
#
# Prerequisites:
#   - .env with RAG_DATABASE_URL, VOYAGE_API_KEY, FINNHUB_API_KEY
#   - research.db available locally or fetchable from S3

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

# Parse flags
DRY_RUN=""
for arg in "$@"; do
    case "$arg" in
        --dry-run) DRY_RUN="--dry-run" ;;
    esac
done

# Activate venv
if [ -d ".venv" ]; then
    source .venv/bin/activate
elif [ -d "venv" ]; then
    source venv/bin/activate
fi

echo "========================================"
echo "RAG Weekly Ingestion — $(date -u '+%Y-%m-%d %H:%M UTC')"
echo "========================================"

# ── Step 1: SEC filings (10-K/10-Q) ─────────────────────────────────────────
echo ""
echo "==> Step 1/5: SEC filings (10-K/10-Q)..."
python -m rag.pipelines.ingest_sec_filings --from-signals --lookback-years 2 $DRY_RUN 2>&1 || \
    echo "  WARNING: SEC filing ingestion failed (non-fatal)"

# ── Step 2: 8-K material events ─────────────────────────────────────────────
echo ""
echo "==> Step 2/5: 8-K material events..."
python -m rag.pipelines.ingest_8k_filings --from-signals --lookback-days 365 $DRY_RUN 2>&1 || \
    echo "  WARNING: 8-K ingestion failed (non-fatal)"

# ── Step 3: Earnings transcripts (Finnhub) ──────────────────────────────────
echo ""
echo "==> Step 3/5: Earnings transcripts (Finnhub)..."
if [ -n "${FINNHUB_API_KEY:-}" ]; then
    python -m rag.pipelines.ingest_earnings_finnhub --from-signals --max-per-ticker 8 $DRY_RUN 2>&1 || \
        echo "  WARNING: Finnhub transcript ingestion failed (non-fatal)"
else
    echo "  SKIPPED: FINNHUB_API_KEY not set"
fi

# ── Step 4: Thesis history (v2 quant/qual from signals.json) ─────────────────
echo ""
echo "==> Step 4/5: Thesis history..."
SINCE=$(date -u -d '14 days ago' '+%Y-%m-%d' 2>/dev/null || date -u -v-14d '+%Y-%m-%d')
python -m rag.pipelines.ingest_theses --signals --since "$SINCE" $DRY_RUN 2>&1 || \
    echo "  WARNING: Thesis ingestion failed (non-fatal)"

# ── Step 5: Filing change detection ──────────────────────────────────────────
echo ""
echo "==> Step 5/5: Filing change detection..."
if [ -z "$DRY_RUN" ]; then
    python -m rag.pipelines.filing_change_detection --output-s3 2>&1 || \
        echo "  WARNING: Filing change detection failed (non-fatal)"
else
    echo "  SKIPPED in dry-run mode"
fi

echo ""
echo "========================================"
echo "RAG Weekly Ingestion Complete — $(date -u '+%Y-%m-%d %H:%M UTC')"
echo "========================================"

# Emit CloudWatch heartbeat on successful completion
aws cloudwatch put-metric-data \
  --namespace "AlphaEngine" \
  --metric-name "Heartbeat" \
  --dimensions "Process=rag-ingestion" \
  --value 1 --unit "Count" \
  --region "${AWS_REGION:-us-east-1}" 2>/dev/null \
  && echo "Heartbeat emitted: rag-ingestion" \
  || echo "WARNING: Failed to emit heartbeat (non-fatal)"
