# Incident: ArcticDB macro/SPY stale — 2026-04-15

**Severity:** P1 — Weekday pipeline fully blocked  
**Status:** Partially mitigated (macro/SPY backfilled to 2026-04-14 via ad-hoc script); root cause not fixed  
**Date:** 2026-04-15

---

## 1. Incident Summary

The weekday Step Function (`alpha-engine-weekday-pipeline`) failed on the morning of 2026-04-15 at the PredictorInference Lambda step with:

```
RuntimeError: Pre-flight: ArcticDB macro/SPY last date 2026-04-10 is 5 days stale (threshold 4)
  at alpha-engine-predictor/inference/handler.py:68
  PredictorPreflight → check_arcticdb_fresh("macro", "SPY", max_stale_days=4)
```

Today is Wednesday 2026-04-15. The last macro/SPY write was Friday 2026-04-10 — meaning both Monday (2026-04-13) and Tuesday (2026-04-14) daily runs produced no macro update.

The preflight check `(today - last_ts).days > 4` resolves to `5 > 4 = True` → raises RuntimeError.

---

## 2. Initial Hypothesis vs Actual Root Cause

### Initial hypothesis
The April 14 commit (`606d77f`, "Hard-fail daily_append + propagate exit code in weekday SSM") fixed silent failures in `daily_append.py` and the SSM command, but those changes were never deployed to AWS Step Functions.

**Partially confirmed:** The deployed Step Function had the old SSM command:
```json
["cd /home/ec2-user/alpha-engine-data",
 "set -a && source /home/ec2-user/.alpha-engine.env && set +a",
 "source .venv/bin/activate",
 "python weekly_collector.py --daily 2>&1 | tee /var/log/daily-data.log",
 "echo \"EXIT_CODE=$?\""]
```
The `echo "EXIT_CODE=$?"` at the end always exits 0, masking any Python failures. The fixed version adds `set -eo pipefail` and `git pull --ff-only`. This was deployed at 07:23:49 AM PT 2026-04-15.

### Actual deeper root cause
Even after the Step Function fix, macro/SPY would still never be updated by the daily path. **The macro tickers (SPY, VIX, GLD, etc.) are NOT in the S&P 500/400 constituent list**, so `daily_closes.collect()` never fetches them. The macro write in `daily_append.py` Section 5 is:

```python
for key in macro_keys:
    bar = closes.get(key)   # <- always None — SPY not in daily_closes parquet
    if bar and not np.isnan(bar.get("Close", np.nan)):
        macro_lib.append(key, new_row)  # <- silently skipped every time
```

This is a design gap: the daily pipeline was never wired to update the ArcticDB macro library.

---

## 3. What Was Actually Happening Mon/Tue (Why "ok" Status)

Step Function history confirmed DailyData steps "SUCCEEDED" on both Monday 2026-04-13 and Tuesday 2026-04-14. This is accurate — the stock data collection worked correctly:

- 2026-04-14 parquet: 902 tickers captured (yfinance fetched April 13 closes and stored as April 14)
- `daily_append.py`: stock universe updated successfully; macro write silently skipped (SPY not in closes)
- SSM status "Success" reported even though Python exit code was non-zero in the original error case, because of the `echo EXIT_CODE=0` workaround

The daily runs were **correctly completing their intended scope**: capturing S&P 500/400 OHLCV and computing features. Macro was never part of that intended scope — it's only done on Saturdays via Phase 1.

---

## 4. The Architectural Issue

Macro data (`SPY`, `VIX`, `VIX3M`, `TNX`, `IRX`, `GLD`, `USO`, sector ETFs `XL*`) is written to ArcticDB in two places:

| Path | When | Writes macro to ArcticDB? |
|------|------|--------------------------|
| Saturday Phase 1 → `builders/backfill.py` | Weekly (Saturday) | Yes — full backfill from price_cache parquets |
| Daily → `builders/daily_append.py` Section 5 | Weekday | Only if tickers are in `daily_closes` parquet — **currently never, because macro symbols aren't passed to `daily_closes.collect()`** |

So macro/SPY was last written by the Saturday Phase 1 backfill on 2026-04-12 (the most recent Saturday). By Wednesday 2026-04-15, that's 5 calendar days stale — just over the 4-day threshold.

The 4-day threshold was designed to cover Fri→Tue long weekends + 1 buffer day, but a Sat→Wed span is 4 days of calendar time and 3 trading days. The preflight check is calendar days, not trading days, so:

- Sat 2026-04-12 → Mon 2026-04-14: 2 calendar days → passes (if Saturday run writes macro)
- Sat 2026-04-12 → Tue 2026-04-15: 3 calendar days → passes
- **Sat 2026-04-12 → Wed 2026-04-15: 3 calendar days → passes... wait**

Actually re-check: the last write was **Friday 2026-04-10**, not Saturday 2026-04-12. Phase 1 was last run Sat 2026-04-05 or earlier (the Saturday before the incident). This means macro hasn't been updated by Phase 1 for at least 10 days, which explains why ArcticDB has 2026-04-10 as the last date.

**Summary:** Macro is designed to be updated only by the Saturday backfill. But the Saturday backfill apparently hasn't run recently or hasn't written macro series to ArcticDB. The daily pipeline was never supposed to update macro — and the 4-day staleness threshold assumed weekly Saturday runs were keeping it fresh.

---

## 5. What Was Done (Actions Taken)

### 5a. Step Function redeployed (permanent fix to the SSM bug)
The weekday Step Function definition was updated with the fixed SSM command (adds `set -eo pipefail` + `git pull`). Deployed at 07:23:49 AM PT 2026-04-15. This prevents future Python failures from being masked as SSM "Success".

This matches the local `infrastructure/step_function_daily.json` which was already correct (commit `606d77f`, 2026-04-14).

### 5b. ArcticDB macro backfill via Polygon (ad-hoc)
A one-time script used Polygon.io's `get_grouped_daily` endpoint to backfill SPY, GLD, USO, and all `XL*` sector ETFs for 2026-04-13 and 2026-04-14. Results:

```
SPY: appended 2026-04-13 close=686.10, 2026-04-14 close=694.46
GLD: appended 2026-04-13/14
USO: appended 2026-04-13/14
XLB, XLC, XLE, XLF, XLI, XLK, XLP, XLRE, XLU, XLV, XLY: appended 2026-04-13/14
```

Final ArcticDB macro/SPY last date: **2026-04-14** (was 2026-04-10)

**Not backfilled:** VIX, VIX3M, TNX, IRX — these are indices, not available on Polygon free tier. They remain at 2026-04-10. This affects feature quality for the stock universe but doesn't block the preflight.

**Current state:** Preflight check `(2026-04-15 - 2026-04-14).days = 1 ≤ 4` → will now pass.

### 5c. yfinance confirmed blocked on ae-dashboard EC2
Attempted to use yfinance from ae-dashboard EC2; all tickers timed out:
```
Failed to get ticker 'SPY' reason: Failed to perform, curl: (28) Connection timed out after 30002 milliseconds.
```
Yahoo Finance blocks cloud IPs. Polygon is the reliable path for future automation.

---

## 6. What Is Still Broken / Open Questions for You

### 6a. Today's daily run still needs to execute
The Step Function failed this morning. Even though macro/SPY is now fresh enough to pass preflight, today's daily_closes parquet for 2026-04-15 may not have been written. To re-run:

```bash
source ~/.zshrc && ae-dashboard "cd /home/ec2-user/alpha-engine-data && set -a && source /home/ec2-user/.alpha-engine.env && set +a && source .venv/bin/activate && python weekly_collector.py --daily 2>&1 | tee /var/log/daily-data.log"
```

Note: Polygon grouped-daily for 2026-04-15 won't be available until after market close (~4 PM ET). If run before that, daily_closes will use yfinance fallback — which is **blocked on EC2**. Consider running this after 4 PM ET.

### 6b. VIX/VIX3M/TNX/IRX still stale (not backfilled)
These 4 macro series remain at 2026-04-10. Features computed for 2026-04-13/14 will use 5-day stale VIX/TNX values. Impact is minor (these are background regime features, not primary signals), but should be fixed.

**Options:**
- FRED API has DGS10 (TNX equivalent) and DGS3MO/DTB3 (IRX equivalent), but values are 1-2 days delayed
- CBOE VIX is not available on free polygon; alternative sources are scraping-based
- Accept stale values for the backfill period, they'll be updated at the next Saturday Phase 1 run

### 6c. Architectural decision needed

**The core question:** Should the daily pipeline update macro series in ArcticDB?

**Option A: Add macro tickers to `daily_closes.collect()`**  
In `_run_daily()` of `weekly_collector.py`, add macro ETFs to the tickers list before calling `daily_closes.collect()`. SPY, GLD, USO, and XL* sector ETFs are available from Polygon's grouped-daily. This would make `daily_append.py`'s Section 5 macro write actually functional.

Change in `weekly_collector.py` around line 380:
```python
# Add macro ETF tickers so daily_append can update ArcticDB macro library
MACRO_ETF_TICKERS = ["SPY", "GLD", "USO", "XLB", "XLC", "XLE", "XLF", "XLI", "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY"]
tickers_for_closes = list(set(tickers + MACRO_ETF_TICKERS))
dc_result = daily_closes.collect(bucket=bucket, tickers=tickers_for_closes, ...)
```

Limitation: VIX/VIX3M/TNX/IRX still can't be auto-updated this way. Those would remain at last-Saturday's values.

**Option B: Add a dedicated macro append step in the daily pipeline**  
A separate `builders/daily_macro_append.py` that fetches SPY close from Polygon (the only one needed for preflight) and writes it to ArcticDB. Cleaner separation of concerns.

**Option C: Raise the staleness threshold from 4 to 7 days**  
Changing `max_stale_days=4` to `max_stale_days=7` in `preflight.py` (daily mode) would make a weekly Saturday update sufficient. The original logic assumed daily updates, but if macro is updated weekly, the threshold should match. This is the minimal-change fix that doesn't require architecture changes. Risky if Saturday runs start failing too.

**Option D: Run Phase 1 Saturday backfill more regularly / verify it's running**  
The Saturday pipeline should be keeping macro fresh. If it's been failing, fix that first before adding complexity to the daily path.

### 6d. Verify the Saturday pipeline is writing macro to ArcticDB
The real macro/SPY last date being 2026-04-10 (a Friday) instead of something more recent suggests Phase 1 hasn't run cleanly recently. Check:

```bash
source ~/.zshrc && aws --region us-east-1 sfn list-executions --state-machine-arn $(aws --region us-east-1 sfn list-state-machines --query "stateMachines[?name=='alpha-engine-saturday-pipeline'].stateMachineArn" --output text) --status-filter SUCCEEDED --max-results 5 --query "executions[*].{name:name,start:startDate,stop:stopDate}" --output table
```

---

## 7. Verification Commands

**Check current macro/SPY last date in ArcticDB:**
```bash
source ~/.zshrc && ae-dashboard "cd /home/ec2-user/alpha-engine-data && source .venv/bin/activate && python -c \"from store.arctic_store import get_macro_lib; lib = get_macro_lib('alpha-engine-research'); df = lib.read('SPY').data; print('SPY last date:', df.index.max())\""
```

**Check if today's daily_closes parquet exists:**
```bash
source ~/.zshrc && aws --region us-east-1 s3 ls s3://alpha-engine-research/predictor/daily_closes/2026-04-15.parquet
```

**Dry-run today's daily collection (to validate without writing):**
```bash
source ~/.zshrc && ae-dashboard "cd /home/ec2-user/alpha-engine-data && set -a && source /home/ec2-user/.alpha-engine.env && set +a && source .venv/bin/activate && python weekly_collector.py --daily --dry-run 2>&1"
```

---

## 8. Relevant Files and Commits

| File | Relevance |
|------|-----------|
| `weekly_collector.py:341-449` | `_run_daily()` — assembles `tickers` for daily_closes (missing macro symbols) |
| `collectors/daily_closes.py:30-68` | `collect()` — fetches closes for passed `tickers` only |
| `builders/daily_append.py:258-290` | Section 5 — macro write, always skips because `closes.get("SPY")` is always None |
| `preflight.py:44-52` | Daily mode preflight — `check_arcticdb_fresh("macro", "SPY", max_stale_days=4)` |
| `infrastructure/step_function_daily.json` | DailyData SSM command — fixed locally, deployed to AWS on 2026-04-15 |

| Commit | Description |
|--------|-------------|
| `606d77f` | 2026-04-14 — Hard-fail daily_append + propagate exit code in weekday SSM (fixed Step Function JSON locally but not deployed until 2026-04-15) |
| `cb64f22` | 2026-04-14 — Fixed preflight library from `universe/SPY` to `macro/SPY` |
| `ed6fcbc` | 2026-04-14 — Related preflight/daily hardening |
| `d54fd1a` | 2026-04-14 — Added PredictorPreflight to Lambda (surfaces the staleness as a hard error) |
