# Alpha Engine Data

## What this repo is

Centralized data collection for the Alpha Engine system. Two-phase pipeline:
- **Phase 1** (before research): prices, macro, constituents, universe returns — runs on EC2
- **Phase 2** (after research): alternative data for promoted tickers (~30) — runs as Lambda

Downstream modules (Research, Predictor, Backtester) read from S3 instead of making redundant API calls.

## Stack

- Python 3.13, venv at `.venv/`
- yfinance for price data + options chains
- polygon.io for universe returns (grouped-daily)
- FRED API for macro data
- FMP for analyst consensus + EPS revisions
- SEC EDGAR for insider trading + institutional 13F
- Wikipedia for S&P constituent lists
- AWS: S3 (shared bucket `alpha-engine-research`), Lambda (Phase 2)
- Phase 1 runs on always-on EC2 (micro instance) via SSM RunCommand
- Phase 2 runs as Lambda triggered by Step Functions

## Key files

```
weekly_collector.py                # CLI entry point (--phase 1 / --phase 2)
polygon_client.py                  # polygon.io rate-limited client (universe returns)
collectors/constituents.py         # S&P 500+400 tickers + GICS sectors from Wikipedia
collectors/prices.py               # 10y OHLCV refresh for stale parquets (yfinance)
collectors/slim_cache.py           # 2y slice writer for inference Lambda
collectors/macro.py                # FRED series + commodity/index prices + market breadth
collectors/universe_returns.py     # Full-population forward returns (polygon.io)
collectors/alternative.py          # Analyst, revisions, options, insider, institutional, news
collectors/short_interest.py       # FINRA short-float (yfinance Ticker.info), bi-monthly cadence
config.yaml                        # GITIGNORED — local config
config.yaml.example                # template
infrastructure/add-cron.sh         # idempotent cron registration on EC2
```

## Config

`config.yaml` is gitignored. Copy `config.yaml.example` to `config.yaml` to configure.

## Common commands

```bash
# Activate venv
source .venv/bin/activate

# Phase 1: pre-research data collection
python weekly_collector.py --phase 1 --dry-run
python weekly_collector.py --phase 1

# Phase 2: post-research alternative data
python weekly_collector.py --phase 2 --dry-run
python weekly_collector.py --phase 2

# Collect specific components only
python weekly_collector.py --phase 1 --only constituents
python weekly_collector.py --phase 1 --only prices
python weekly_collector.py --phase 1 --only macro
python weekly_collector.py --phase 1 --only universe_returns
python weekly_collector.py --phase 2 --only alternative
```

## Environment variables

```
# Phase 1
FRED_API_KEY          # FRED macro data
POLYGON_API_KEY       # polygon.io universe returns

# Phase 2
FMP_API_KEY           # Financial Modeling Prep (analyst + revisions)
EDGAR_IDENTITY        # SEC EDGAR User-Agent (format: "Name email@domain.com")
```

## Schedule (Step Functions)

```
Sat 00:00 UTC    EventBridge triggers Step Functions
  ├─ DataPhase1 (EC2)          → prices, macro, constituents, universe returns
  ├─ RAGIngestion (EC2)        → SEC filings, 8-Ks, earnings, theses → research knowledge base
  ├─ Research (Lambda)         → signals.json
  ├─ DataPhase2 (Lambda)       → alternative data for promoted tickers
  ├─ Predictor Training (spot) → GBM retrain
  ├─ Backtester (spot)         → signal quality + param optimization
  └─ NotifyComplete (SNS)      → success email
```

## S3 output paths

```
s3://alpha-engine-research/
├── market_data/
│   ├── weekly/{date}/
│   │   ├── constituents.json         # tickers, sector_map, sector_etf_map
│   │   ├── macro.json                # FRED + market prices + breadth
│   │   ├── manifest.json             # run status, counts, errors
│   │   └── alternative/
│   │       ├── {TICKER}.json         # per-ticker alternative data
│   │       └── manifest.json         # Phase 2 run status
│   └── latest_weekly.json            # pointer to most recent weekly date
├── predictor/
│   ├── price_cache/*.parquet         # 10y OHLCV per ticker
│   ├── price_cache/sector_map.json   # ticker → sector ETF mapping
│   └── price_cache_slim/*.parquet    # 2y slices
├── health/
│   ├── data_phase1.json              # Phase 1 completion marker
│   └── data_phase2.json              # Phase 2 completion marker
└── research.db                       # universe_returns table updated by Phase 1
```

## Deployment

```bash
# Deploy to EC2
git push origin main && aem "cd ~/alpha-engine-data && git pull"

# Legacy cron removed — now orchestrated by Step Functions (2026-04-02)
```
