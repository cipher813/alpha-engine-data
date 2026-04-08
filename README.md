# alpha-engine-data

[![Python](https://img.shields.io/badge/python-3.13+-blue.svg)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-56_passing-brightgreen.svg)]()

> Centralized data collection, storage, and distribution for the Alpha Engine system. Owns price data (ArcticDB), macro indicators, universe returns, and alternative data fetchers. Runs as the first step in both Saturday and daily Step Function pipelines.

**Part of the [Nous Ergon](https://nousergon.ai) autonomous trading system.**
See the [system overview](https://github.com/cipher813/alpha-engine#readme) for how all modules connect.

## Table of Contents

- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [How It Works](#how-it-works)
- [Configuration](#configuration)
- [Key Files](#key-files)
- [Deployment](#deployment)
- [Testing](#testing)
- [S3 Contract](#s3-contract)
- [Related Modules](#related-modules)

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  WEEKLY (Saturday, Step Function)                                │
│                                                                  │
│  Phase 1 (EC2 SSM, 15-25 min)                                  │
│    Constituents ──── S&P 500+400 tickers + GICS sectors         │
│    Prices ──── 10y OHLCV → ArcticDB universe library            │
│    Slim Cache ──── 2y slices → ArcticDB universe_slim           │
│    Macro ──── FRED + commodities + market breadth               │
│    Universe Returns ──── polygon.io grouped-daily → research.db │
│    Feature Store ──── 54 features for 903 tickers (~20s)        │
│                                                                  │
│  Phase 2 (Lambda, after Research)                               │
│    Alternative Data ──── analyst consensus, EPS revisions,      │
│      options chains, insider filings, 13F, news sentiment       │
│      (FMP, yfinance, SEC EDGAR — only for ~30 promoted tickers) │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  DAILY (Mon-Fri, Step Function)                                  │
│                                                                  │
│  DailyData (EC2 SSM)                                            │
│    daily_closes/{date}.parquet ──── OHLCV for all tickers       │
│    Macro refresh ──── yields, VIX, commodities                  │
│  FeatureStoreCompute (EC2 SSM)                                  │
│    Pre-compute 54 features for inference                        │
└─────────────────────────────────────────────────────────────────┘
```

**Design rationale:** With 6 modules consuming overlapping data, centralizing collection reduces API costs, eliminates rate-limiting issues, and ensures all modules operate on identical data snapshots.

## Quick Start

### Prerequisites

- Python 3.13+
- AWS credentials with S3 read/write
- API keys: FRED, Polygon.io, FMP, SEC EDGAR identity

### Setup

```bash
git clone https://github.com/cipher813/alpha-engine-data.git
cd alpha-engine-data
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp config.yaml.example config.yaml
# Edit config.yaml — set S3 bucket, API keys

python weekly_collector.py --phase 1 --dry-run
```

## How It Works

### Price Data (ArcticDB)

As of 2026-04-07, all price data is stored in ArcticDB (backed by S3), replacing fragmented per-ticker Parquet files. ArcticDB provides versioned time-series storage with deduplication and efficient range queries.

| Library | S3 Prefix | History | Refresh | Consumers |
|---------|-----------|---------|---------|-----------|
| `universe` | `arcticdb/universe/` | 10 years | Weekly (Phase 1) | Predictor training, Backtester |
| `universe_slim` | `arcticdb/universe_slim/` | 2 years | Weekly (post-training) | Predictor inference, Executor (ATR) |

909 tickers backfilled and validated. Legacy Parquet paths still written during Phase 7 transition (target deprecation ~2026-04-21).

### Data Quality Gates

Price validation runs automatically after each refresh:
- OHLC relationship checks (open/high/low/close ordering)
- Zero price detection
- Extreme return filtering (>50% daily moves)
- Zero volume detection
- Volume spike detection
- Trading day gap detection

Anomalies are surfaced in per-step completion emails.

## Configuration

`config.yaml` is gitignored. Copy `config.yaml.example` and set:
- S3 bucket name
- API keys (FRED, Polygon, FMP, EDGAR)
- Email settings for completion notifications

Modules search `alpha-engine-config` (private repo) first, fall back to local config.

## Key Files

```
weekly_collector.py            # CLI entry point (--phase 1 / --phase 2)
polygon_client.py              # Rate-limited polygon.io client
collectors/constituents.py     # S&P 500+400 tickers + GICS sectors
collectors/prices.py           # 10y OHLCV → ArcticDB + legacy Parquet
collectors/slim_cache.py       # 2y slice writer
collectors/macro.py            # FRED + commodities + breadth
collectors/universe_returns.py # Full-population forward returns
collectors/alternative.py      # Per-ticker alternative data (Phase 2)
feature_store/compute.py       # 54 features for 903 tickers
validators/price_validator.py  # 6-check data quality gates
trading_calendar.py            # NYSE holiday detection (through 2030)
emailer.py                     # Per-step completion emails
health_checker.py              # 6-hourly freshness monitoring + SNS
```

## Deployment

- **Orchestrated by Step Functions** — Saturday pipeline (Phase 1 + 2) and daily pipeline (DailyData + FeatureStore)
- **EC2 micro instance** (always-on): Phase 1 runs via SSM RunCommand
- **Lambda**: Phase 2 runs as Lambda function
- **Deploy**: `git push origin main && ae-dashboard "cd ~/alpha-engine-data && git pull"`
- **Health monitoring**: 6-hourly cron on micro instance → SNS alerts on stale/missing data

## Testing

```bash
pytest tests/ -v  # 56 tests
```

## S3 Contract

### Writes
| Path | Content | Frequency |
|------|---------|-----------|
| `arcticdb/universe/` | 10y OHLCV for 909 tickers | Weekly |
| `arcticdb/universe_slim/` | 2y OHLCV slices | Weekly |
| `predictor/daily_closes/{date}.parquet` | Daily OHLCV | Daily |
| `predictor/feature_store/{date}/` | 54 features x 903 tickers | Weekly + Daily |
| `market_data/weekly/{date}/` | Constituents, macro, alternative data | Weekly |
| `health/data_phase1.json` | Phase 1 completion marker | Weekly |
| `research.db` (universe_returns table) | Forward returns for all tickers | Weekly |

### Reads
| Path | Content |
|------|---------|
| `signals/{date}/signals.json` | Promoted tickers for Phase 2 alternative data |

## Related Modules

- [`alpha-engine`](https://github.com/cipher813/alpha-engine) — Executor + system overview
- [`alpha-engine-research`](https://github.com/cipher813/alpha-engine-research) — Autonomous LLM research pipeline
- [`alpha-engine-predictor`](https://github.com/cipher813/alpha-engine-predictor) — Meta-model predictor
- [`alpha-engine-backtester`](https://github.com/cipher813/alpha-engine-backtester) — Evaluation framework and parameter optimization
- [`alpha-engine-dashboard`](https://github.com/cipher813/alpha-engine-dashboard) — Streamlit monitoring dashboard
- [`alpha-engine-docs`](https://github.com/cipher813/alpha-engine-docs) — Documentation index and system audits

## License

MIT — see [LICENSE](LICENSE).
