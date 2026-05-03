# alpha-engine-data

> Part of [**Nous Ergon**](https://nousergon.ai) — Autonomous Multi-Agent Trading System. Repo and S3 names use the underlying project name `alpha-engine`.

[![Part of Nous Ergon](https://img.shields.io/badge/Part_of-Nous_Ergon-1a73e8?style=flat-square)](https://nousergon.ai)
[![Python](https://img.shields.io/badge/python-3.13+-blue?style=flat-square)](https://www.python.org/)
[![ArcticDB](https://img.shields.io/badge/ArcticDB-0e1117?style=flat-square)](https://docs.arcticdb.io/)
[![Polygon.io](https://img.shields.io/badge/Polygon.io-1a73e8?style=flat-square)](https://polygon.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow?style=flat-square)](LICENSE)
[![Phase 2 · Reliability](https://img.shields.io/badge/Phase_2-Reliability-e9c46a?style=flat-square)](https://github.com/cipher813/alpha-engine-docs#phase-trajectory)

Centralized data collection, storage, and distribution. Owns the price universe (ArcticDB), macro indicators, universe returns, the engineered feature store, and per-ticker alternative data. Runs as the first step of the weekly pipeline and the daily EOD pipeline.

> System overview, Step Function orchestration, and module relationships live in [`alpha-engine-docs`](https://github.com/cipher813/alpha-engine-docs).

## What this does

- Maintains a 10-year ArcticDB price universe across ~900 S&P 500+400 tickers, refreshed weekly with daily EOD appends
- Ingests macro indicators from FRED (rates, VIX, commodities) and computes derived signals (yield-curve slope, VIX term slope, market breadth)
- Pulls per-ticker alternative data (analyst consensus, EPS revisions, options chains, insider filings, 13F holdings, news sentiment) only for tickers promoted by Research — keeps API spend bounded
- Computes the engineered feature store (~50 features × ~900 tickers) used by the Predictor for both training and inference
- Runs the RAG ingestion step: SEC 10-K/10-Q/8-K filings, earnings transcripts, and thesis history embedded into the pgvector knowledge base that Research's qual-analyst agents query via tool calls
- Runs OHLC validation, zero-price detection, extreme-return filtering, and trading-day-gap detection on every refresh; anomalies surface in completion emails

## Phase 2 measurement contribution

Data is the substrate everything else measures against. Phase 2 contribution: feature coverage, freshness tracking, and per-feature drift detection that downstream modules can rely on. Every signal, prediction, and trade traces back to inputs that have been validated, freshness-checked, and tagged with quality flags. If the substrate is wrong, every metric above it is wrong.

## Architecture

```
External APIs
  polygon.io · FRED · Wikipedia · FMP · SEC EDGAR · yfinance
       │
       ├─ Phase 1 (pre-research, weekly Sat — EC2 SSM) ───────────────
       │    constituents          S&P 500+400 tickers + GICS sectors
       │    prices                10y OHLCV → ArcticDB universe
       │    slim_cache            2y slices → ArcticDB universe_slim
       │    macro                 FRED + commodities + market breadth
       │    universe_returns      polygon.io grouped-daily → research.db
       │    feature_store         ~50 features × ~900 tickers
       │
       ├─ RAG ingestion (between Phase 1 and Research, EC2 SSM) ──────
       │    SEC 10-K/10-Q/8-K     section extraction + chunking
       │    earnings transcripts  FMP API
       │    thesis history        self-ingestion from research.db
       │                          → Voyage voyage-3-lite embeddings (512d)
       │                          → Neon pgvector (HNSW index)
       │
       ├─ Phase 2 (post-research, weekly Sat — Lambda) ───────────────
       │    alternative_data      analyst · revisions · options · insider · 13F · news
       │                          (only for tickers promoted by Research)
       │
       └─ EOD (Mon-Fri post-close — EC2 SSM) ─────────────────────────
            daily_closes          EOD OHLCV → ArcticDB universe
            macro refresh         yields, VIX, commodities
```

**Quality gates run automatically after each refresh:** OHLC ordering, zero-price detection, extreme returns (>50% daily moves), zero-volume detection, volume-spike detection, trading-day gap detection. Anomalies surface in per-step completion emails.

**Design rationale:** centralizing data collection (vs. each module fetching its own) removes redundant API calls, eliminates rate-limiting risk, and ensures every consumer operates on identical snapshots. The ArcticDB substrate replaces an earlier per-ticker Parquet sprawl that was fragmenting cache reads and slowing inference.

## Quick start

```bash
git clone https://github.com/cipher813/alpha-engine-data.git
cd alpha-engine-data
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp config.yaml.example config.yaml
# Edit: S3 bucket, API keys (FRED, Polygon, FMP, EDGAR identity)

python weekly_collector.py --phase 1 --dry-run
```

## Key files

| File | What it does |
|---|---|
| [`weekly_collector.py`](weekly_collector.py) | CLI entry point — `--phase 1` / `--phase 2` / `--only <component>` |
| [`polygon_client.py`](polygon_client.py) | Rate-limited polygon.io client (universe returns) |
| [`collectors/prices.py`](collectors/prices.py) | 10y OHLCV refresh into ArcticDB universe |
| [`collectors/macro.py`](collectors/macro.py) | FRED + commodities + market breadth |
| [`collectors/universe_returns.py`](collectors/universe_returns.py) | Full-population forward returns via polygon.io grouped-daily |
| [`feature_store/compute.py`](feature_store/compute.py) | Engineered feature store builder |
| [`rag/pipelines/`](rag/pipelines/) | RAG ingestion pipelines — SEC filings, earnings transcripts, theses |
| [`validators/price_validator.py`](validators/price_validator.py) | 6-check data quality gates |

## How it runs

| Pipeline | When | Where | Trigger |
|---|---|---|---|
| Phase 1 | Sat 00:00 UTC | EC2 SSM (always-on micro instance) | Saturday Step Function |
| RAG ingestion | Sat after Phase 1 | EC2 SSM | Saturday Step Function |
| Phase 2 | Sat after Research | Lambda | Saturday Step Function |
| Daily EOD | Mon-Fri ~1:05 PM PT | EC2 SSM (ae-trading) | EOD Step Function |
| Health monitoring | Every 6 hours | Cron on micro instance | SNS alerts on stale data |

Deploy: `git push origin main && ae-dashboard "cd ~/alpha-engine-data && git pull"`.

## Configuration

`config.yaml` is gitignored. Real values live in the private [`alpha-engine-config`](https://github.com/cipher813/alpha-engine-config) repo (proprietary thresholds + bucket names). Local development copies `config.yaml.example`.

Environment variables:

| Variable | Used by |
|---|---|
| `FRED_API_KEY` | Phase 1 macro |
| `POLYGON_API_KEY` | Phase 1 universe returns + EOD prices |
| `FMP_API_KEY` | Phase 2 analyst + revisions |
| `EDGAR_IDENTITY` | Phase 2 SEC EDGAR User-Agent |

## S3 contract

### Writes
| Path | Content | Cadence |
|---|---|---|
| `arcticdb/universe/` | 10y OHLCV for ~900 tickers | Weekly |
| `arcticdb/universe_slim/` | 2y OHLCV slices | Weekly |
| `staging/daily_closes/{date}.parquet` | Daily OHLCV staging (7-day lifecycle) — canonical home is ArcticDB universe | Daily |
| `predictor/feature_store/{date}/` | ~50 features × ~900 tickers | Weekly + Daily |
| `market_data/weekly/{date}/` | Constituents, macro, alternative data | Weekly |
| `health/data_phase1.json` | Phase 1 completion marker | Weekly |
| `research.db` (`universe_returns` table) | Forward returns for all tickers | Weekly |
| Neon pgvector (`rag.documents`, `rag.chunks`) | RAG corpus — SEC filings, transcripts, theses with HNSW index | Weekly |

### Reads
| Path | Content |
|---|---|
| `signals/{date}/signals.json` | Promoted tickers for Phase 2 alternative-data scope |

## Testing

```bash
pytest tests/ -v
```

## Sister repos

| Module | Repo |
|---|---|
| Executor | [`alpha-engine`](https://github.com/cipher813/alpha-engine) |
| Research | [`alpha-engine-research`](https://github.com/cipher813/alpha-engine-research) |
| Predictor | [`alpha-engine-predictor`](https://github.com/cipher813/alpha-engine-predictor) |
| Backtester | [`alpha-engine-backtester`](https://github.com/cipher813/alpha-engine-backtester) |
| Dashboard | [`alpha-engine-dashboard`](https://github.com/cipher813/alpha-engine-dashboard) |
| Library | [`alpha-engine-lib`](https://github.com/cipher813/alpha-engine-lib) |
| Docs | [`alpha-engine-docs`](https://github.com/cipher813/alpha-engine-docs) |

## License

MIT — see [LICENSE](LICENSE).
