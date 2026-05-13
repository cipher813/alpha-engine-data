"""Daily snapshot writers — own the time-series state required by
self-derived signals.

Some signals (analyst-estimate revisions, sentiment trends) require
knowing yesterday's value to compute today's delta. The producers
(yfinance, Finnhub, FMP) don't expose historical revisions on their
free tiers — so we own the time series by snapshotting the current
state daily.

Each snapshotter writes one JSON / parquet to S3 per day. The matching
``data/derived/`` module reads the time series and computes deltas.

Modules:

  analyst_daily — analyst consensus + price target snapshot (PR C)

See ``~/Development/alpha-engine-docs/private/data-revamp-260513.md``
for the full institutional data-revamp arc context.
"""
