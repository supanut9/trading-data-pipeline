# trading-data-pipeline

Collect, store, and serve market data from crypto exchanges.

## Tech Stack
- **Python 3.12+** with `ccxt`, `DuckDB`, `APScheduler`
- Uses `uv` for package management

## Quick Start
```bash
uv sync
uv run datapipe fetch --exchange binance --symbol BTC/USDT --timeframe 1h
```
