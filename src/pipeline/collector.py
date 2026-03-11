import ccxt  # type: ignore
import logging
import pandas as pd
from datetime import datetime
from typing import Optional
from .db import TradingDB

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataCollector:
    def __init__(self, exchange_id: str = "binance", db: Optional[TradingDB] = None):
        self.exchange = getattr(ccxt, exchange_id)(
            {
                "enableRateLimit": True,
            }
        )
        self.db = db or TradingDB()

    def fetch_historical_ohlcv(
        self,
        symbol: str,
        timeframe: str = "1m",
        since_ms: Optional[int] = None,
        limit: int = 1000,
    ) -> pd.DataFrame:
        """Fetch OHLCV from exchange and return as DataFrame."""
        logger.info(
            f"Fetching {limit} candles for {symbol} ({timeframe}) from {self.exchange.id}..."
        )

        ohlcv = self.exchange.fetch_ohlcv(
            symbol, timeframe, since=since_ms, limit=limit
        )

        df = pd.DataFrame(
            ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"]
        )
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")

        return df

    def sync_historical_data(self, symbol: str, timeframe: str, days: int = 30):
        """Deep sync: Fetch multiple batches to fill historical data."""
        since = int((datetime.now().timestamp() - (days * 86400)) * 1000)

        while since < datetime.now().timestamp() * 1000:
            df = self.fetch_historical_ohlcv(symbol, timeframe, since_ms=since)
            if df.empty:
                break

            self.db.save_ohlcv(df, symbol, self.exchange.id, timeframe)

            # Set since to the last timestamp + 1ms to avoid overlap
            last_ts = int(df["timestamp"].iloc[-1].timestamp() * 1000)
            if last_ts == since:  # Stuck
                break
            since = last_ts + 1

            logger.info(f"Synced up to {df['timestamp'].iloc[-1]}")

            # Rate limiting safety check
            if len(df) < 500:  # Usually means we hit the tip
                break

        logger.info(f"Sync complete for {symbol}")
