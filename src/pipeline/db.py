import duckdb
import pandas as pd
from pathlib import Path


class TradingDB:
    def __init__(self, db_path: str = "data/trading.db"):
        self.db_path = db_path
        Path("data").mkdir(exist_ok=True)
        self.conn = duckdb.connect(self.db_path)
        self._init_db()

    def _init_db(self):
        """Create tables if they don't exist."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ohlcv (
                symbol VARCHAR,
                exchange VARCHAR,
                timeframe VARCHAR,
                timestamp TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                PRIMARY KEY (symbol, exchange, timeframe, timestamp)
            )
        """)

    def save_ohlcv(self, df: pd.DataFrame, symbol: str, exchange: str, timeframe: str):
        """Save OHLCV data to DuckDB."""
        if df.empty:
            return

        # Prepare for bulk insert and ensure column alignment
        df["symbol"] = symbol
        df["exchange"] = exchange
        df["timeframe"] = timeframe

        # Explicitly order columns to match the table schema
        column_order = [
            "symbol",
            "exchange",
            "timeframe",
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        df = df[column_order]

        # Use DuckDB's native pandas integration for fast insertion
        # We use a temp table to handle "UPSERT" logic (update if exists)
        self.conn.execute(
            "CREATE TEMP TABLE IF NOT EXISTS stage_ohlcv AS SELECT * FROM ohlcv WHERE FALSE"
        )
        # Ensure we insert into stage table with the dataframe data correctly mapped
        self.conn.execute("INSERT INTO stage_ohlcv SELECT * FROM df")

        self.conn.execute("""
            INSERT INTO ohlcv 
            SELECT * FROM stage_ohlcv
            ON CONFLICT (symbol, exchange, timeframe, timestamp) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume
        """)
        self.conn.execute("DROP TABLE stage_ohlcv")

    def get_ohlcv(self, symbol: str, timeframe: str, limit: int = 1000):
        """Query data back for analysis."""
        return self.conn.execute(
            """
            SELECT * FROM ohlcv 
            WHERE symbol = ? AND timeframe = ?
            ORDER BY timestamp DESC
            LIMIT ?
        """,
            [symbol, timeframe, limit],
        ).df()
