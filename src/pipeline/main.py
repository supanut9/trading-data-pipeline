import argparse
from pipeline.collector import DataCollector
from pipeline.db import TradingDB


def main():
    parser = argparse.ArgumentParser(description="Trading Data Pipeline Collector")
    parser.add_argument(
        "--symbol", type=str, default="BTC/USDT", help="Symbol to fetch (e.g. BTC/USDT)"
    )
    parser.add_argument(
        "--timeframe", type=str, default="1h", help="Timeframe (1m, 5m, 1h, 1d)"
    )
    parser.add_argument(
        "--days", type=int, default=7, help="How many days of history to sync"
    )
    parser.add_argument(
        "--exchange", type=str, default="binance", help="Exchange ID (ccxt compatible)"
    )

    args = parser.parse_args()

    db = TradingDB()
    collector = DataCollector(exchange_id=args.exchange, db=db)

    collector.sync_historical_data(
        symbol=args.symbol, timeframe=args.timeframe, days=args.days
    )

    # Show summary
    data = db.get_ohlcv(args.symbol, args.timeframe, limit=5)
    print("\nLatest data in DB:")
    print(data)


if __name__ == "__main__":
    main()
