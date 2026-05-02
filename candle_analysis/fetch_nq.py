"""Fetch NQ futures 5m data and save to CSV.

Yahoo Finance is the source (yfinance). TradingView export is not available
in this environment, so we use Yahoo's continuous front-month proxy 'NQ=F'.
Yahoo intraday history is capped at ~60 days, which exceeds the 30-day target.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

OUT = Path(__file__).parent / "nq_5m.csv"
TICKER = "NQ=F"
LOOKBACK_DAYS = 59  # Yahoo limit


def fetch() -> pd.DataFrame:
    end = datetime.now()
    start = end - timedelta(days=LOOKBACK_DAYS)
    df = yf.download(
        TICKER,
        start=start.strftime("%Y-%m-%d"),
        end=end.strftime("%Y-%m-%d"),
        interval="5m",
        auto_adjust=False,
        progress=False,
        prepost=True,
    )
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
    df.index.name = "datetime_utc"
    # Convert to US/Eastern for session alignment
    et = df.index.tz_convert("US/Eastern")
    df["datetime_et"] = et
    df["date_et"] = et.date
    df["time_et"] = et.time
    return df


def main() -> None:
    df = fetch()
    df.to_csv(OUT)
    print(f"Saved {len(df)} rows to {OUT}")
    print(f"Date range (ET): {df['date_et'].min()} -> {df['date_et'].max()}")
    print(f"Unique trading dates: {df['date_et'].nunique()}")


if __name__ == "__main__":
    main()
