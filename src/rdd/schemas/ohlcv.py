"""Pandera schema for OHLCV daily bars."""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import DataFrameModel
from pandera.typing import Series


class OHLCVSchema(DataFrameModel):
    """Schema for daily OHLCV bars stored per ticker.

    One row = one trading day for one ticker.
    Prices are split-and-dividend adjusted (adj_close always; open/high/low/close
    are unadjusted raw prices from yfinance with auto_adjust=False).
    """

    ticker: Series[str] = pa.Field(
        nullable=False,
        description="Exchange-listed ticker symbol in yfinance format (e.g. 'AAPL', 'BRK-B').",
    )
    date: Series[pd.Timestamp] = pa.Field(
        nullable=False,
        description="Trading day as a timezone-naive timestamp (one row per trading day per ticker).",
    )
    open: Series[float] = pa.Field(
        nullable=True,
        ge=0,
        description="Unadjusted opening price for the session.",
    )
    high: Series[float] = pa.Field(
        nullable=True,
        ge=0,
        description="Unadjusted intraday high price.",
    )
    low: Series[float] = pa.Field(
        nullable=True,
        ge=0,
        description="Unadjusted intraday low price.",
    )
    close: Series[float] = pa.Field(
        nullable=True,
        ge=0,
        description="Unadjusted closing price.",
    )
    adj_close: Series[float] = pa.Field(
        nullable=True,
        ge=0,
        description="Split-and-dividend-adjusted closing price.",
    )
    volume: Series[float] = pa.Field(
        nullable=True,
        ge=0,
        description="Number of shares traded during the session.",
    )

    class Config:
        """Pandera config."""

        strict = True
        coerce = True

    @pa.dataframe_check
    def high_gte_low(cls, df: pd.DataFrame) -> pd.Series:
        """High must be >= low wherever both are present."""
        mask = df["high"].notna() & df["low"].notna()
        return (df.loc[mask, "high"] >= df.loc[mask, "low"]).reindex(df.index, fill_value=True)

    @pa.dataframe_check
    def close_within_high_low(cls, df: pd.DataFrame) -> pd.Series:
        """Close must fall within [low, high] wherever all three are present."""
        mask = df["close"].notna() & df["high"].notna() & df["low"].notna()
        within = (df.loc[mask, "close"] >= df.loc[mask, "low"]) & (
            df.loc[mask, "close"] <= df.loc[mask, "high"]
        )
        return within.reindex(df.index, fill_value=True)
