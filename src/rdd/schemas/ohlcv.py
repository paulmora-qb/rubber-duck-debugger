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

    ticker: Series[str] = pa.Field(nullable=False)
    date: Series[pd.Timestamp] = pa.Field(nullable=False)
    open: Series[float] = pa.Field(nullable=True, ge=0)
    high: Series[float] = pa.Field(nullable=True, ge=0)
    low: Series[float] = pa.Field(nullable=True, ge=0)
    close: Series[float] = pa.Field(nullable=True, ge=0)
    adj_close: Series[float] = pa.Field(nullable=True, ge=0)
    volume: Series[float] = pa.Field(nullable=True, ge=0)

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
