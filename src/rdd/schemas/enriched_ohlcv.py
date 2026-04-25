"""Pandera schema for OHLCV data enriched with Finnhub fundamentals."""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import DataFrameModel
from pandera.typing import Series


class EnrichedOHLCVSchema(DataFrameModel):
    """Schema for daily OHLCV bars joined with company fundamentals.

    Extends the raw OHLCV schema by broadcasting static fundamental
    columns (sector, industry, country, etc.) across all trading days
    for each ticker.
    """

    # --- price columns (mirrors OHLCVSchema) ---
    ticker: Series[str] = pa.Field(nullable=False)
    date: Series[pd.Timestamp] = pa.Field(nullable=False)
    open: Series[float] = pa.Field(nullable=True, ge=0)
    high: Series[float] = pa.Field(nullable=True, ge=0)
    low: Series[float] = pa.Field(nullable=True, ge=0)
    close: Series[float] = pa.Field(nullable=True, ge=0)
    adj_close: Series[float] = pa.Field(nullable=True, ge=0)
    volume: Series[float] = pa.Field(nullable=True, ge=0)

    # --- fundamental columns (from FinnhubFundamentalsSchema) ---
    sector: Series[str] = pa.Field(nullable=True)
    industry: Series[str] = pa.Field(nullable=True)
    country: Series[str] = pa.Field(nullable=True)
    market_cap: Series[float] = pa.Field(nullable=True, ge=0)
    pe_ratio: Series[float] = pa.Field(nullable=True)
    eps: Series[float] = pa.Field(nullable=True)

    class Config:
        """Pandera config."""

        strict = True
        coerce = True

    @pa.dataframe_check
    def high_gte_low(cls, df: pd.DataFrame) -> pd.Series:
        """High must be >= low wherever both are non-null."""
        mask = df["high"].notna() & df["low"].notna()
        return (df.loc[mask, "high"] >= df.loc[mask, "low"]).reindex(
            df.index, fill_value=True
        )
