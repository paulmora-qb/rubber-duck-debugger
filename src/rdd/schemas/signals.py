"""Pandera schema for strategy signal output."""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import DataFrameModel
from pandera.typing import Series


class SignalSchema(DataFrameModel):
    """Schema for strategy signals fed into the backtester.

    One row = one active long position on one rebalance date.
    Only non-flat rows are included; weights must sum to 1.0 per date.
    """

    date: Series[pd.Timestamp] = pa.Field(
        nullable=False,
        description="Rebalance date. Trades execute at the next trading day's open.",
    )
    ticker: Series[str] = pa.Field(
        nullable=False,
        description="Ticker symbol in yfinance format.",
    )
    position: Series[int] = pa.Field(
        isin=[1],
        nullable=False,
        description="Always 1 for long-only. Short positions are not supported.",
    )
    weight: Series[float] = pa.Field(
        gt=0,
        le=1.0,
        nullable=False,
        description="Target portfolio weight. Weights must sum to 1.0 per date.",
    )

    class Config:
        """Pandera config."""

        strict = True
        coerce = True

    @pa.dataframe_check
    def weights_sum_to_one_per_date(cls, df: pd.DataFrame) -> pd.Series:
        """Weights must sum to 1.0 (within 1e-6) for each rebalance date."""
        sums = df.groupby("date")["weight"].sum()
        ok = sums.sub(1.0).abs().lt(1e-6)
        return df["date"].map(ok)
