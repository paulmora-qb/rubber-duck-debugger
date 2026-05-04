"""Pandera schema for strategy portfolio holdings snapshots.

Long format: one row per (strategy, rebalance_date, ticker).
All strategies must emit a DataFrame conforming to this schema before
it can be consumed by the portfolio_performance pipeline.
"""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import DataFrameModel
from pandera.typing import Series


class PortfolioHoldingsSchema(DataFrameModel):
    """One row = one ticker held by one strategy on one rebalance date.

    Columns
    -------
    strategy : str
        Unique name for the strategy (e.g. ``"portfolio_construction"``).
    date : Timestamp
        Rebalance date — the day the weights take effect.
    ticker : str
        Exchange-listed ticker symbol (upper-case, e.g. ``"NVDA"``).
    weight : float
        Fractional portfolio weight in [0, 1].  All weights for a given
        (strategy, date) pair must sum to exactly 1.0 (±0.01 tolerance).
    """

    strategy: Series[str] = pa.Field(nullable=False)
    date: Series[pd.Timestamp] = pa.Field(nullable=False)
    ticker: Series[str] = pa.Field(nullable=False, str_startswith="")
    weight: Series[float] = pa.Field(nullable=False, ge=0.0, le=1.0)

    class Config:
        """Pandera config."""

        strict = True
        coerce = True

    @pa.dataframe_check(error="weights must sum to 1.0 (±0.01) per (strategy, date)")
    def weights_sum_to_one(cls, df: pd.DataFrame) -> bool:
        """Return True if every (strategy, date) group sums to 1.0 ±0.01."""
        sums = df.groupby(["strategy", "date"])["weight"].sum()
        return bool((sums - 1.0).abs().le(0.01).all())

    @pa.dataframe_check(error="duplicate (strategy, date, ticker) rows found")
    def no_duplicate_positions(cls, df: pd.DataFrame) -> bool:
        """Return True if there are no duplicate (strategy, date, ticker) rows."""
        return not df.duplicated(subset=["strategy", "date", "ticker"]).any()
