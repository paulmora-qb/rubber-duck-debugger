"""Pandera schema for historical EPS actuals vs estimates."""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import DataFrameModel
from pandera.typing import Series


class EarningsHistorySchema(DataFrameModel):
    """Schema for historical earnings (EPS actuals vs estimates) from yfinance.

    One row = one earnings event for one ticker.
    """

    ticker: Series[str] = pa.Field(
        nullable=False,
        description="Exchange-listed ticker symbol (lowercase).",
    )
    earnings_date: Series[pd.Timestamp] = pa.Field(
        nullable=False,
        description="Earnings announcement date (tz-naive).",
    )
    eps_estimate: Series[float] = pa.Field(
        nullable=True,
        description="Analyst consensus EPS estimate for the quarter.",
    )
    reported_eps: Series[float] = pa.Field(
        nullable=True,
        description="Actual reported EPS for the quarter.",
    )
    surprise_pct: Series[float] = pa.Field(
        nullable=True,
        description="Earnings surprise as a percentage: (reported - estimate) / |estimate| * 100.",
    )
    fetched_at: Series[pd.Timestamp] = pa.Field(
        nullable=False,
        description="Timestamp (UTC, second precision) when this data was fetched.",
    )

    class Config:
        """Pandera config."""

        strict = True
        coerce = True
