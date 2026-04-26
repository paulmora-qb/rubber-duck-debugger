"""Pandera schema for company news articles fetched from Finnhub."""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import DataFrameModel
from pandera.typing import Series


class CompanyNewsSchema(DataFrameModel):
    """Schema for company news articles stored per ticker.

    One row = one news article for one ticker.
    Articles are sourced from Finnhub and stored incrementally.
    """

    ticker: Series[str] = pa.Field(
        nullable=False,
        description="Exchange-listed ticker symbol in yfinance format (e.g. 'AAPL', 'BRK-B').",
    )
    published_at: Series[pd.Timestamp] = pa.Field(
        nullable=False,
        description="Publication timestamp as a timezone-naive UTC timestamp.",
    )
    headline: Series[str] = pa.Field(
        nullable=True,
        description="Article headline.",
    )
    summary: Series[str] = pa.Field(
        nullable=True,
        description="Short article summary.",
    )
    source: Series[str] = pa.Field(
        nullable=True,
        description="News source name (e.g. 'Reuters').",
    )
    url: Series[str] = pa.Field(
        nullable=True,
        description="Full URL of the original article.",
    )
    category: Series[str] = pa.Field(
        nullable=True,
        description="Finnhub news category (e.g. 'company news', 'top news').",
    )

    class Config:
        """Pandera config."""

        strict = True
        coerce = True
