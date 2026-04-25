"""Pandera schemas for Finnhub data."""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import DataFrameModel
from pandera.typing import Series


class FinnhubNewsSchema(DataFrameModel):
    """Schema for per-company news articles pulled from the Finnhub API.

    One row = one article for one ticker.
    """

    ticker: Series[str] = pa.Field(
        nullable=False,
        description="Ticker symbol in yfinance format (e.g. 'AAPL').",
    )
    datetime: Series[pd.Timestamp] = pa.Field(
        nullable=False,
        description="Publication timestamp (UTC, timezone-naive).",
    )
    headline: Series[str] = pa.Field(
        nullable=False,
        description="Article headline.",
    )
    summary: Series[str] = pa.Field(
        nullable=True,
        description="Short article summary as provided by Finnhub.",
    )
    source: Series[str] = pa.Field(
        nullable=True,
        description="Publisher / source name.",
    )
    url: Series[str] = pa.Field(
        nullable=True,
        description="Canonical article URL.",
    )

    class Config:
        """Pandera config."""

        strict = True
        coerce = True


class FinnhubFundamentalsSchema(DataFrameModel):
    """Schema for company profile and basic financials from Finnhub.

    One row = one company. Static snapshot — refreshed periodically, not daily.
    """

    ticker: Series[str] = pa.Field(
        nullable=False,
        description="Ticker symbol.",
    )
    company_name: Series[str] = pa.Field(
        nullable=True,
        description="Full legal company name.",
    )
    sector: Series[str] = pa.Field(
        nullable=True,
        description="GICS sector (e.g. 'Technology').",
    )
    industry: Series[str] = pa.Field(
        nullable=True,
        description="GICS industry sub-group.",
    )
    country: Series[str] = pa.Field(
        nullable=True,
        description="Country of incorporation (ISO 3166-1 alpha-2).",
    )
    exchange: Series[str] = pa.Field(
        nullable=True,
        description="Primary listing exchange (e.g. 'NASDAQ', 'NYSE').",
    )
    market_cap: Series[float] = pa.Field(
        nullable=True,
        ge=0,
        description="Market capitalisation in USD.",
    )
    pe_ratio: Series[float] = pa.Field(
        nullable=True,
        description="Trailing 12-month price-to-earnings ratio.",
    )
    eps: Series[float] = pa.Field(
        nullable=True,
        description="Trailing 12-month earnings per share (USD).",
    )

    class Config:
        """Pandera config."""

        strict = True
        coerce = True
