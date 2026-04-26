"""Pandera schema for company information snapshots."""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import DataFrameModel
from pandera.typing import Series


class CompanyInfoSchema(DataFrameModel):
    """Schema for company metadata snapshots fetched from yfinance.

    One row = one company snapshot for one ticker.
    Data is point-in-time as of ``fetched_at``; re-fetching overwrites the row.
    """

    ticker: Series[str] = pa.Field(
        nullable=False,
        description="Exchange-listed ticker symbol in yfinance format (e.g. 'AAPL', 'BRK-B').",
    )
    name: Series[str] = pa.Field(
        nullable=True,
        description="Full legal company name (yfinance ``longName``).",
    )
    sector: Series[str] = pa.Field(
        nullable=True,
        description="GICS sector (e.g. 'Technology', 'Financials').",
    )
    industry: Series[str] = pa.Field(
        nullable=True,
        description="GICS industry sub-classification.",
    )
    market_cap: Series[float] = pa.Field(
        nullable=True,
        ge=0,
        description="Market capitalisation in the reporting currency.",
    )
    employees: Series[float] = pa.Field(
        nullable=True,
        ge=0,
        description="Number of full-time employees (stored as float to allow NaN).",
    )
    country: Series[str] = pa.Field(
        nullable=True,
        description="Country of incorporation.",
    )
    currency: Series[str] = pa.Field(
        nullable=True,
        description="Reporting currency code (e.g. 'USD').",
    )
    exchange: Series[str] = pa.Field(
        nullable=True,
        description="Primary listing exchange (e.g. 'NMS' for NASDAQ).",
    )
    fetched_at: Series[pd.Timestamp] = pa.Field(
        nullable=False,
        description="Timestamp (UTC, second precision) when this snapshot was fetched.",
    )

    class Config:
        """Pandera config."""

        strict = True
        coerce = True
