"""Pandera schema for valuation and profitability ratios."""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import DataFrameModel
from pandera.typing import Series


class ValuationRatiosSchema(DataFrameModel):
    """Schema for valuation and profitability ratios derived from company financials.

    One row = one ticker's latest-quarter ratios snapshot.
    All ratio fields are nullable because many tickers will have missing source data.
    """

    ticker: Series[str] = pa.Field(
        nullable=False,
        description="Exchange-listed ticker symbol (lowercase).",
    )
    period_end: Series[pd.Timestamp] = pa.Field(
        nullable=False,
        description="Period end date of the latest quarter used (tz-naive).",
    )
    market_cap: Series[float] = pa.Field(
        nullable=True,
        description="Market capitalisation in the reporting currency.",
    )
    pe_ratio: Series[float] = pa.Field(
        nullable=True,
        description="Price-to-earnings ratio: market_cap / (net_income * 4).",
    )
    pb_ratio: Series[float] = pa.Field(
        nullable=True,
        description="Price-to-book ratio: market_cap / equity.",
    )
    ev_ebitda: Series[float] = pa.Field(
        nullable=True,
        description="EV/EBITDA: (market_cap + total_debt - cash) / (ebitda * 4).",
    )
    gross_margin: Series[float] = pa.Field(
        nullable=True,
        description="Gross margin: gross_profit / total_revenue.",
    )
    operating_margin: Series[float] = pa.Field(
        nullable=True,
        description="Operating margin: operating_income / total_revenue.",
    )
    net_margin: Series[float] = pa.Field(
        nullable=True,
        description="Net margin: net_income / total_revenue.",
    )
    roe: Series[float] = pa.Field(
        nullable=True,
        description="Return on equity: (net_income * 4) / equity.",
    )
    roa: Series[float] = pa.Field(
        nullable=True,
        description="Return on assets: (net_income * 4) / total_assets.",
    )
    debt_to_equity: Series[float] = pa.Field(
        nullable=True,
        description="Debt-to-equity: total_debt / equity.",
    )
    free_cash_flow_yield: Series[float] = pa.Field(
        nullable=True,
        description="FCF yield: (free_cash_flow * 4) / market_cap.",
    )
    fetched_at: Series[pd.Timestamp] = pa.Field(
        nullable=False,
        description="Timestamp (UTC, second precision) when this snapshot was computed.",
    )

    class Config:
        """Pandera config."""

        strict = True
        coerce = True
