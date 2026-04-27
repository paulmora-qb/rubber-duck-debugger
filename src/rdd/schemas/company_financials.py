"""Pandera schema for company financial statements (income, balance sheet, cash flow)."""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import DataFrameModel
from pandera.typing import Series


class CompanyFinancialsSchema(DataFrameModel):
    """Schema for company financials fetched from yfinance.

    One row = one reporting period for one ticker.
    Applies to both quarterly and annual datasets — same columns, different cadence.
    All monetary values are in the reporting currency (typically USD).
    """

    ticker: Series[str] = pa.Field(nullable=False)
    period_end: Series[pd.Timestamp] = pa.Field(
        nullable=False,
        description="Period end date as reported (tz-naive).",
    )

    # --- Income Statement ---
    total_revenue: Series[float] = pa.Field(nullable=True, ge=0)
    gross_profit: Series[float] = pa.Field(nullable=True)
    operating_income: Series[float] = pa.Field(nullable=True)
    net_income: Series[float] = pa.Field(nullable=True)
    ebitda: Series[float] = pa.Field(nullable=True)
    ebit: Series[float] = pa.Field(nullable=True)
    diluted_eps: Series[float] = pa.Field(nullable=True)
    basic_eps: Series[float] = pa.Field(nullable=True)
    cost_of_revenue: Series[float] = pa.Field(nullable=True)
    research_and_development: Series[float] = pa.Field(nullable=True)
    selling_general_and_administration: Series[float] = pa.Field(nullable=True)
    pretax_income: Series[float] = pa.Field(nullable=True)
    tax_provision: Series[float] = pa.Field(nullable=True)

    # --- Balance Sheet ---
    total_assets: Series[float] = pa.Field(nullable=True)
    total_liabilities: Series[float] = pa.Field(nullable=True)
    equity: Series[float] = pa.Field(nullable=True)
    total_debt: Series[float] = pa.Field(nullable=True)
    cash_and_equivalents: Series[float] = pa.Field(nullable=True)
    net_debt: Series[float] = pa.Field(nullable=True)
    working_capital: Series[float] = pa.Field(nullable=True)
    net_ppe: Series[float] = pa.Field(nullable=True)
    accounts_receivable: Series[float] = pa.Field(nullable=True)
    inventory: Series[float] = pa.Field(nullable=True)
    accounts_payable: Series[float] = pa.Field(nullable=True)
    long_term_debt: Series[float] = pa.Field(nullable=True)

    # --- Cash Flow ---
    free_cash_flow: Series[float] = pa.Field(nullable=True)
    operating_cash_flow: Series[float] = pa.Field(nullable=True)
    capital_expenditure: Series[float] = pa.Field(nullable=True)
    cash_dividends_paid: Series[float] = pa.Field(nullable=True)
    stock_based_compensation: Series[float] = pa.Field(nullable=True)
    depreciation_and_amortization: Series[float] = pa.Field(nullable=True)
    net_long_term_debt_issuance: Series[float] = pa.Field(nullable=True)
    repurchase_of_capital_stock: Series[float] = pa.Field(nullable=True)

    fetched_at: Series[pd.Timestamp] = pa.Field(nullable=False)

    class Config:
        """Pandera config."""

        strict = True
        coerce = True
