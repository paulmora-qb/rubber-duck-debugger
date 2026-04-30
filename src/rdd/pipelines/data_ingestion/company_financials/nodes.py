"""Nodes for the company financials ingestion pipeline (yfinance)."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

# Maps yfinance income-statement row names → schema column names
_INCOME_FIELDS: dict[str, str] = {
    "Total Revenue": "total_revenue",
    "Gross Profit": "gross_profit",
    "Operating Income": "operating_income",
    "Net Income": "net_income",
    "EBITDA": "ebitda",
    "EBIT": "ebit",
    "Diluted EPS": "diluted_eps",
    "Basic EPS": "basic_eps",
    "Cost Of Revenue": "cost_of_revenue",
    "Research And Development": "research_and_development",
    "Selling General And Administration": "selling_general_and_administration",
    "Pretax Income": "pretax_income",
    "Tax Provision": "tax_provision",
}

_BALANCE_FIELDS: dict[str, str] = {
    "Total Assets": "total_assets",
    "Total Liabilities Net Minority Interest": "total_liabilities",
    "Common Stock Equity": "equity",
    "Total Debt": "total_debt",
    "Cash And Cash Equivalents": "cash_and_equivalents",
    "Net Debt": "net_debt",
    "Working Capital": "working_capital",
    "Net PPE": "net_ppe",
    "Accounts Receivable": "accounts_receivable",
    "Inventory": "inventory",
    "Accounts Payable": "accounts_payable",
    "Long Term Debt": "long_term_debt",
}

_CASHFLOW_FIELDS: dict[str, str] = {
    "Free Cash Flow": "free_cash_flow",
    "Operating Cash Flow": "operating_cash_flow",
    "Capital Expenditure": "capital_expenditure",
    "Cash Dividends Paid": "cash_dividends_paid",
    "Stock Based Compensation": "stock_based_compensation",
    "Depreciation And Amortization": "depreciation_and_amortization",
    "Net Long Term Debt Issuance": "net_long_term_debt_issuance",
    "Repurchase Of Capital Stock": "repurchase_of_capital_stock",
}

# All schema columns (excluding ticker, period_end, fetched_at)
_ALL_METRIC_COLS: list[str] = (
    list(_INCOME_FIELDS.values())
    + list(_BALANCE_FIELDS.values())
    + list(_CASHFLOW_FIELDS.values())
)


def _extract_statement(
    raw: pd.DataFrame,
    field_map: dict[str, str],
) -> pd.DataFrame:
    """Transpose a yfinance statement DataFrame and rename columns.

    yfinance returns metrics as rows, periods as columns.
    Returns a DataFrame with periods as rows, selected metrics as columns.
    """
    if raw is None or raw.empty:
        return pd.DataFrame()
    # Transpose: rows → periods, cols → metrics
    df = raw.T.copy()
    df.index.name = "period_end"
    df = df.reset_index()
    df["period_end"] = pd.to_datetime(df["period_end"]).dt.tz_localize(None)
    renamed: dict[str, str] = {}
    for src, dst in field_map.items():
        if src in df.columns:
            renamed[src] = dst
    df = df.rename(columns=renamed)
    keep = ["period_end"] + [c for c in field_map.values() if c in df.columns]
    return df[keep]


def _merge_statements(*dfs: pd.DataFrame) -> pd.DataFrame:
    """Outer-merge multiple statement DataFrames on period_end."""
    result = dfs[0]
    for other in dfs[1:]:
        if other.empty:
            continue
        result = result.merge(other, on="period_end", how="outer")
    return result


def _fetch_financials(
    ticker: str,
    quarterly: bool,
) -> pd.DataFrame | None:
    """Fetch and merge all three financial statements for one ticker.

    Returns None if yfinance raises an exception.
    """
    try:
        t = yf.Ticker(ticker)
        if quarterly:
            income = _extract_statement(t.quarterly_financials, _INCOME_FIELDS)
            balance = _extract_statement(t.quarterly_balance_sheet, _BALANCE_FIELDS)
            cashflow = _extract_statement(t.quarterly_cashflow, _CASHFLOW_FIELDS)
        else:
            income = _extract_statement(t.financials, _INCOME_FIELDS)
            balance = _extract_statement(t.balance_sheet, _BALANCE_FIELDS)
            cashflow = _extract_statement(t.cashflow, _CASHFLOW_FIELDS)
    except Exception:
        logger.warning(
            "Failed to fetch financials for %s — skipping.", ticker, exc_info=True
        )
        return None

    non_empty = [df for df in (income, balance, cashflow) if not df.empty]
    if not non_empty:
        return None

    merged = _merge_statements(*non_empty)
    merged["ticker"] = ticker
    merged["fetched_at"] = pd.Timestamp.now("UTC").tz_convert(None).floor("s")

    # Ensure all metric columns exist (fill missing with NaN)
    for col in _ALL_METRIC_COLS:
        if col not in merged.columns:
            merged[col] = float("nan")

    cols = ["ticker", "period_end", *_ALL_METRIC_COLS, "fetched_at"]
    return merged[cols].sort_values("period_end").reset_index(drop=True)


def _is_fresh(
    df: pd.DataFrame,
    cutoff: pd.Timestamp,
) -> bool:
    if df.empty or "fetched_at" not in df.columns:
        return False
    return pd.Timestamp(df["fetched_at"].iloc[0]) >= cutoff


def _partition_fresh_stale(
    ticker_universe: list[str],
    existing: dict[str, Callable[[], pd.DataFrame]],
    cutoff: pd.Timestamp,
    label: str,
) -> tuple[dict[str, pd.DataFrame], list[str]]:
    """Split tickers into a dict of fresh DataFrames and a list of stale ticker names."""
    fresh: dict[str, pd.DataFrame] = {}
    stale: list[str] = []
    for ticker in ticker_universe:
        key = ticker.lower()
        if key not in existing:
            stale.append(ticker)
            continue
        try:
            df = existing[key]()
            if _is_fresh(df, cutoff):
                fresh[key] = df
            else:
                stale.append(ticker)
        except Exception:
            logger.warning("Could not load existing %s data for %s.", label, ticker)
            stale.append(ticker)
    return fresh, stale


def ingest_company_financials(
    ticker_universe: list[str],
    existing_quarterly: dict[str, Callable[[], pd.DataFrame]],
    existing_annual: dict[str, Callable[[], pd.DataFrame]],
    params: dict[str, Any],
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """Fetch quarterly and annual financial statements from yfinance.

    Skips tickers whose stored data is fresher than ``params.refresh_days``.
    Failed fetches are logged and skipped.

    Args:
        ticker_universe: Sorted list of ticker symbols to process.
        existing_quarterly: Lazy loaders from quarterly PartitionedDataset.
        existing_annual: Lazy loaders from annual PartitionedDataset.
        params: ``company_financials`` parameter block.

    Returns:
        Tuple of (quarterly_dict, annual_dict) mapping ticker (lowercase) →
        DataFrame for the PartitionedDataset to persist.
    """
    refresh_days = int(params["refresh_days"])
    cutoff = pd.Timestamp.now("UTC").tz_convert(None) - pd.Timedelta(days=refresh_days)

    quarterly_out, stale_q = _partition_fresh_stale(
        ticker_universe, existing_quarterly, cutoff, "quarterly"
    )
    annual_out, stale_a = _partition_fresh_stale(
        ticker_universe, existing_annual, cutoff, "annual"
    )

    logger.info(
        "Quarterly: %d up to date, %d to fetch. Annual: %d up to date, %d to fetch.",
        len(quarterly_out),
        len(stale_q),
        len(annual_out),
        len(stale_a),
    )

    for ticker in stale_q:
        df = _fetch_financials(ticker, quarterly=True)
        if df is not None:
            quarterly_out[ticker.lower()] = df

    for ticker in stale_a:
        df = _fetch_financials(ticker, quarterly=False)
        if df is not None:
            annual_out[ticker.lower()] = df

    logger.info(
        "Company financials complete. %d quarterly, %d annual tickers written.",
        len(quarterly_out),
        len(annual_out),
    )
    return quarterly_out, annual_out
