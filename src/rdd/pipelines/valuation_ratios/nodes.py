"""Nodes for the valuation ratios derivation pipeline."""

from __future__ import annotations

import logging
import math
from collections.abc import Callable

import pandas as pd

logger = logging.getLogger(__name__)

# Ratio column names in output order (after ticker, period_end, market_cap)
_RATIO_COLS: list[str] = [
    "pe_ratio",
    "pb_ratio",
    "ev_ebitda",
    "gross_margin",
    "operating_margin",
    "net_margin",
    "roe",
    "roa",
    "debt_to_equity",
    "free_cash_flow_yield",
]


def _safe_div(numerator: float | None, denominator: float | None) -> float:
    """Return numerator / denominator, or NaN when either operand is missing/zero."""
    if (
        numerator is None
        or denominator is None
        or math.isnan(float(numerator))
        or math.isnan(float(denominator))
        or float(denominator) == 0.0
    ):
        return float("nan")
    return float(numerator) / float(denominator)


def _compute_ratios(
    ticker: str,
    latest_q: pd.Series,
    market_cap: float | None,
) -> dict[str, object]:
    """Compute all ratios from a single-period financials Series and market cap."""
    net_income = latest_q.get("net_income")
    equity = latest_q.get("equity")
    total_debt = latest_q.get("total_debt")
    cash = latest_q.get("cash_and_equivalents")
    ebitda = latest_q.get("ebitda")
    gross_profit = latest_q.get("gross_profit")
    total_revenue = latest_q.get("total_revenue")
    operating_income = latest_q.get("operating_income")
    total_assets = latest_q.get("total_assets")
    free_cash_flow = latest_q.get("free_cash_flow")

    # Annualise quarterly figures
    net_income_ann = (
        None if net_income is None or _isnan(net_income) else net_income * 4
    )
    ebitda_ann = None if ebitda is None or _isnan(ebitda) else ebitda * 4
    fcf_ann = (
        None if free_cash_flow is None or _isnan(free_cash_flow) else free_cash_flow * 4
    )

    # Enterprise value components
    ev = None
    if market_cap is not None and not _isnan(market_cap):
        debt_val = (
            total_debt if (total_debt is not None and not _isnan(total_debt)) else 0.0
        )
        cash_val = cash if (cash is not None and not _isnan(cash)) else 0.0
        ev = market_cap + debt_val - cash_val

    return {
        "ticker": ticker,
        "period_end": latest_q.get("period_end"),
        "market_cap": float("nan") if market_cap is None else market_cap,
        "pe_ratio": _safe_div(market_cap, net_income_ann),
        "pb_ratio": _safe_div(market_cap, equity),
        "ev_ebitda": _safe_div(ev, ebitda_ann),
        "gross_margin": _safe_div(gross_profit, total_revenue),
        "operating_margin": _safe_div(operating_income, total_revenue),
        "net_margin": _safe_div(net_income, total_revenue),
        "roe": _safe_div(net_income_ann, equity),
        "roa": _safe_div(net_income_ann, total_assets),
        "debt_to_equity": _safe_div(total_debt, equity),
        "free_cash_flow_yield": _safe_div(fcf_ann, market_cap),
        "fetched_at": pd.Timestamp.now("UTC").tz_convert(None).floor("s"),
    }


def _isnan(value: object) -> bool:
    try:
        return math.isnan(float(value))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return False


def compute_valuation_ratios(
    quarterly_financials: dict[str, Callable[[], pd.DataFrame]],
    company_info: dict[str, Callable[[], pd.DataFrame]],
) -> dict[str, pd.DataFrame]:
    """Derive valuation and profitability ratios from the latest quarter's financials.

    Reads the most recent quarter per ticker from ``raw_company_financials_quarterly``
    and the current market cap from ``raw_company_info``.  Returns one row per ticker.

    Tickers present in ``quarterly_financials`` but missing from ``company_info``
    (or vice-versa) are handled gracefully: the ratio row is still produced with
    NaN where source data is unavailable.

    Args:
        quarterly_financials: Lazy loaders from the quarterly PartitionedDataset.
        company_info: Lazy loaders from the company info PartitionedDataset.

    Returns:
        Mapping of ticker (lowercase) → single-row DataFrame with ratios.
    """
    result: dict[str, pd.DataFrame] = {}

    # All tickers that have quarterly financials
    all_tickers = set(quarterly_financials.keys())

    logger.info("Computing valuation ratios for %d tickers.", len(all_tickers))

    for key in sorted(all_tickers):
        try:
            fin_df = quarterly_financials[key]()
        except Exception:
            logger.warning(
                "Could not load quarterly financials for %s — skipping.", key
            )
            continue

        if fin_df is None or fin_df.empty:
            logger.debug("Empty financials for %s — skipping.", key)
            continue

        # Sort by period_end descending and take the latest quarter
        fin_df = fin_df.sort_values("period_end", ascending=False)
        latest_q = fin_df.iloc[0]

        # Load market cap from company_info if available
        market_cap: float | None = None
        if key in company_info:
            try:
                info_df = company_info[key]()
                if (
                    info_df is not None
                    and not info_df.empty
                    and "market_cap" in info_df.columns
                ):
                    mc_val = info_df["market_cap"].iloc[0]
                    if mc_val is not None and not _isnan(mc_val):
                        market_cap = float(mc_val)
            except Exception:
                logger.warning(
                    "Could not load company info for %s — market_cap will be NaN.", key
                )

        # Infer ticker symbol from the dataframe if available, else use the partition key
        ticker_sym = latest_q.get("ticker", key)

        row = _compute_ratios(str(ticker_sym), latest_q, market_cap)
        df = pd.DataFrame([row])

        # Ensure all ratio columns exist
        for col in _RATIO_COLS:
            if col not in df.columns:
                df[col] = float("nan")

        cols = ["ticker", "period_end", "market_cap", *_RATIO_COLS, "fetched_at"]
        result[key] = df[cols].reset_index(drop=True)

    logger.info("Valuation ratios complete. %d tickers written.", len(result))
    return result
