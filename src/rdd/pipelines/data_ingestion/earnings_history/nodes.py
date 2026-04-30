"""Nodes for the earnings history ingestion pipeline (yfinance)."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)


def _normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename yfinance earnings_history columns to schema names.

    Handles both index-named and column-named date fields, and various yfinance
    column capitalisation conventions.
    """
    # Reset a date index so we can work with it as a column
    if df.index.name and "date" in str(df.index.name).lower():
        df = df.reset_index()

    # Normalise all column names: lowercase, spaces → underscore, strip parens
    col_map = {
        col: col.lower().replace(" ", "_").replace("(%)", "pct").replace("()", "")
        for col in df.columns
    }
    df = df.rename(columns=col_map)

    # Map normalised names to schema names
    schema_map: dict[str, str] = {}
    for col in list(df.columns):
        col_norm = col.lower().replace(" ", "_")
        if "earnings_date" in col_norm or col_norm in ("earnings_date", "index"):
            schema_map[col] = "earnings_date"
        elif "eps_estimate" in col_norm:
            schema_map[col] = "eps_estimate"
        elif "reported_eps" in col_norm or col_norm == "reported":
            schema_map[col] = "reported_eps"
        elif "surprise" in col_norm:
            schema_map[col] = "surprise_pct"

    return df.rename(columns=schema_map)


def _fetch_earnings_history(ticker: str) -> pd.DataFrame | None:
    """Fetch historical EPS actuals vs estimates for one ticker from yfinance.

    Returns None if the fetch fails or yields no data.
    """
    try:
        raw = yf.Ticker(ticker).earnings_history
    except Exception:
        logger.warning(
            "Failed to fetch earnings history for %s — skipping.", ticker, exc_info=True
        )
        return None

    if raw is None or (hasattr(raw, "empty") and raw.empty):
        logger.debug("No earnings history available for %s.", ticker)
        return None

    df = _normalise_columns(raw.copy())

    # Ensure earnings_date column exists (fallback: use integer index column)
    if "earnings_date" not in df.columns and len(df.columns) > 0:
        df = df.rename(columns={df.columns[0]: "earnings_date"})

    df["ticker"] = ticker
    df["fetched_at"] = pd.Timestamp.now("UTC").tz_convert(None).floor("s")

    # Coerce earnings_date to tz-naive Timestamp
    if "earnings_date" in df.columns:
        df["earnings_date"] = pd.to_datetime(df["earnings_date"]).dt.tz_localize(None)

    # Ensure numeric columns are float; add missing ones as NaN
    for col in ("eps_estimate", "reported_eps", "surprise_pct"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = float("nan")

    cols = [
        "ticker",
        "earnings_date",
        "eps_estimate",
        "reported_eps",
        "surprise_pct",
        "fetched_at",
    ]
    cols = [c for c in cols if c in df.columns]
    df = df[cols].reset_index(drop=True)

    return df if not df.empty else None


def ingest_earnings_history(
    ticker_universe: list[str],
    existing: dict[str, Callable[[], pd.DataFrame]],
    params: dict[str, Any],
) -> dict[str, pd.DataFrame]:
    """Fetch historical EPS actuals vs estimates from yfinance.

    Skips tickers whose stored data is fresher than ``params.refresh_days``.
    Failed fetches are logged and skipped.

    Args:
        ticker_universe: Sorted list of ticker symbols to process.
        existing: Lazy loaders from the existing PartitionedDataset.
        params: ``earnings_history`` parameter block.

    Returns:
        Mapping of ticker (lowercase) → DataFrame for the PartitionedDataset
        to persist.
    """
    refresh_days = int(params["refresh_days"])
    cutoff = pd.Timestamp.now("UTC").tz_convert(None) - pd.Timedelta(days=refresh_days)

    result: dict[str, pd.DataFrame] = {}
    stale: list[str] = []

    for ticker in ticker_universe:
        key = ticker.lower()
        if key in existing:
            try:
                df = existing[key]()
                if not df.empty and "fetched_at" in df.columns:
                    last_fetch = pd.Timestamp(df["fetched_at"].iloc[0])
                    if last_fetch >= cutoff:
                        result[key] = df
                        continue
            except Exception:
                logger.warning(
                    "Could not load existing earnings history for %s — will re-fetch.",
                    ticker,
                )
        stale.append(ticker)

    logger.info(
        "%d tickers up to date, %d to fetch (refresh_days=%d).",
        len(result),
        len(stale),
        refresh_days,
    )

    for ticker in stale:
        df = _fetch_earnings_history(ticker)
        if df is not None:
            result[ticker.lower()] = df

    logger.info("Earnings history ingestion complete. %d tickers written.", len(result))
    return result
