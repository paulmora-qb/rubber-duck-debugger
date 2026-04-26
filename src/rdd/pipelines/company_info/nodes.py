"""Nodes for the company information ingestion pipeline."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

_INFO_FIELDS: dict[str, str] = {
    "longName": "name",
    "sector": "sector",
    "industry": "industry",
    "marketCap": "market_cap",
    "fullTimeEmployees": "employees",
    "country": "country",
    "currency": "currency",
    "exchange": "exchange",
}


def _extract_info(ticker: str, info: dict[str, Any]) -> dict[str, Any]:
    row: dict[str, Any] = {"ticker": ticker}
    for src, dst in _INFO_FIELDS.items():
        row[dst] = info.get(src)
    row["fetched_at"] = pd.Timestamp.now("UTC").tz_convert(None).floor("s")
    return row


def ingest_company_info(
    ticker_universe: list[str],
    existing_company_info: dict[str, Callable[[], pd.DataFrame]],
    params: dict[str, Any],
) -> dict[str, pd.DataFrame]:
    """Fetch company metadata from yfinance and persist per-ticker snapshots.

    Skips tickers whose existing snapshot is fresher than ``params.refresh_days``
    to avoid unnecessary API calls.  Failed fetches are logged and skipped so a
    single bad ticker does not abort the whole run.

    Args:
        ticker_universe: Sorted list of ticker symbols to process.
        existing_company_info: Mapping of ticker (lowercase) → lazy loader from
            Kedro's ``PartitionedDataset``.  Call ``loader()`` to materialise.
        params: ``company_info`` parameter block from ``params_company_info.yml``.

    Returns:
        Mapping of ticker (lowercase) → single-row DataFrame for the
        ``PartitionedDataset`` to persist.
    """
    refresh_days = int(params["refresh_days"])
    cutoff = pd.Timestamp.now("UTC").tz_convert(None).tz_localize(None) - pd.Timedelta(
        days=refresh_days
    )

    result: dict[str, pd.DataFrame] = {}
    stale: list[str] = []

    for ticker in ticker_universe:
        partition_key = ticker.lower()
        if partition_key in existing_company_info:
            try:
                df = existing_company_info[partition_key]()
                if not df.empty and "fetched_at" in df.columns:
                    last_fetch = pd.Timestamp(df["fetched_at"].iloc[0])
                    if last_fetch >= cutoff:
                        result[partition_key] = df
                        continue
            except Exception:
                logger.warning(
                    "Could not load existing snapshot for %s — will re-fetch.", ticker
                )
        stale.append(ticker)

    logger.info(
        "%d tickers up to date, %d to fetch (refresh_days=%d).",
        len(result),
        len(stale),
        refresh_days,
    )

    for ticker in stale:
        try:
            info = yf.Ticker(ticker).info
            row = _extract_info(ticker, info)
            result[ticker.lower()] = pd.DataFrame([row])
        except Exception:
            logger.warning(
                "Failed to fetch info for %s — skipping.", ticker, exc_info=True
            )

    logger.info("Company info ingestion complete. %d tickers written.", len(result))
    return result
