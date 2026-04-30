"""Nodes for the analyst consensus ingestion pipeline (yfinance)."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)

# Maps yfinance info keys → schema column names
_CONSENSUS_FIELDS: dict[str, str] = {
    "recommendationKey": "recommendation_key",
    "recommendationMean": "recommendation_mean",
    "numberOfAnalystOpinions": "analyst_count",
    "targetMeanPrice": "target_mean_price",
    "targetHighPrice": "target_high_price",
    "targetLowPrice": "target_low_price",
    "targetMedianPrice": "target_median_price",
    "currentPrice": "current_price",
}


def _fetch_analyst_consensus(ticker: str) -> pd.DataFrame | None:
    """Fetch analyst consensus data for one ticker from yfinance.

    Returns None if the fetch fails.
    """
    try:
        info = yf.Ticker(ticker).info
    except Exception:
        logger.warning(
            "Failed to fetch analyst consensus for %s — skipping.",
            ticker,
            exc_info=True,
        )
        return None

    row: dict[str, object] = {"ticker": ticker}
    for src, dst in _CONSENSUS_FIELDS.items():
        row[dst] = info.get(src)
    row["fetched_at"] = pd.Timestamp.now("UTC").tz_convert(None).floor("s")

    return pd.DataFrame([row])


def ingest_analyst_consensus(
    ticker_universe: list[str],
    existing: dict[str, Callable[[], pd.DataFrame]],
    params: dict[str, Any],
) -> dict[str, pd.DataFrame]:
    """Fetch analyst buy/sell/hold consensus and price targets from yfinance.

    Skips tickers whose stored snapshot is fresher than ``params.refresh_days``.
    Failed fetches are logged and skipped.

    Args:
        ticker_universe: Sorted list of ticker symbols to process.
        existing: Lazy loaders from the existing PartitionedDataset.
        params: ``analyst_consensus`` parameter block.

    Returns:
        Mapping of ticker (lowercase) → single-row DataFrame for the
        PartitionedDataset to persist.
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
                    "Could not load existing consensus for %s — will re-fetch.", ticker
                )
        stale.append(ticker)

    logger.info(
        "%d tickers up to date, %d to fetch (refresh_days=%d).",
        len(result),
        len(stale),
        refresh_days,
    )

    for ticker in stale:
        df = _fetch_analyst_consensus(ticker)
        if df is not None:
            result[ticker.lower()] = df

    logger.info(
        "Analyst consensus ingestion complete. %d tickers written.", len(result)
    )
    return result
