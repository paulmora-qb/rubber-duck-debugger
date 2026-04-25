"""Nodes for the Finnhub company (micro) news pipeline."""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable
from typing import Any

import finnhub
import pandas as pd

from rdd.pipelines.data_ingestion.nodes import fetch_ticker_universe

logger = logging.getLogger(__name__)

_ARTICLE_COLUMNS = [
    "article_id",
    "ticker",
    "datetime",
    "headline",
    "summary",
    "source",
    "url",
]


def _parse_articles(raw_articles: list[dict], ticker: str) -> pd.DataFrame:
    """Normalise raw Finnhub company-news dicts into a tidy DataFrame.

    Args:
        raw_articles: List of article dicts from ``finnhub.Client.company_news``.
        ticker: The ticker symbol used to fetch these articles (stored as-is).

    Returns:
        DataFrame with columns matching ``_ARTICLE_COLUMNS``.  Returns an empty
        DataFrame with correct columns when ``raw_articles`` is empty.
    """
    if not raw_articles:
        return pd.DataFrame(columns=_ARTICLE_COLUMNS)

    rows = [
        {
            "article_id": article.get("id"),
            "ticker": ticker,
            "datetime": pd.Timestamp(article.get("datetime", 0), unit="s"),
            "headline": article.get("headline", ""),
            "summary": article.get("summary") or None,
            "source": article.get("source", ""),
            "url": article.get("url", ""),
        }
        for article in raw_articles
    ]
    return pd.DataFrame(rows, columns=_ARTICLE_COLUMNS)


def fetch_company_news(
    ticker_universe: list[str],
    existing_news: dict[str, Callable[[], pd.DataFrame]],
    params: dict[str, Any],
) -> dict[str, pd.DataFrame]:
    """Fetch per-company news from Finnhub and merge with stored partitions.

    For each ticker in the universe, determines the effective fetch window:
    - First run (no existing partition): fetches from ``params.start_date``.
    - Subsequent runs: fetches from the day after the latest stored article.

    A configurable sleep between tickers keeps request rate below 60/min.

    Args:
        ticker_universe: Sorted list of ticker symbols to process.
        existing_news: Mapping of lowercase ticker → lazy loader as provided
            by Kedro's ``NullablePartitionedDataset``.
        params: ``finnhub_company_news`` parameter block. Keys:
            ``start_date`` (str ISO date), ``sleep_seconds`` (float).

    Returns:
        Mapping of lowercase ticker → merged DataFrame for
        ``PartitionedDataset`` to persist as
        ``data/raw/finnhub_company_news/{ticker}.parquet``.

    Raises:
        KeyError: If the ``FINNHUB_API_KEY`` environment variable is not set.
    """
    api_key = os.environ["FINNHUB_API_KEY"]
    client = finnhub.Client(api_key=api_key)

    default_start = pd.Timestamp(params["start_date"])
    sleep_seconds: float = float(params.get("sleep_seconds", 1.0))
    end_date = pd.Timestamp.today().normalize()

    result: dict[str, pd.DataFrame] = {}
    n = len(ticker_universe)

    for i, ticker in enumerate(ticker_universe):
        partition_key = ticker.lower()
        # Finnhub expects dot-notation (BRK.B); yfinance normalises to hyphens (BRK-B)
        finnhub_ticker = ticker.replace("-", ".")

        # Determine incremental start date for this ticker
        if partition_key in existing_news:
            try:
                existing_df = existing_news[partition_key]()
                max_dt = pd.Timestamp(existing_df["datetime"].max())
                start = (max_dt + pd.Timedelta(days=1)).normalize()
                result[partition_key] = existing_df
            except Exception:
                logger.warning("Could not load existing partition for %s.", ticker)
                start = default_start
        else:
            start = default_start

        if start >= end_date:
            logger.debug("%s is up to date — skipping.", ticker)
            if i < n - 1:
                time.sleep(sleep_seconds)
            continue

        try:
            raw = client.company_news(
                finnhub_ticker,
                _from=start.strftime("%Y-%m-%d"),
                to=end_date.strftime("%Y-%m-%d"),
            )
        except Exception:
            logger.warning(
                "Failed to fetch news for %s — skipping.", ticker, exc_info=True
            )
            if i < n - 1:
                time.sleep(sleep_seconds)
            continue

        new_df = _parse_articles(raw, ticker=ticker)
        logger.info("[%d/%d] %s: %d new articles.", i + 1, n, ticker, len(new_df))

        if new_df.empty:
            if i < n - 1:
                time.sleep(sleep_seconds)
            continue

        if partition_key in result:
            merged = (
                pd.concat([result[partition_key], new_df], ignore_index=True)
                .drop_duplicates(subset=["article_id"])
                .sort_values("datetime")
                .reset_index(drop=True)
            )
        else:
            merged = new_df.sort_values("datetime").reset_index(drop=True)

        result[partition_key] = merged

        if i < n - 1:
            time.sleep(sleep_seconds)

    logger.info("Company news ingestion complete. %d tickers written.", len(result))
    return result


# Re-export so the pipeline module can import both nodes from one place.
__all__ = ["fetch_company_news", "fetch_ticker_universe"]
