"""Nodes for the company news ingestion pipeline (Finnhub)."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from typing import Any

import finnhub
import pandas as pd

logger = logging.getLogger(__name__)

_DATE_FMT = "%Y-%m-%d"


def _make_client() -> finnhub.Client:
    api_key = os.environ.get("FINNHUB_API_KEY", "")
    if not api_key:
        msg = "FINNHUB_API_KEY environment variable is not set."
        raise OSError(msg)
    return finnhub.Client(api_key=api_key)


def _articles_to_df(ticker: str, articles: list[dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for art in articles:
        rows.append(
            {
                "ticker": ticker,
                # tz-naive UTC timestamp — consistent with existing partitioned data
                "published_at": pd.Timestamp(art.get("datetime", 0), unit="s").floor(
                    "s"
                ),
                "headline": art.get("headline"),
                "summary": art.get("summary"),
                "source": art.get("source"),
                "url": art.get("url"),
                "category": art.get("category"),
            }
        )
    return pd.DataFrame(
        rows,
        columns=[
            "ticker",
            "published_at",
            "headline",
            "summary",
            "source",
            "url",
            "category",
        ],
    )


def _merge_articles(
    old_df: pd.DataFrame | None,
    new_df: pd.DataFrame,
) -> pd.DataFrame | None:
    if old_df is not None and not new_df.empty:
        return (
            pd.concat([old_df, new_df], ignore_index=True)
            .drop_duplicates(subset=["ticker", "published_at", "headline"])
            .sort_values("published_at")
            .reset_index(drop=True)
        )
    if old_df is not None:
        return old_df
    if not new_df.empty:
        return new_df.sort_values("published_at").reset_index(drop=True)
    return None


def ingest_company_news(
    ticker_universe: list[str],
    existing_news: dict[str, Callable[[], pd.DataFrame]],
    params: dict[str, Any],
) -> dict[str, pd.DataFrame]:
    """Fetch company news from Finnhub and merge with any existing stored articles.

    Determines the effective fetch window per ticker:
    - First run (no existing partition): fetches from ``params.start_date``.
    - Subsequent runs: fetches from the day after the latest stored article.

    Requires ``FINNHUB_API_KEY`` to be set in the environment.

    Args:
        ticker_universe: Sorted list of ticker symbols to process.
        existing_news: Mapping of ticker (lowercase) → lazy loader from Kedro's
            ``PartitionedDataset``.  Call ``loader()`` to materialise.
        params: ``company_news`` parameter block from ``params_company_news.yml``.

    Returns:
        Mapping of ticker (lowercase) → merged DataFrame for the
        ``PartitionedDataset`` to persist.
    """
    default_start = pd.Timestamp(params["start_date"])
    end_date = pd.Timestamp.now("UTC").tz_convert(None).normalize()

    client = _make_client()

    existing_data: dict[str, pd.DataFrame] = {}
    ticker_starts: dict[str, pd.Timestamp] = {}

    for ticker in ticker_universe:
        partition_key = ticker.lower()
        if partition_key in existing_news:
            try:
                df = existing_news[partition_key]()
                if not df.empty and "published_at" in df.columns:
                    existing_data[ticker] = df
                    ticker_starts[ticker] = pd.Timestamp(
                        df["published_at"].max()
                    ) + pd.Timedelta(days=1)
                    continue
            except Exception:
                logger.warning(
                    "Could not load existing news for %s — will re-fetch.", ticker
                )
        ticker_starts[ticker] = default_start

    result: dict[str, pd.DataFrame] = {}

    for ticker in ticker_universe:
        fetch_start = ticker_starts[ticker]

        if fetch_start >= end_date:
            if ticker in existing_data:
                result[ticker.lower()] = existing_data[ticker]
            continue

        try:
            articles = client.company_news(
                ticker,
                _from=fetch_start.strftime(_DATE_FMT),
                to=end_date.strftime(_DATE_FMT),
            )
        except Exception:
            logger.warning(
                "Finnhub request failed for %s — skipping.", ticker, exc_info=True
            )
            if ticker in existing_data:
                result[ticker.lower()] = existing_data[ticker]
            continue

        new_df = _articles_to_df(ticker, articles)
        merged = _merge_articles(existing_data.get(ticker), new_df)
        if merged is not None:
            result[ticker.lower()] = merged
        logger.debug("Fetched %d articles for %s.", len(new_df), ticker)

    logger.info("Company news ingestion complete. %d tickers written.", len(result))
    return result
