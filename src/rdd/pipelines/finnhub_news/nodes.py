"""Nodes for the Finnhub market (macro) news pipeline."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from typing import Any

import finnhub
import pandas as pd

logger = logging.getLogger(__name__)

_ARTICLE_COLUMNS = [
    "article_id",
    "datetime",
    "headline",
    "summary",
    "source",
    "url",
    "image",
    "category",
]


def _parse_articles(raw_articles: list[dict], category: str) -> pd.DataFrame:
    """Normalise a list of raw Finnhub article dicts into a tidy DataFrame.

    Args:
        raw_articles: List of article dicts as returned by ``finnhub.Client.general_news``.
        category: The Finnhub news category used to fetch these articles.

    Returns:
        DataFrame with columns matching ``_ARTICLE_COLUMNS``.  Empty DataFrame
        (with correct columns) when ``raw_articles`` is empty.
    """
    if not raw_articles:
        return pd.DataFrame(columns=_ARTICLE_COLUMNS)

    rows = [
        {
            "article_id": article.get("id"),
            "datetime": pd.Timestamp(article.get("datetime", 0), unit="s"),
            "headline": article.get("headline", ""),
            "summary": article.get("summary") or "",
            "source": article.get("source", ""),
            "url": article.get("url", ""),
            "image": article.get("image") or None,
            "category": category,
        }
        for article in raw_articles
    ]
    return pd.DataFrame(rows, columns=_ARTICLE_COLUMNS)


def fetch_market_news(
    existing_news: dict[str, Callable[[], pd.DataFrame]],
    params: dict[str, Any],
) -> dict[str, pd.DataFrame]:
    """Fetch macro market news from Finnhub and merge with stored partitions.

    For each configured category (default: ``general``), calls
    ``finnhub.Client.general_news`` and groups returned articles by calendar
    date.  Each date group is merged with any pre-existing partition for that
    date, deduplicated by ``article_id``, and added to the output dict.
    Existing partitions for dates not present in the new fetch are carried
    forward unchanged.

    Args:
        existing_news: Mapping of date-string partition key → lazy loader as
            provided by Kedro's ``NullablePartitionedDataset``.
        params: ``finnhub_news`` parameter block.  Must contain a
            ``categories`` list (e.g. ``["general"]``).

    Returns:
        Mapping of date-string → merged DataFrame for ``PartitionedDataset``
        to persist as ``data/raw/finnhub_market_news/<date>.parquet``.

    Raises:
        KeyError: If the ``FINNHUB_API_KEY`` environment variable is not set.
    """
    api_key = os.environ["FINNHUB_API_KEY"]
    client = finnhub.Client(api_key=api_key)

    categories: list[str] = params.get("categories", ["general"])

    fetched_frames: list[pd.DataFrame] = []
    for category in categories:
        raw = client.general_news(category=category, min_id=0)
        df = _parse_articles(raw, category=category)
        if not df.empty:
            fetched_frames.append(df)
            logger.info("Fetched %d articles for category '%s'.", len(df), category)
        else:
            logger.info("No articles returned for category '%s'.", category)

    # Carry existing partitions forward; we'll overwrite updated dates below.
    result: dict[str, pd.DataFrame] = {
        key: loader() for key, loader in existing_news.items()
    }

    if not fetched_frames:
        logger.info("No new articles fetched. Existing partitions unchanged.")
        return result

    new_df = pd.concat(fetched_frames, ignore_index=True)
    new_df["_date_key"] = new_df["datetime"].dt.strftime("%Y-%m-%d")

    for date_key, group in new_df.groupby("_date_key"):
        group = group.drop(columns=["_date_key"]).copy()

        if date_key in result:
            merged = (
                pd.concat([result[date_key], group], ignore_index=True)
                .drop_duplicates(subset=["article_id"])
                .sort_values("datetime")
                .reset_index(drop=True)
            )
        else:
            merged = group.sort_values("datetime").reset_index(drop=True)

        result[date_key] = merged

    logger.info(
        "Market news ingestion complete. %d date partitions written.", len(result)
    )
    return result
