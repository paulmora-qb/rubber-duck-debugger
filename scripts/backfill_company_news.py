#!/usr/bin/env python3
"""One-shot backfill: fetch N days of history before each ticker's earliest stored article.

Usage:
    uv run python scripts/backfill_company_news.py            # 7 days before earliest
    uv run python scripts/backfill_company_news.py --days 14  # 14 days
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from pathlib import Path

import finnhub
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
_logger = logging.getLogger(__name__)

_DATE_FMT = "%Y-%m-%d"
_NEWS_DIR = Path(__file__).parent.parent / "data" / "raw" / "company_news"
_RATE_LIMIT_PAUSE = 1.1  # seconds between calls (free tier: 60/min)


def _make_client() -> finnhub.Client:
    api_key = os.environ.get("FINNHUB_API_KEY", "")
    if not api_key:
        raise OSError("FINNHUB_API_KEY environment variable is not set.")
    return finnhub.Client(api_key=api_key)


def _articles_to_df(ticker: str, articles: list[dict]) -> pd.DataFrame:
    rows = [
        {
            "ticker": ticker,
            "published_at": pd.Timestamp(art.get("datetime", 0), unit="s").floor("s"),
            "headline": art.get("headline"),
            "summary": art.get("summary"),
            "source": art.get("source"),
            "url": art.get("url"),
            "category": art.get("category"),
        }
        for art in articles
    ]
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


def _merge(old: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    return (
        pd.concat([old, new], ignore_index=True)
        .drop_duplicates(subset=["ticker", "published_at", "headline"])
        .sort_values("published_at")
        .reset_index(drop=True)
    )


def main() -> None:
    """Backfill company news by fetching N days before each ticker's earliest stored article."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--days", type=int, default=7, help="Days of history to backfill"
    )
    args = parser.parse_args()

    if not _NEWS_DIR.exists():
        _logger.error(
            "No company_news directory found at %s — run the pipeline first.", _NEWS_DIR
        )
        return

    parquets = sorted(_NEWS_DIR.glob("*.parquet"))
    if not parquets:
        _logger.error("No parquet files found — run the pipeline first.")
        return

    client = _make_client()
    updated = 0
    skipped = 0

    for f in parquets:
        ticker = f.stem.upper()
        try:
            existing = pd.read_parquet(f)
        except Exception:
            _logger.warning("Could not read %s — skipping.", f)
            skipped += 1
            continue

        if existing.empty or "published_at" not in existing.columns:
            skipped += 1
            continue

        earliest = pd.Timestamp(existing["published_at"].min())
        fetch_to = (earliest - pd.Timedelta(days=1)).normalize()
        fetch_from = fetch_to - pd.Timedelta(days=args.days - 1)

        if fetch_from >= earliest:
            skipped += 1
            continue

        try:
            articles = client.company_news(
                ticker,
                _from=fetch_from.strftime(_DATE_FMT),
                to=fetch_to.strftime(_DATE_FMT),
            )
        except Exception:
            _logger.warning(
                "Finnhub request failed for %s — skipping.", ticker, exc_info=True
            )
            skipped += 1
            time.sleep(_RATE_LIMIT_PAUSE)
            continue

        if not articles:
            _logger.debug(
                "%s: no articles in %s -%s", ticker, fetch_from.date(), fetch_to.date()
            )
            skipped += 1
            time.sleep(_RATE_LIMIT_PAUSE)
            continue

        new_df = _articles_to_df(ticker, articles)
        merged = _merge(existing, new_df)
        merged.to_parquet(f, index=False)
        _logger.info(
            "%s: added %d articles (%s -%s), total now %d",
            ticker,
            len(new_df),
            fetch_from.date(),
            fetch_to.date(),
            len(merged),
        )
        updated += 1
        time.sleep(_RATE_LIMIT_PAUSE)

    _logger.info("Done. %d tickers updated, %d skipped.", updated, skipped)


if __name__ == "__main__":
    main()
