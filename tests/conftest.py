"""Shared pytest fixtures."""

from __future__ import annotations

import pandas as pd
import pytest

from rdd.schemas.ohlcv import OHLCVSchema


def make_ohlcv_df(
    n: int = 5,
    ticker: str = "AAPL",
    base_price: float = 100.0,
    start: str = "2024-01-02",
) -> pd.DataFrame:
    """Return *n* valid OHLCV rows generated from OHLCVSchema.

    Uses ``OHLCVSchema.example()`` to bootstrap the column structure, then
    overwrites the price columns with consistent OHLCV values so the
    dataframe-level checks (``high >= low``, ``close`` in ``[low, high]``) pass.
    """
    df = OHLCVSchema.example(size=n)
    df["ticker"] = ticker
    df["date"] = pd.date_range(start, periods=n, freq="B")
    df["low"] = base_price * 0.99
    df["high"] = base_price * 1.01
    df["close"] = base_price
    df["open"] = base_price * 1.005
    df["adj_close"] = base_price * 0.98
    df["volume"] = 1_000_000.0
    return df


@pytest.fixture
def ohlcv_df() -> pd.DataFrame:
    """Five-row OHLCV DataFrame that satisfies OHLCVSchema (AAPL, 2024-01-02…)."""
    return make_ohlcv_df()


def make_market_news_df(
    n: int = 3,
    category: str = "general",
    base_date: str = "2024-01-15",
    start_id: int = 1000,
) -> pd.DataFrame:
    """Return *n* valid market news rows for ``MarketNewsSchema``.

    Args:
        n: Number of articles to generate.
        category: Finnhub news category string.
        base_date: ISO date string for the first article's date; subsequent
            articles are spaced one hour apart.
        start_id: Starting ``article_id`` value (increments by 1 per row).
    """
    base = pd.Timestamp(base_date)
    rows = [
        {
            "article_id": start_id + i,
            "datetime": base + pd.Timedelta(hours=i),
            "headline": f"Market headline {i}",
            "summary": f"Summary {i}",
            "source": "Reuters",
            "url": f"https://reuters.com/article/{start_id + i}",
            "image": None,
            "category": category,
        }
        for i in range(n)
    ]
    return pd.DataFrame(rows)


@pytest.fixture
def market_news_df() -> pd.DataFrame:
    """Three-row market news DataFrame that satisfies MarketNewsSchema."""
    return make_market_news_df()
