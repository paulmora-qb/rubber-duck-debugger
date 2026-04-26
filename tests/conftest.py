"""Shared pytest fixtures."""

from __future__ import annotations

import pandas as pd
import pytest

from rdd.schemas.company_info import CompanyInfoSchema
from rdd.schemas.company_news import CompanyNewsSchema
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


def make_company_info_df(
    ticker: str = "AAPL",
    fetched_at: str = "2024-01-02",
) -> pd.DataFrame:
    """Return a single-row company info DataFrame that satisfies CompanyInfoSchema."""
    df = CompanyInfoSchema.example(size=1)
    df["ticker"] = ticker
    df["name"] = "Apple Inc."
    df["sector"] = "Technology"
    df["industry"] = "Consumer Electronics"
    df["market_cap"] = 3_000_000_000_000.0
    df["employees"] = 161_000.0
    df["country"] = "United States"
    df["currency"] = "USD"
    df["exchange"] = "NMS"
    df["fetched_at"] = pd.Timestamp(fetched_at)
    return df


@pytest.fixture
def company_info_df() -> pd.DataFrame:
    """Single-row company info DataFrame that satisfies CompanyInfoSchema (AAPL)."""
    return make_company_info_df()


def make_company_news_df(
    n: int = 3,
    ticker: str = "AAPL",
    start: str = "2024-01-02",
) -> pd.DataFrame:
    """Return *n* valid company news rows that satisfy CompanyNewsSchema."""
    df = CompanyNewsSchema.example(size=n)
    df["ticker"] = ticker
    df["published_at"] = pd.date_range(start, periods=n, freq="D")
    df["headline"] = [f"Headline {i}" for i in range(n)]
    df["summary"] = [f"Summary {i}" for i in range(n)]
    df["source"] = "Reuters"
    df["url"] = [f"https://example.com/{i}" for i in range(n)]
    df["category"] = "company news"
    return df


@pytest.fixture
def company_news_df() -> pd.DataFrame:
    """Three-row company news DataFrame that satisfies CompanyNewsSchema (AAPL)."""
    return make_company_news_df()
