"""Pandera schema for Finnhub company (micro) news articles."""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import DataFrameModel
from pandera.typing import Series


class CompanyNewsSchema(DataFrameModel):
    """Schema for company-level news articles sourced from Finnhub.

    One row = one article for one company. Articles are partitioned by ticker
    and stored under ``data/raw/finnhub_company_news/{ticker}.parquet``.

    The ``summary`` field is nullable because quality varies by source: Yahoo
    Finance articles typically include a real paragraph; Reuters articles via
    Google News often return only a truncated headline repeat.
    """

    article_id: Series[int] = pa.Field(
        nullable=False,
        description="Finnhub-assigned article identifier. Primary deduplication key across incremental runs.",
    )
    ticker: Series[str] = pa.Field(
        nullable=False,
        description="Exchange-listed ticker symbol in yfinance format (e.g. 'AAPL', 'BRK-B'). Sourced from Finnhub's 'related' field.",
    )
    datetime: Series[pd.Timestamp] = pa.Field(
        nullable=False,
        description="Publication timestamp as a timezone-naive UTC timestamp, converted from Finnhub's Unix integer.",
    )
    headline: Series[str] = pa.Field(
        nullable=False,
        description="Article headline text. Always present; minimum signal unit for sentiment analysis.",
    )
    summary: Series[str] = pa.Field(
        nullable=True,
        description="Article summary or lead paragraph. Quality varies: Yahoo Finance provides real paragraphs; Reuters/Google News often duplicates the headline.",
    )
    source: Series[str] = pa.Field(
        nullable=False,
        description="Publisher name as returned by Finnhub (e.g. 'Yahoo', 'Bloomberg', 'CNBC', 'Benzinga').",
    )
    url: Series[str] = pa.Field(
        nullable=False,
        description="Article URL. May be a direct publisher link or a Finnhub proxy URL (https://finnhub.io/api/news?id=...).",
    )

    class Config:
        """Pandera config."""

        strict = True
        coerce = True
