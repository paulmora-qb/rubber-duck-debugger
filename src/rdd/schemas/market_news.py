"""Pandera schema for Finnhub market (macro) news articles."""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import DataFrameModel
from pandera.typing import Series


class MarketNewsSchema(DataFrameModel):
    """Schema for macro market news articles sourced from Finnhub.

    One row = one article. Articles are partitioned by calendar date derived
    from the article's ``datetime`` field and stored under
    ``data/raw/finnhub_market_news/<YYYY-MM-DD>.parquet``.
    """

    article_id: Series[int] = pa.Field(
        nullable=False,
        description="Finnhub-assigned article identifier. Used as the deduplication key across runs.",
    )
    datetime: Series[pd.Timestamp] = pa.Field(
        nullable=False,
        description="Publication timestamp as a timezone-naive UTC timestamp.",
    )
    headline: Series[str] = pa.Field(
        nullable=False,
        description="Article headline text.",
    )
    summary: Series[str] = pa.Field(
        nullable=True,
        description="Short summary or lead paragraph provided by Finnhub (may be empty).",
    )
    source: Series[str] = pa.Field(
        nullable=False,
        description="Publisher name (e.g. 'Reuters', 'Bloomberg').",
    )
    url: Series[str] = pa.Field(
        nullable=False,
        description="Canonical URL for the full article.",
    )
    image: Series[str] = pa.Field(
        nullable=True,
        description="URL of the article's lead image; absent for many articles.",
    )
    category: Series[str] = pa.Field(
        nullable=False,
        description="Finnhub news category used to retrieve the article (e.g. 'general', 'forex').",
    )

    class Config:
        """Pandera config."""

        strict = True
        coerce = True
