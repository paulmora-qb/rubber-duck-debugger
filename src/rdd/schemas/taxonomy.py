"""Pandera schemas for the stock–industry–country taxonomy and news linkage tables."""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import DataFrameModel
from pandera.typing import Series


class TaxonomySchema(DataFrameModel):
    """Schema for the deduplicated stock taxonomy table.

    One row = one ticker. Built from enriched OHLCV fundamentals.
    """

    ticker: Series[str] = pa.Field(nullable=False, unique=True)
    company_name: Series[str] = pa.Field(nullable=True)
    sector: Series[str] = pa.Field(nullable=True)
    industry: Series[str] = pa.Field(nullable=True)
    country: Series[str] = pa.Field(nullable=True)
    exchange: Series[str] = pa.Field(nullable=True)

    class Config:
        """Pandera config."""

        strict = True
        coerce = True


class IndustryNewsLinksSchema(DataFrameModel):
    """Schema for the article → industry match table.

    One row = one (article, industry) match. An article may appear in multiple
    rows if it matches more than one industry.
    """

    article_url: Series[str] = pa.Field(nullable=False)
    published_at: Series[pd.Timestamp] = pa.Field(nullable=False)
    matched_industry: Series[str] = pa.Field(nullable=False)
    matched_country: Series[str] = pa.Field(nullable=True)
    match_score: Series[float] = pa.Field(nullable=False, ge=0.0, le=1.0)

    class Config:
        """Pandera config."""

        strict = True
        coerce = True


class StockNewsLinksSchema(DataFrameModel):
    """Schema for the article → ticker relevance table.

    One row = one (ticker, article) pair.
    ``relevance`` encodes how the link was established.
    """

    ticker: Series[str] = pa.Field(nullable=False)
    article_url: Series[str] = pa.Field(nullable=False)
    source: Series[str] = pa.Field(
        nullable=False,
        isin=["finnhub", "newsapi"],
        description="Origin of the article.",
    )
    published_at: Series[pd.Timestamp] = pa.Field(nullable=False)
    relevance: Series[str] = pa.Field(
        nullable=False,
        isin=["direct", "industry", "country"],
        description=(
            "direct   — article explicitly names the ticker (Finnhub feed);\n"
            "industry — matched via industry keyword;\n"
            "country  — matched via country filter only."
        ),
    )

    class Config:
        """Pandera config."""

        strict = True
        coerce = True
