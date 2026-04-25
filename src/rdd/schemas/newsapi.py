"""Pandera schemas for NewsAPI data."""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import DataFrameModel
from pandera.typing import Series


class NewsAPISchema(DataFrameModel):
    """Schema for articles pulled from the NewsAPI.

    One row = one article for one query string.
    """

    query: Series[str] = pa.Field(
        nullable=False,
        description="The search query string used to fetch this article.",
    )
    published_at: Series[pd.Timestamp] = pa.Field(
        nullable=False,
        description="Article publication timestamp (UTC, timezone-naive).",
    )
    title: Series[str] = pa.Field(
        nullable=True,
        description="Article title.",
    )
    description: Series[str] = pa.Field(
        nullable=True,
        description="Short description / lede provided by NewsAPI.",
    )
    content: Series[str] = pa.Field(
        nullable=True,
        description="Truncated article body (NewsAPI free tier caps at 200 chars).",
    )
    source_name: Series[str] = pa.Field(
        nullable=True,
        description="Publisher name as returned by NewsAPI.",
    )
    url: Series[str] = pa.Field(
        nullable=True,
        description="Canonical article URL.",
    )

    class Config:
        """Pandera config."""

        strict = True
        coerce = True
