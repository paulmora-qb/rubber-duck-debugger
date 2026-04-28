"""Pandera schema for analyst consensus and price-target snapshots."""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import DataFrameModel
from pandera.typing import Series


class AnalystConsensusSchema(DataFrameModel):
    """Schema for analyst buy/sell/hold consensus fetched from yfinance.

    One row = one ticker's current consensus snapshot.
    """

    ticker: Series[str] = pa.Field(
        nullable=False,
        description="Exchange-listed ticker symbol (lowercase).",
    )
    recommendation_key: Series[str] = pa.Field(
        nullable=True,
        description="Consensus recommendation key, e.g. 'buy', 'hold', 'sell'.",
    )
    recommendation_mean: Series[float] = pa.Field(
        nullable=True,
        description="Mean analyst recommendation score (1=strongBuy, 5=strongSell).",
    )
    analyst_count: Series[float] = pa.Field(
        nullable=True,
        ge=0,
        description="Number of analyst opinions (stored as float to allow NaN).",
    )
    target_mean_price: Series[float] = pa.Field(
        nullable=True,
        ge=0,
        description="Mean analyst 12-month price target.",
    )
    target_high_price: Series[float] = pa.Field(
        nullable=True,
        ge=0,
        description="Highest analyst 12-month price target.",
    )
    target_low_price: Series[float] = pa.Field(
        nullable=True,
        ge=0,
        description="Lowest analyst 12-month price target.",
    )
    target_median_price: Series[float] = pa.Field(
        nullable=True,
        ge=0,
        description="Median analyst 12-month price target.",
    )
    current_price: Series[float] = pa.Field(
        nullable=True,
        ge=0,
        description="Current market price at time of fetch.",
    )
    fetched_at: Series[pd.Timestamp] = pa.Field(
        nullable=False,
        description="Timestamp (UTC, second precision) when this snapshot was fetched.",
    )

    class Config:
        """Pandera config."""

        strict = True
        coerce = True
