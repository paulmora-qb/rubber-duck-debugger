"""Pandera schema for momentum strategy signals."""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import DataFrameModel
from pandera.typing import Series


class MomentumSignalSchema(DataFrameModel):
    """Schema for the cross-sectional momentum signal table.

    One row = one (date, ticker) observation on a rebalance date.
    Only rebalance dates are stored; intra-period days are not represented.
    """

    date: Series[pd.Timestamp] = pa.Field(
        nullable=False,
        description="Rebalance date (monthly, ~21 trading days apart).",
    )
    ticker: Series[str] = pa.Field(
        nullable=False,
        description="Ticker symbol.",
    )
    score: Series[float] = pa.Field(
        nullable=True,
        description="12-1 momentum score: (close_{t-21} / close_{t-252}) - 1.",
    )
    rank: Series[int] = pa.Field(
        nullable=True,
        ge=1,
        description="Cross-sectional rank (1 = lowest score).",
    )
    percentile: Series[float] = pa.Field(
        nullable=True,
        ge=0.0,
        le=1.0,
        description="Rank expressed as a percentile within the universe.",
    )
    in_portfolio: Series[bool] = pa.Field(
        nullable=False,
        description="True if the ticker is in the long basket for this rebalance.",
    )

    class Config:
        """Pandera config."""

        strict = True
        coerce = True

    @pa.dataframe_check
    def rank_matches_percentile_direction(cls, df: pd.DataFrame) -> pd.Series:
        """Higher rank should correspond to higher percentile (monotone relationship)."""
        mask = df["rank"].notna() & df["percentile"].notna()
        sub = df[mask]
        if sub.empty:
            return pd.Series(True, index=df.index)
        corr = sub["rank"].corr(sub["percentile"])
        # Allow corr=NaN when all ranks are identical (single-ticker universe)
        result = pd.Series(True, index=df.index)
        if corr is not None and not (corr != corr):  # NaN check
            result[:] = corr >= 0
        return result
