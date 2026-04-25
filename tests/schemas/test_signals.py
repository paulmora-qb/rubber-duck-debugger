"""Tests for SignalSchema."""

from __future__ import annotations

import pandas as pd
import pandera.errors
import pytest

from rdd.schemas.signals import SignalSchema


def _make_signals(
    dates: list[str] | None = None,
    tickers: list[str] | None = None,
    weights: list[float] | None = None,
) -> pd.DataFrame:
    dates = dates or ["2024-01-02", "2024-01-02", "2024-01-02"]
    tickers = tickers or ["AAPL", "MSFT", "GOOG"]
    weights = weights or [1 / 3, 1 / 3, 1 / 3]
    return pd.DataFrame(
        {
            "date": pd.to_datetime(dates),
            "ticker": tickers,
            "position": [1] * len(tickers),
            "weight": weights,
        }
    )


class TestSignalSchema:
    def test_valid_signals_pass(self) -> None:
        df = _make_signals()
        SignalSchema.validate(df)

    def test_weights_summing_to_1_1_fail(self) -> None:
        df = _make_signals(weights=[0.4, 0.4, 0.4])
        with pytest.raises(pandera.errors.SchemaError):
            SignalSchema.validate(df)

    def test_position_zero_rejected(self) -> None:
        df = _make_signals()
        df.loc[0, "position"] = 0
        with pytest.raises(pandera.errors.SchemaError):
            SignalSchema.validate(df)

    def test_position_minus_one_rejected(self) -> None:
        df = _make_signals()
        df.loc[0, "position"] = -1
        with pytest.raises(pandera.errors.SchemaError):
            SignalSchema.validate(df)

    def test_negative_weight_rejected(self) -> None:
        df = _make_signals(weights=[-0.5, 0.8, 0.7])
        with pytest.raises(pandera.errors.SchemaError):
            SignalSchema.validate(df)

    def test_multiple_dates_each_sum_to_one(self) -> None:
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(
                    ["2024-01-02", "2024-01-02", "2024-01-09", "2024-01-09"]
                ),
                "ticker": ["AAPL", "MSFT", "AAPL", "TSLA"],
                "position": [1, 1, 1, 1],
                "weight": [0.5, 0.5, 0.6, 0.4],
            }
        )
        SignalSchema.validate(df)
