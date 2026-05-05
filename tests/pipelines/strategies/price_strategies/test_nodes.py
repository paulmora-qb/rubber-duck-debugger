"""Unit tests for price_strategies pipeline nodes.

Strategy nodes are tested with synthetic price series designed to produce
unambiguous signal directions. Each test class covers one strategy:
  - bullish series  → holdings are produced with weights summing to 1.0
  - bearish series  → no holdings (all scores filtered as non-positive)
  - insufficient data → no holdings produced

The rebalance-date logic depends on today's date; tests use data that spans
~620 business days from 2024-01-02, which covers the 12-week backfill window
as of mid-2026.
"""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from rdd.pipelines.strategies.price_strategies.nodes import (
    _build_holdings,
    compute_52w_high_holdings,
    compute_adx_holdings,
    compute_cross_sect_momentum_holdings,
    compute_donchian_holdings,
    compute_obv_holdings,
)
from tests.conftest import make_price_ohlcv

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_N = 620  # ~620 business days covers the 12-week backfill window from 2024
_START = "2024-01-02"

_UPTREND = [100.0 + i * 0.5 for i in range(_N)]
_DOWNTREND = [400.0 - i * 0.5 for i in range(_N)]

_BASE_PARAMS: dict = {
    "backfill_weeks": 12,
    "top_pct": 1.0,  # take all positive-score tickers to simplify assertions
    "donchian": {"window": 20, "strategy_name": "donchian_breakout"},
    "high_52w": {
        "window": 252,
        "proximity_threshold": 0.95,
        "strategy_name": "high_52w",
    },
    "cross_sect_momentum": {
        "lookback": 252,
        "skip": 21,
        "strategy_name": "cross_sect_momentum",
    },
    "obv": {"window": 21, "strategy_name": "obv_momentum"},
    "adx": {"window": 14, "adx_threshold": 25.0, "strategy_name": "adx_trend"},
}


def _ohlcv(
    df: pd.DataFrame, key: str = "aapl"
) -> dict[str, Callable[[], pd.DataFrame]]:
    return {key: lambda _df=df: _df}


# ---------------------------------------------------------------------------
# _build_holdings (shared helper)
# ---------------------------------------------------------------------------


class TestBuildHoldings:
    def test_positive_scores_produce_holdings(self) -> None:
        scores = {"aapl": 0.3, "msft": 0.5, "nvda": 0.1}
        result = _build_holdings(
            "test", pd.Timestamp("2026-01-01"), scores, top_pct=1.0
        )
        assert result is not None
        assert set(result["ticker"]) == {"AAPL", "MSFT", "NVDA"}
        assert abs(result["weight"].sum() - 1.0) < 0.01

    def test_all_negative_scores_returns_none(self) -> None:
        scores = {"aapl": -0.3, "msft": -0.1}
        result = _build_holdings(
            "test", pd.Timestamp("2026-01-01"), scores, top_pct=1.0
        )
        assert result is None

    def test_top_pct_limits_holdings(self) -> None:
        scores = {f"t{i}": float(i) for i in range(10)}
        result = _build_holdings(
            "test", pd.Timestamp("2026-01-01"), scores, top_pct=0.3
        )
        assert result is not None
        # top 30% of 10 = 3 tickers (the 3 highest scores: t7, t8, t9)
        assert len(result) == 3

    def test_mixed_scores_filters_negatives_from_top(self) -> None:
        # top 50%: t3 (3.0), t2 (2.0), t1 (1.0) — all positive → 3 holdings
        # if top had negatives they'd be dropped
        scores = {"t1": 1.0, "t2": 2.0, "t3": 3.0, "neg1": -1.0, "neg2": -2.0}
        result = _build_holdings(
            "test", pd.Timestamp("2026-01-01"), scores, top_pct=0.5
        )
        assert result is not None
        assert all(result["weight"] > 0)

    def test_empty_scores_returns_none(self) -> None:
        result = _build_holdings("test", pd.Timestamp("2026-01-01"), {}, top_pct=1.0)
        assert result is None

    def test_single_ticker_weight_is_one(self) -> None:
        result = _build_holdings(
            "test", pd.Timestamp("2026-01-01"), {"aapl": 0.5}, top_pct=1.0
        )
        assert result is not None
        assert abs(result["weight"].iloc[0] - 1.0) < 1e-9


# ---------------------------------------------------------------------------
# Donchian Channel Breakout
# ---------------------------------------------------------------------------


class TestComputeDonchianHoldings:
    def test_uptrend_produces_holdings(self) -> None:
        # Steadily rising prices → last price near N-day high → score near +0.5
        df = make_price_ohlcv(_UPTREND, start=_START)
        result = compute_donchian_holdings(_ohlcv(df), _BASE_PARAMS)
        assert len(result) > 0
        for holdings in result.values():
            assert abs(holdings["weight"].sum() - 1.0) < 0.01
            assert "AAPL" in holdings["ticker"].values

    def test_downtrend_produces_no_holdings(self) -> None:
        # Steadily falling prices → last price near N-day low → score near -0.5
        df = make_price_ohlcv(_DOWNTREND, start=_START)
        result = compute_donchian_holdings(_ohlcv(df), _BASE_PARAMS)
        assert len(result) == 0

    def test_insufficient_data_skips_ticker(self) -> None:
        # Only 5 rows — less than window=20
        df = make_price_ohlcv([100.0 + i for i in range(5)], start=_START)
        result = compute_donchian_holdings(_ohlcv(df), _BASE_PARAMS)
        assert len(result) == 0

    def test_holdings_conform_to_schema(self) -> None:
        df = make_price_ohlcv(_UPTREND, start=_START)
        result = compute_donchian_holdings(_ohlcv(df), _BASE_PARAMS)
        for holdings in result.values():
            assert list(holdings.columns) == ["strategy", "date", "ticker", "weight"]
            assert (holdings["strategy"] == "donchian_breakout").all()


# ---------------------------------------------------------------------------
# 52-Week High Proximity
# ---------------------------------------------------------------------------


class TestCompute52wHighHoldings:
    def test_price_at_52w_high_produces_holdings(self) -> None:
        # Monotonically increasing → last price IS the 252-day high → score > 0
        df = make_price_ohlcv(_UPTREND, start=_START)
        result = compute_52w_high_holdings(_ohlcv(df), _BASE_PARAMS)
        assert len(result) > 0
        for holdings in result.values():
            assert abs(holdings["weight"].sum() - 1.0) < 0.01

    def test_price_far_below_52w_high_no_holdings(self) -> None:
        # Steep downtrend: current price << rolling max → score << 0
        steep_down = [500.0 - i * 0.8 for i in range(_N)]
        df = make_price_ohlcv(steep_down, start=_START)
        result = compute_52w_high_holdings(_ohlcv(df), _BASE_PARAMS)
        assert len(result) == 0

    def test_insufficient_data_skips_ticker(self) -> None:
        df = make_price_ohlcv([100.0] * 100, start=_START)
        result = compute_52w_high_holdings(_ohlcv(df), _BASE_PARAMS)
        assert len(result) == 0

    def test_strategy_name_in_holdings(self) -> None:
        df = make_price_ohlcv(_UPTREND, start=_START)
        result = compute_52w_high_holdings(_ohlcv(df), _BASE_PARAMS)
        for holdings in result.values():
            assert (holdings["strategy"] == "high_52w").all()


# ---------------------------------------------------------------------------
# Cross-sectional Momentum
# ---------------------------------------------------------------------------


class TestComputeCrossSectMomentumHoldings:
    def test_uptrend_produces_positive_score(self) -> None:
        df = make_price_ohlcv(_UPTREND, start=_START)
        result = compute_cross_sect_momentum_holdings(_ohlcv(df), _BASE_PARAMS)
        assert len(result) > 0
        for holdings in result.values():
            assert abs(holdings["weight"].sum() - 1.0) < 0.01

    def test_downtrend_produces_no_holdings(self) -> None:
        df = make_price_ohlcv(_DOWNTREND, start=_START)
        result = compute_cross_sect_momentum_holdings(_ohlcv(df), _BASE_PARAMS)
        assert len(result) == 0

    def test_insufficient_data_skips(self) -> None:
        # Need lookback=252 rows; only provide 100
        df = make_price_ohlcv([100.0 + i for i in range(100)], start=_START)
        result = compute_cross_sect_momentum_holdings(_ohlcv(df), _BASE_PARAMS)
        assert len(result) == 0

    def test_strategy_name_correct(self) -> None:
        df = make_price_ohlcv(_UPTREND, start=_START)
        result = compute_cross_sect_momentum_holdings(_ohlcv(df), _BASE_PARAMS)
        for holdings in result.values():
            assert (holdings["strategy"] == "cross_sect_momentum").all()


# ---------------------------------------------------------------------------
# OBV Momentum
# ---------------------------------------------------------------------------


class TestComputeOBVHoldings:
    def test_rising_obv_produces_holdings(self) -> None:
        # Uptrend: every day close > prev close → OBV accumulates positively
        # OBV grows monotonically → last value elevated above rolling mean
        df = make_price_ohlcv(_UPTREND, start=_START)
        result = compute_obv_holdings(_ohlcv(df), _BASE_PARAMS)
        assert len(result) > 0
        for holdings in result.values():
            assert abs(holdings["weight"].sum() - 1.0) < 0.01

    def test_falling_obv_no_holdings(self) -> None:
        # Downtrend: every day close < prev close → OBV negative and decreasing
        df = make_price_ohlcv(_DOWNTREND, start=_START)
        result = compute_obv_holdings(_ohlcv(df), _BASE_PARAMS)
        assert len(result) == 0

    def test_insufficient_data_skips(self) -> None:
        df = make_price_ohlcv([100.0 + i for i in range(10)], start=_START)
        result = compute_obv_holdings(_ohlcv(df), _BASE_PARAMS)
        assert len(result) == 0

    def test_strategy_name_correct(self) -> None:
        df = make_price_ohlcv(_UPTREND, start=_START)
        result = compute_obv_holdings(_ohlcv(df), _BASE_PARAMS)
        for holdings in result.values():
            assert (holdings["strategy"] == "obv_momentum").all()


# ---------------------------------------------------------------------------
# ADX Trend Strength
# ---------------------------------------------------------------------------


class TestComputeADXHoldings:
    def test_strong_uptrend_produces_positive_score(self) -> None:
        # Steady 0.5-per-day rise → DI+ > DI-, ADX converges above 25
        df = make_price_ohlcv(_UPTREND, start=_START)
        result = compute_adx_holdings(_ohlcv(df), _BASE_PARAMS)
        assert len(result) > 0
        for holdings in result.values():
            assert abs(holdings["weight"].sum() - 1.0) < 0.01

    def test_strong_downtrend_no_holdings(self) -> None:
        # Steady downtrend → DI- > DI+ → negative score → filtered out
        df = make_price_ohlcv(_DOWNTREND, start=_START)
        result = compute_adx_holdings(_ohlcv(df), _BASE_PARAMS)
        assert len(result) == 0

    def test_insufficient_data_skips(self) -> None:
        df = make_price_ohlcv([100.0 + i * 0.1 for i in range(10)], start=_START)
        result = compute_adx_holdings(_ohlcv(df), _BASE_PARAMS)
        assert len(result) == 0

    def test_strategy_name_correct(self) -> None:
        df = make_price_ohlcv(_UPTREND, start=_START)
        result = compute_adx_holdings(_ohlcv(df), _BASE_PARAMS)
        for holdings in result.values():
            assert (holdings["strategy"] == "adx_trend").all()

    def test_adx_threshold_filters_weak_trends(self) -> None:
        # Flat prices → no directional movement → ADX stays near 0 → no holdings
        flat = [100.0] * _N
        df = make_price_ohlcv(flat, start=_START)
        result = compute_adx_holdings(_ohlcv(df), _BASE_PARAMS)
        assert len(result) == 0


# ---------------------------------------------------------------------------
# Multi-ticker: top_pct slices correctly
# ---------------------------------------------------------------------------


class TestTopPctSlicing:
    def test_only_top_fraction_held_across_strategies(self) -> None:
        # 5 tickers: 3 uptrend, 2 downtrend; top_pct=0.6 → 3 candidates → 3 positive
        params = {**_BASE_PARAMS, "top_pct": 0.6}
        ohlcv: dict[str, Callable[[], pd.DataFrame]] = {
            "aapl": lambda: make_price_ohlcv(_UPTREND, "AAPL", _START),
            "msft": lambda: make_price_ohlcv(_UPTREND, "MSFT", _START),
            "nvda": lambda: make_price_ohlcv(_UPTREND, "NVDA", _START),
            "bad1": lambda: make_price_ohlcv(_DOWNTREND, "BAD1", _START),
            "bad2": lambda: make_price_ohlcv(_DOWNTREND, "BAD2", _START),
        }
        result = compute_donchian_holdings(ohlcv, params)
        assert len(result) > 0
        for holdings in result.values():
            # All selected tickers should be from the uptrend group
            assert set(holdings["ticker"]).issubset({"AAPL", "MSFT", "NVDA"})
            assert abs(holdings["weight"].sum() - 1.0) < 0.01
