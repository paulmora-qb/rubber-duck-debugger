"""Unit tests for strategies pipeline nodes.

Each node is tested in isolation with synthetic price series designed to
produce unambiguous signal directions, so test intent is self-evident from
the data rather than from magic expected values.
"""

from __future__ import annotations

from collections.abc import Callable

import numpy as np
import pandas as pd
import pytest

from rdd.pipelines.strategies.models import StrategySignal
from rdd.pipelines.strategies.nodes import (
    assemble_stock_analyses,
    compute_mean_reversion_signals,
    compute_momentum_signals,
    compute_trend_signals,
    compute_volatility_signals,
)
from tests.conftest import make_price_ohlcv

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_UPTREND = [100.0 + i * 0.5 for i in range(260)]  # 260 days, steadily rising
_DOWNTREND = [230.0 - i * 0.5 for i in range(260)]  # 260 days, steadily falling


def _ohlcv(
    df: pd.DataFrame, key: str = "aapl"
) -> dict[str, Callable[[], pd.DataFrame]]:
    return {key: lambda _df=df: _df}


@pytest.fixture
def params() -> dict:
    return {
        "momentum_windows": {"1m": 21, "3m": 63, "6m": 126, "12m": 252},
        "momentum_min_bullish": 3,
        "trend_short_window": 50,
        "trend_long_window": 200,
        "rsi_window": 14,
        "rsi_oversold": 30.0,
        "rsi_overbought": 70.0,
        "bb_window": 20,
        "bb_std": 2.0,
    }


# ---------------------------------------------------------------------------
# compute_momentum_signals
# ---------------------------------------------------------------------------


class TestComputeMomentumSignals:
    def test_uptrend_is_bullish(self, params) -> None:
        result = compute_momentum_signals(_ohlcv(make_price_ohlcv(_UPTREND)), params)

        assert result["aapl"].direction == "bullish"

    def test_downtrend_is_bearish(self, params) -> None:
        result = compute_momentum_signals(_ohlcv(make_price_ohlcv(_DOWNTREND)), params)

        assert result["aapl"].direction == "bearish"

    def test_metrics_contain_all_four_windows(self, params) -> None:
        result = compute_momentum_signals(_ohlcv(make_price_ohlcv(_UPTREND)), params)

        assert set(result["aapl"].metrics.keys()) == {
            "return_1m",
            "return_3m",
            "return_6m",
            "return_12m",
        }

    def test_short_series_only_emits_available_windows(self, params) -> None:
        # 30 rows → only 1m (21d) window fits; 3m/6m/12m are skipped
        prices = [100.0 + i * 0.5 for i in range(30)]
        result = compute_momentum_signals(_ohlcv(make_price_ohlcv(prices)), params)

        assert "return_1m" in result["aapl"].metrics
        assert "return_3m" not in result["aapl"].metrics

    def test_single_row_ticker_is_skipped(self, params) -> None:
        result = compute_momentum_signals(_ohlcv(make_price_ohlcv([100.0])), params)

        assert "aapl" not in result

    def test_multiple_tickers_are_independent(self, params) -> None:
        ohlcv = {
            "aapl": lambda: make_price_ohlcv(_UPTREND, ticker="AAPL"),
            "msft": lambda: make_price_ohlcv(_DOWNTREND, ticker="MSFT"),
        }
        result = compute_momentum_signals(ohlcv, params)

        assert result["aapl"].direction == "bullish"
        assert result["msft"].direction == "bearish"


# ---------------------------------------------------------------------------
# compute_trend_signals
# ---------------------------------------------------------------------------


class TestComputeTrendSignals:
    def test_golden_cross_is_bullish(self, params) -> None:
        # Uptrend → MA50 > MA200 → golden cross
        result = compute_trend_signals(_ohlcv(make_price_ohlcv(_UPTREND)), params)

        assert result["aapl"].direction == "bullish"
        assert result["aapl"].metrics.get("cross") == "golden"

    def test_death_cross_is_bearish(self, params) -> None:
        result = compute_trend_signals(_ohlcv(make_price_ohlcv(_DOWNTREND)), params)

        assert result["aapl"].direction == "bearish"
        assert result["aapl"].metrics.get("cross") == "death"

    def test_short_series_uses_price_vs_ma50(self, params) -> None:
        # 60 rows: MA50 available, MA200 not — direction follows price vs MA50
        prices = [100.0 + i * 0.5 for i in range(60)]  # rising → price > MA50
        result = compute_trend_signals(_ohlcv(make_price_ohlcv(prices)), params)

        assert result["aapl"].direction == "bullish"
        assert "cross" not in result["aapl"].metrics
        assert "ma200" not in result["aapl"].metrics

    def test_insufficient_data_ticker_is_skipped(self, params) -> None:
        prices = [100.0 + i for i in range(10)]
        result = compute_trend_signals(_ohlcv(make_price_ohlcv(prices)), params)

        assert "aapl" not in result

    def test_metrics_contain_ma_values(self, params) -> None:
        result = compute_trend_signals(_ohlcv(make_price_ohlcv(_UPTREND)), params)
        metrics = result["aapl"].metrics

        assert "ma50" in metrics
        assert "ma200" in metrics
        assert "price_vs_ma_short_pct" in metrics


# ---------------------------------------------------------------------------
# compute_mean_reversion_signals
# ---------------------------------------------------------------------------


class TestComputeMeanReversionSignals:
    def test_declining_prices_are_oversold_bullish(self, params) -> None:
        # Pure decline → RSI ≈ 0 (< 30) → bullish
        prices = [200.0 - i * 2.0 for i in range(30)]
        result = compute_mean_reversion_signals(
            _ohlcv(make_price_ohlcv(prices)), params
        )

        assert result["aapl"].direction == "bullish"
        assert result["aapl"].metrics[f"rsi_{params['rsi_window']}"] < 30

    def test_rising_prices_are_overbought_bearish(self, params) -> None:
        # Pure rise → RSI ≈ 100 (> 70) → bearish
        prices = [100.0 + i * 2.0 for i in range(30)]
        result = compute_mean_reversion_signals(
            _ohlcv(make_price_ohlcv(prices)), params
        )

        assert result["aapl"].direction == "bearish"
        assert result["aapl"].metrics[f"rsi_{params['rsi_window']}"] > 70

    def test_alternating_prices_are_neutral(self, params) -> None:
        # Equal up/down moves → RSI ≈ 50 → neutral
        prices = [
            100.0 + (1.0 if i % 2 == 0 else -1.0) * (i // 2 + 1) for i in range(30)
        ]
        # Simpler: fixed alternating deltas from a constant base
        base = 100.0
        prices = []
        for i in range(30):
            base += 1.0 if i % 2 == 0 else -1.0
            prices.append(base)
        result = compute_mean_reversion_signals(
            _ohlcv(make_price_ohlcv(prices)), params
        )

        assert result["aapl"].direction == "neutral"

    def test_insufficient_data_ticker_is_skipped(self, params) -> None:
        prices = [100.0 + i for i in range(5)]
        result = compute_mean_reversion_signals(
            _ohlcv(make_price_ohlcv(prices)), params
        )

        assert "aapl" not in result

    def test_metrics_contain_bb_and_rsi(self, params) -> None:
        prices = [200.0 - i * 2.0 for i in range(30)]
        result = compute_mean_reversion_signals(
            _ohlcv(make_price_ohlcv(prices)), params
        )
        metrics = result["aapl"].metrics

        assert f"rsi_{params['rsi_window']}" in metrics
        assert "bb_position" in metrics
        assert "bb_upper" in metrics
        assert "bb_lower" in metrics

    def test_bb_position_within_zero_one(self, params) -> None:
        prices = [100.0 + i * 0.1 for i in range(30)]
        result = compute_mean_reversion_signals(
            _ohlcv(make_price_ohlcv(prices)), params
        )

        bb_pos = result["aapl"].metrics["bb_position"]
        assert 0.0 <= bb_pos <= 1.0


# ---------------------------------------------------------------------------
# compute_volatility_signals
# ---------------------------------------------------------------------------

# 300 days of realistic-looking prices for GARCH to fit — fixed seed for determinism
_rng = np.random.default_rng(42)
_GARCH_PRICES = list(100.0 * np.exp(np.cumsum(_rng.normal(0, 0.01, 300))))


class TestComputeVolatilitySignals:
    @pytest.fixture
    def params(self) -> dict:
        return {
            "garch_min_obs": 252,
            "garch_vol_ratio_high": 1.5,
            "garch_vol_ratio_low": 0.75,
        }

    def test_returns_signal_for_sufficient_data(self, params) -> None:
        result = compute_volatility_signals(
            _ohlcv(make_price_ohlcv(_GARCH_PRICES)), params
        )

        assert "aapl" in result
        assert isinstance(result["aapl"], StrategySignal)
        assert result["aapl"].strategy == "volatility"

    def test_direction_is_valid(self, params) -> None:
        result = compute_volatility_signals(
            _ohlcv(make_price_ohlcv(_GARCH_PRICES)), params
        )

        assert result["aapl"].direction in {"bullish", "bearish", "neutral"}

    def test_metrics_contain_required_keys(self, params) -> None:
        result = compute_volatility_signals(
            _ohlcv(make_price_ohlcv(_GARCH_PRICES)), params
        )
        metrics = result["aapl"].metrics

        assert "current_vol_ann" in metrics
        assert "long_run_vol_ann" in metrics
        assert "vol_ratio" in metrics
        assert "persistence" in metrics

    def test_persistence_between_zero_and_one(self, params) -> None:
        result = compute_volatility_signals(
            _ohlcv(make_price_ohlcv(_GARCH_PRICES)), params
        )

        persistence = result["aapl"].metrics["persistence"]
        assert 0.0 <= persistence < 1.0

    def test_vol_ratio_positive(self, params) -> None:
        result = compute_volatility_signals(
            _ohlcv(make_price_ohlcv(_GARCH_PRICES)), params
        )

        assert result["aapl"].metrics["vol_ratio"] > 0

    def test_skips_ticker_with_insufficient_data(self, params) -> None:
        short_prices = [100.0 + i * 0.1 for i in range(100)]
        result = compute_volatility_signals(
            _ohlcv(make_price_ohlcv(short_prices)), params
        )

        assert "aapl" not in result


# ---------------------------------------------------------------------------
# assemble_stock_analyses
# ---------------------------------------------------------------------------


def _make_signal(strategy: str) -> StrategySignal:
    return StrategySignal(strategy=strategy, direction="bullish", metrics={})


class TestAssembleStockAnalyses:
    def test_output_contains_all_tickers(self) -> None:
        momentum = {"aapl": _make_signal("momentum"), "msft": _make_signal("momentum")}
        trend = {"aapl": _make_signal("trend"), "msft": _make_signal("trend")}
        mean_rev = {"aapl": _make_signal("mean_reversion")}

        result = assemble_stock_analyses(momentum, trend, mean_rev, {})

        assert "aapl" in result
        assert "msft" in result

    def test_ticker_is_uppercased_in_output(self) -> None:
        signal = {"aapl": _make_signal("momentum")}
        result = assemble_stock_analyses(signal, {}, {}, {})

        assert result["aapl"]["ticker"] == "AAPL"

    def test_all_four_strategy_signals_present(self) -> None:
        result = assemble_stock_analyses(
            {"aapl": _make_signal("momentum")},
            {"aapl": _make_signal("trend")},
            {"aapl": _make_signal("mean_reversion")},
            {"aapl": _make_signal("volatility")},
        )
        strategies = {s["strategy"] for s in result["aapl"]["signals"]}

        assert strategies == {"momentum", "trend", "mean_reversion", "volatility"}

    def test_missing_strategy_excluded_from_signals(self) -> None:
        # volatility skipped ticker (e.g. insufficient data for GARCH)
        result = assemble_stock_analyses(
            {"aapl": _make_signal("momentum")},
            {"aapl": _make_signal("trend")},
            {},
            {},
        )

        strategies = {s["strategy"] for s in result["aapl"]["signals"]}
        assert "mean_reversion" not in strategies
        assert "volatility" not in strategies
        assert len(result["aapl"]["signals"]) == 2

    def test_output_has_required_top_level_keys(self) -> None:
        result = assemble_stock_analyses({"aapl": _make_signal("momentum")}, {}, {}, {})
        entry = result["aapl"]

        assert {"ticker", "generated_at", "signals"} <= entry.keys()

    def test_tickers_are_sorted_in_output(self) -> None:
        signals = {
            "msft": _make_signal("momentum"),
            "aapl": _make_signal("momentum"),
            "goog": _make_signal("momentum"),
        }
        result = assemble_stock_analyses(signals, {}, {}, {})

        assert list(result.keys()) == sorted(result.keys())
