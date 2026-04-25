"""Unit tests for the Backtester engine."""

from __future__ import annotations

import logging

import pandas as pd
import pandera.errors
import pytest

from rdd.backtest.engine import Backtester


def _make_ohlcv(
    tickers: list[str],
    dates: pd.DatetimeIndex,
    price: float = 100.0,
) -> pd.DataFrame:
    """Flat OHLCV fixture: all prices equal to `price` for easy math."""
    rows = []
    for ticker in tickers:
        for date in dates:
            rows.append({
                "date": date,
                "ticker": ticker,
                "open": price,
                "high": price * 1.01,
                "low": price * 0.99,
                "close": price,
                "adj_close": price,
                "volume": 1_000_000.0,
            })
    return pd.DataFrame(rows)


def _make_signals(
    date: str,
    tickers: list[str],
    equal_weight: bool = True,
    weights: list[float] | None = None,
) -> pd.DataFrame:
    n = len(tickers)
    w = weights if weights is not None else [1 / n] * n
    return pd.DataFrame({
        "date": pd.to_datetime([date] * n),
        "ticker": tickers,
        "position": [1] * n,
        "weight": w,
    })


@pytest.fixture
def tickers() -> list[str]:
    return ["AAPL", "MSFT", "GOOG"]


@pytest.fixture
def trading_days() -> pd.DatetimeIndex:
    return pd.date_range("2024-01-02", periods=20, freq="B")


@pytest.fixture
def flat_ohlcv(tickers, trading_days) -> pd.DataFrame:
    return _make_ohlcv(tickers, trading_days, price=100.0)


@pytest.fixture
def single_rebalance_signals(tickers) -> pd.DataFrame:
    return _make_signals("2024-01-02", tickers)


class TestBacktesterEquityCurve:
    def test_equity_curve_length_matches_trading_days(
        self, flat_ohlcv, single_rebalance_signals, trading_days
    ) -> None:
        result = Backtester().run(single_rebalance_signals, flat_ohlcv)
        assert len(result.equity_curve) == len(trading_days)

    def test_initial_value_equals_capital(self, flat_ohlcv, single_rebalance_signals) -> None:
        bt = Backtester(initial_capital=100_000)
        result = bt.run(single_rebalance_signals, flat_ohlcv)
        # First day: no trades yet (signal executes next day), portfolio = cash = initial_capital
        assert result.equity_curve.iloc[0] == pytest.approx(100_000.0, rel=1e-6)

    def test_flat_prices_equity_decreases_only_by_costs(
        self, flat_ohlcv, single_rebalance_signals
    ) -> None:
        bt = Backtester(initial_capital=100_000, cost_bps=10)
        result = bt.run(single_rebalance_signals, flat_ohlcv)
        # With flat prices, portfolio value can only decrease due to transaction costs
        assert result.equity_curve.iloc[-1] <= result.equity_curve.iloc[0]


class TestBacktesterTransactionCosts:
    def test_known_cost_single_ticker(self, trading_days) -> None:
        """Buy $100k of 1 ticker at $100/share with 10bps → cost = $100."""
        tickers = ["AAPL"]
        ohlcv = _make_ohlcv(tickers, trading_days, price=100.0)
        signals = _make_signals("2024-01-02", tickers)

        bt = Backtester(initial_capital=100_000, cost_bps=10)
        result = bt.run(signals, ohlcv)

        total_cost = result.trades["cost"].sum()
        # Buy 1000 shares at $100 → notional = $100k → cost = $100k × 10/10000 = $100
        assert total_cost == pytest.approx(100.0, rel=1e-4)

    def test_zero_cost_bps_no_cost(self, flat_ohlcv, single_rebalance_signals) -> None:
        bt = Backtester(cost_bps=0)
        result = bt.run(single_rebalance_signals, flat_ohlcv)
        assert result.trades["cost"].sum() == pytest.approx(0.0)


class TestBacktesterValidation:
    def test_weights_not_summing_to_one_raises(self, flat_ohlcv, tickers) -> None:
        bad_signals = _make_signals("2024-01-02", tickers, weights=[0.4, 0.4, 0.4])
        with pytest.raises(pandera.errors.SchemaError):
            Backtester().run(bad_signals, flat_ohlcv)


class TestBacktesterMissingTicker:
    def test_missing_ticker_skipped_with_warning(
        self, flat_ohlcv, trading_days, caplog
    ) -> None:
        # Signal includes ZZZZ which has no OHLCV data
        signals = pd.DataFrame({
            "date": pd.to_datetime(["2024-01-02", "2024-01-02"]),
            "ticker": ["AAPL", "ZZZZ"],
            "position": [1, 1],
            "weight": [0.5, 0.5],
        })

        with caplog.at_level(logging.WARNING, logger="rdd.backtest.engine"):
            result = Backtester().run(signals, flat_ohlcv)

        assert "ZZZZ" in caplog.text
        assert not result.equity_curve.empty


class TestBacktesterMultipleRebalances:
    def test_three_rebalances_trade_log_has_entries(
        self, flat_ohlcv, tickers, trading_days
    ) -> None:
        signals = pd.concat([
            _make_signals("2024-01-02", tickers),
            _make_signals("2024-01-09", tickers[:2], weights=[0.5, 0.5]),
            _make_signals("2024-01-16", tickers),
        ], ignore_index=True)

        result = Backtester().run(signals, flat_ohlcv)

        # Should have trades from all three rebalances
        assert len(result.trades) > 3

    def test_positions_index_matches_equity_curve(
        self, flat_ohlcv, single_rebalance_signals
    ) -> None:
        result = Backtester().run(single_rebalance_signals, flat_ohlcv)
        assert list(result.positions.index) == list(result.equity_curve.index)
