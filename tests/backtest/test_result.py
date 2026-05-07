"""Tests for BacktestResult.summary()."""

from __future__ import annotations

import pandas as pd
import pytest

from rdd.backtest.result import BacktestResult


def _flat_result(n: int = 10, value: float = 100_000.0) -> BacktestResult:
    """BacktestResult with a perfectly flat equity curve."""
    idx = pd.date_range("2024-01-02", periods=n, freq="B")
    equity = pd.Series([value] * n, index=idx, name="portfolio_value")
    returns = equity.pct_change()
    positions = pd.DataFrame(index=idx)
    trades = pd.DataFrame(columns=["date", "ticker", "shares", "price", "notional", "cost", "direction"])
    return BacktestResult(equity_curve=equity, returns=returns, positions=positions, trades=trades)


class TestBacktestResultSummary:
    def test_summary_has_expected_keys(self) -> None:
        result = _flat_result()
        summary = result.summary()
        expected = {"total_return", "annualised_return", "max_drawdown", "sharpe_ratio", "n_trades", "total_cost"}
        assert expected == set(summary.keys())

    def test_flat_curve_total_return_is_zero(self) -> None:
        summary = _flat_result().summary()
        assert summary["total_return"] == pytest.approx(0.0, abs=1e-9)

    def test_flat_curve_max_drawdown_is_zero(self) -> None:
        summary = _flat_result().summary()
        assert summary["max_drawdown"] == pytest.approx(0.0, abs=1e-9)

    def test_flat_curve_sharpe_is_zero(self) -> None:
        summary = _flat_result().summary()
        assert summary["sharpe_ratio"] == pytest.approx(0.0, abs=1e-9)

    def test_no_trades_total_cost_is_zero(self) -> None:
        summary = _flat_result().summary()
        assert summary["total_cost"] == pytest.approx(0.0)
        assert summary["n_trades"] == 0

    def test_growing_curve_positive_total_return(self) -> None:
        idx = pd.date_range("2024-01-02", periods=5, freq="B")
        equity = pd.Series([100_000, 101_000, 102_000, 103_000, 104_000], index=idx, dtype=float)
        result = BacktestResult(
            equity_curve=equity,
            returns=equity.pct_change(),
            positions=pd.DataFrame(index=idx),
            trades=pd.DataFrame(columns=["date", "ticker", "shares", "price", "notional", "cost", "direction"]),
        )
        assert result.summary()["total_return"] == pytest.approx(0.04, rel=1e-6)
