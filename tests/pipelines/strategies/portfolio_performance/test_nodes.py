"""Unit tests for portfolio_performance nodes."""

from __future__ import annotations

import math
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from rdd.pipelines.strategies.portfolio_performance.nodes import (
    _build_html,
    compile_report,
    compute_performance_metrics,
    compute_strategy_returns,
    send_performance_email,
)


# ── fixtures ──────────────────────────────────────────────────────────────────


def _make_holdings(
    strategy: str = "s1",
    date: str = "2024-01-01",
    tickers: list[str] | None = None,
    weights: list[float] | None = None,
) -> pd.DataFrame:
    tickers = tickers or ["AAPL", "MSFT"]
    weights = weights or [0.6, 0.4]
    return pd.DataFrame(
        {
            "strategy": strategy,
            "date": pd.Timestamp(date),
            "ticker": tickers,
            "weight": weights,
        }
    )


def _make_ohlcv(
    tickers: list[str] | None = None,
    start: str = "2024-01-01",
    periods: int = 10,
) -> pd.DataFrame:
    tickers = tickers or ["AAPL", "MSFT"]
    dates = pd.date_range(start, periods=periods)
    rows = []
    for t in tickers:
        price = 100.0
        for d in dates:
            rows.append(
                {
                    "ticker": t,
                    "date": d,
                    "adj_close": price,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "volume": 1_000_000.0,
                }
            )
            price *= 1.01
    return pd.DataFrame(rows)


# ── compute_strategy_returns ──────────────────────────────────────────────────


_WIDE_PARAMS = {"lookback_months": 60}  # wide window so fixture dates are always included


class TestComputeStrategyReturns:
    def test_returns_daily_series(self) -> None:
        holdings_df = _make_holdings()
        ohlcv_df = _make_ohlcv(periods=10)
        result = compute_strategy_returns(
            holdings_existing={"2024-01-01": lambda: holdings_df},
            ohlcv_existing={"aapl": lambda: ohlcv_df[ohlcv_df["ticker"] == "AAPL"],
                            "msft": lambda: ohlcv_df[ohlcv_df["ticker"] == "MSFT"]},
            params=_WIDE_PARAMS,
        )
        assert "date" in result.columns
        assert "portfolio_return" in result.columns
        assert len(result) > 0

    def test_empty_holdings_returns_empty(self) -> None:
        result = compute_strategy_returns(
            holdings_existing={},
            ohlcv_existing={"aapl": lambda: _make_ohlcv(["AAPL"])},
            params=_WIDE_PARAMS,
        )
        assert result.empty

    def test_empty_ohlcv_returns_empty(self) -> None:
        holdings_df = _make_holdings()
        result = compute_strategy_returns(
            holdings_existing={"2024-01-01": lambda: holdings_df},
            ohlcv_existing={},
            params=_WIDE_PARAMS,
        )
        assert result.empty

    def test_portfolio_return_is_weighted_average(self) -> None:
        """With equal 1% daily gain on both tickers, portfolio return ~= 1%."""
        holdings_df = _make_holdings(weights=[0.5, 0.5])
        ohlcv_df = _make_ohlcv(periods=5)
        result = compute_strategy_returns(
            holdings_existing={"2024-01-01": lambda: holdings_df},
            ohlcv_existing={
                "aapl": lambda: ohlcv_df[ohlcv_df["ticker"] == "AAPL"],
                "msft": lambda: ohlcv_df[ohlcv_df["ticker"] == "MSFT"],
            },
            params=_WIDE_PARAMS,
        )
        non_nan = result["portfolio_return"].dropna()
        assert non_nan.notna().all()
        assert (non_nan.abs() < 0.05).all()

    def test_lookback_filters_old_data(self) -> None:
        """Returns outside the lookback window are excluded."""
        today = pd.Timestamp.now().normalize()
        recent_date = (today - pd.DateOffset(months=1)).strftime("%Y-%m-%d")
        holdings_df = _make_holdings(date=recent_date)
        ohlcv_df = _make_ohlcv(start=recent_date, periods=5)
        result = compute_strategy_returns(
            holdings_existing={recent_date: lambda: holdings_df},
            ohlcv_existing={
                "aapl": lambda: ohlcv_df[ohlcv_df["ticker"] == "AAPL"],
                "msft": lambda: ohlcv_df[ohlcv_df["ticker"] == "MSFT"],
            },
            params={"lookback_months": 3},
        )
        assert len(result) > 0
        assert result["date"].min() >= today - pd.DateOffset(months=3)


# ── compute_performance_metrics ───────────────────────────────────────────────


class TestComputePerformanceMetrics:
    def _flat_returns(self, n: int = 252, daily: float = 0.001) -> pd.DataFrame:
        return pd.DataFrame({"date": pd.date_range("2024-01-01", periods=n), "portfolio_return": daily})

    def test_output_has_required_columns(self) -> None:
        result = compute_performance_metrics(self._flat_returns())
        for col in ["cumulative_return", "annualised_return", "annualised_volatility", "sharpe_ratio", "max_drawdown", "observation_days"]:
            assert col in result.columns

    def test_cumulative_return_positive_for_gains(self) -> None:
        result = compute_performance_metrics(self._flat_returns(daily=0.001))
        assert result.iloc[0]["cumulative_return"] > 0

    def test_max_drawdown_non_positive(self) -> None:
        result = compute_performance_metrics(self._flat_returns())
        assert result.iloc[0]["max_drawdown"] <= 0

    def test_empty_returns_gives_nan(self) -> None:
        result = compute_performance_metrics(pd.DataFrame(columns=["date", "portfolio_return"]))
        assert math.isnan(result.iloc[0]["cumulative_return"])

    def test_observation_days_correct(self) -> None:
        result = compute_performance_metrics(self._flat_returns(n=100))
        assert result.iloc[0]["observation_days"] == 100


# ── compile_report ────────────────────────────────────────────────────────────


class TestCompileReport:
    def _metrics(self) -> pd.DataFrame:
        return pd.DataFrame([{
            "cumulative_return": 0.12,
            "annualised_return": 0.15,
            "annualised_volatility": 0.10,
            "sharpe_ratio": 1.5,
            "max_drawdown": -0.05,
            "observation_days": 252,
        }])

    def test_one_strategy(self) -> None:
        result = compile_report(strategy_a=self._metrics())
        assert "strategy" in result.columns
        assert result.iloc[0]["strategy"] == "strategy_a"

    def test_multiple_strategies(self) -> None:
        result = compile_report(s1=self._metrics(), s2=self._metrics())
        assert len(result) == 2
        assert set(result["strategy"]) == {"s1", "s2"}


# ── send_performance_email ────────────────────────────────────────────────────


class TestSendPerformanceEmail:
    def _report(self) -> pd.DataFrame:
        return pd.DataFrame([{
            "strategy": "portfolio_construction",
            "cumulative_return": 0.08,
            "annualised_return": 0.10,
            "annualised_volatility": 0.12,
            "sharpe_ratio": 0.83,
            "max_drawdown": -0.04,
            "observation_days": 30,
        }])

    def test_skips_send_with_missing_config(self) -> None:
        """No SMTP credentials → logs warning, does not raise."""
        send_performance_email(self._report(), params={})

    def test_sends_with_valid_config(self) -> None:
        params = {
            "email_to": "test@example.com",
            "smtp_host": "smtp.example.com",
            "smtp_port": 465,
            "smtp_user": "user@example.com",
            "smtp_pass": "secret",
        }
        mock_server = MagicMock()
        with patch("smtplib.SMTP_SSL", return_value=mock_server.__enter__.return_value) as mock_ssl:
            mock_ssl.return_value.__enter__ = lambda s: mock_server
            mock_ssl.return_value.__exit__ = MagicMock(return_value=False)
            send_performance_email(self._report(), params=params)

    def test_html_contains_strategy_name(self) -> None:
        html = _build_html(self._report(), chart_b64="", holdings_html="")
        # _fmt_strategy_name converts "portfolio_construction" → "Portfolio Construction"
        assert "Portfolio Construction" in html
        assert "<table" in html
