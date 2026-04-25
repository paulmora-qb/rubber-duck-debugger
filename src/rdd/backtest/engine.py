"""Vectorised long-only backtesting engine."""

from __future__ import annotations

import logging
from typing import Literal

import pandas as pd

from rdd.backtest.result import BacktestResult
from rdd.schemas.signals import SignalSchema

logger = logging.getLogger(__name__)

_TRADE_COLS = ["date", "ticker", "shares", "price", "notional", "cost", "direction"]


class Backtester:
    """Simulates a long-only portfolio against historical OHLCV data.

    Signals on day T execute at day T+1 open (no lookahead bias).
    Transaction costs are charged on traded notional only.
    """

    def __init__(
        self,
        initial_capital: float = 100_000,
        cost_bps: float = 10,
        execution: Literal["next_open"] = "next_open",
    ) -> None:
        self.initial_capital = initial_capital
        self.cost_bps = cost_bps
        self.execution = execution

    def run(self, signals: pd.DataFrame, ohlcv: pd.DataFrame) -> BacktestResult:
        """Run the backtest.

        Args:
            signals: Strategy output validated against SignalSchema.
                     One row per active position per rebalance date.
            ohlcv: Long-format OHLCV DataFrame with columns
                   [date, ticker, open, close]. All tickers, all trading days.

        Returns:
            BacktestResult with equity curve, returns, positions, and trade log.
        """
        SignalSchema.validate(signals)

        signals = signals.copy()
        signals["date"] = pd.to_datetime(signals["date"])
        ohlcv = ohlcv.copy()
        ohlcv["date"] = pd.to_datetime(ohlcv["date"])

        open_prices = ohlcv.pivot(index="date", columns="ticker", values="open")
        close_prices = ohlcv.pivot(index="date", columns="ticker", values="close")

        trading_days = close_prices.index.sort_values()
        rebalance_dates = signals["date"].sort_values().unique()

        cash = float(self.initial_capital)
        holdings: dict[str, float] = {}  # ticker → shares

        equity_records: list[tuple] = []
        position_records: list[dict] = []
        trade_records: list[dict] = []

        pending_signals: pd.DataFrame | None = None

        for day in trading_days:
            # Execute trades from previous rebalance date at today's open
            if pending_signals is not None:
                cash, new_trades = self._execute(
                    pending_signals, holdings, open_prices, day, cash
                )
                trade_records.extend(new_trades)
                pending_signals = None

            # Queue up today's signals to execute tomorrow
            if day in rebalance_dates:
                pending_signals = signals[signals["date"] == day]

            # Mark-to-market at close
            portfolio_value = cash + sum(
                holdings.get(t, 0.0) * close_prices.at[day, t]
                for t in holdings
                if t in close_prices.columns and not pd.isna(close_prices.at[day, t])
            )
            equity_records.append((day, portfolio_value))
            position_records.append({"date": day, **{t: s for t, s in holdings.items()}})

        equity_curve = pd.Series(
            dict(equity_records), name="portfolio_value", dtype=float
        )
        equity_curve.index = pd.to_datetime(equity_curve.index)

        returns = equity_curve.pct_change()

        positions = pd.DataFrame(position_records).set_index("date").fillna(0.0)
        positions.index = pd.to_datetime(positions.index)

        trades = (
            pd.DataFrame(trade_records, columns=_TRADE_COLS)
            if trade_records
            else pd.DataFrame(columns=_TRADE_COLS)
        )

        return BacktestResult(
            equity_curve=equity_curve,
            returns=returns,
            positions=positions,
            trades=trades,
        )

    def _execute(
        self,
        signals: pd.DataFrame,
        holdings: dict[str, float],
        open_prices: pd.DataFrame,
        execution_day: pd.Timestamp,
        cash: float,
    ) -> tuple[float, list[dict]]:
        """Execute a rebalance at execution_day's open prices."""
        if execution_day not in open_prices.index:
            logger.warning("No open prices for %s — skipping rebalance.", execution_day.date())
            return cash, []

        day_opens = open_prices.loc[execution_day]

        # Filter out tickers with no open price data
        valid_signals = signals[signals["ticker"].isin(day_opens.dropna().index)].copy()
        skipped = set(signals["ticker"]) - set(valid_signals["ticker"])
        for t in skipped:
            logger.warning("Ticker %s has no open price on %s — skipping.", t, execution_day.date())

        # Renormalise weights for valid tickers so they still sum to 1
        if valid_signals.empty:
            return cash, []

        valid_signals = valid_signals.copy()
        valid_signals["weight"] = valid_signals["weight"] / valid_signals["weight"].sum()

        portfolio_value = cash + sum(
            holdings.get(t, 0.0) * day_opens[t]
            for t in holdings
            if t in day_opens.index and not pd.isna(day_opens[t])
        )

        target_shares: dict[str, float] = {
            row["ticker"]: (row["weight"] * portfolio_value) / day_opens[row["ticker"]]
            for _, row in valid_signals.iterrows()
        }

        # Tickers in current portfolio but not in new signals → sell to zero
        all_tickers = set(holdings) | set(target_shares)
        trades = []

        for ticker in all_tickers:
            current = holdings.get(ticker, 0.0)
            target = target_shares.get(ticker, 0.0)
            delta = target - current

            if abs(delta) < 1e-6:
                continue

            price = float(day_opens[ticker])
            notional = abs(delta * price)
            cost = notional * self.cost_bps / 10_000
            cash -= delta * price + cost
            holdings[ticker] = target

            trades.append({
                "date": execution_day,
                "ticker": ticker,
                "shares": delta,
                "price": price,
                "notional": notional,
                "cost": cost,
                "direction": "buy" if delta > 0 else "sell",
            })

        # Remove fully exited positions
        for ticker in list(holdings):
            if abs(holdings[ticker]) < 1e-6:
                del holdings[ticker]

        return cash, trades
