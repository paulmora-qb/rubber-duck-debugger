"""Backtest result container."""

from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd


@dataclass
class BacktestResult:
    """Holds all outputs from a single backtest run."""

    equity_curve: pd.Series
    returns: pd.Series
    positions: pd.DataFrame
    trades: pd.DataFrame

    def summary(self) -> dict:
        """Return a flat dict of scalar performance statistics."""
        r = self.returns.dropna()
        ev = self.equity_curve

        total_return = (ev.iloc[-1] / ev.iloc[0]) - 1 if len(ev) > 1 else 0.0

        n_years = len(r) / 252
        annualised_return = (
            (1 + total_return) ** (1 / n_years) - 1 if n_years > 0 else 0.0
        )

        rolling_max = ev.cummax()
        drawdown = (ev - rolling_max) / rolling_max
        max_drawdown = float(drawdown.min())

        sharpe_ratio = (
            float(r.mean() / r.std() * math.sqrt(252))
            if r.std() > 0
            else 0.0
        )

        n_trades = len(self.trades)
        total_cost = float(self.trades["cost"].sum()) if n_trades > 0 else 0.0

        return {
            "total_return": float(total_return),
            "annualised_return": float(annualised_return),
            "max_drawdown": float(max_drawdown),
            "sharpe_ratio": float(sharpe_ratio) if not math.isnan(sharpe_ratio) else 0.0,
            "n_trades": n_trades,
            "total_cost": total_cost,
        }
