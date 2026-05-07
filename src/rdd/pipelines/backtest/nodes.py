"""Kedro node for running the backtest."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import pandas as pd

from rdd.backtest.engine import Backtester

logger = logging.getLogger(__name__)


def run_backtest(
    strategy_signals: pd.DataFrame,
    raw_ohlcv: dict[str, Callable[[], pd.DataFrame]],
    params: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Run the backtesting engine against historical OHLCV data.

    Args:
        strategy_signals: Signal DataFrame matching SignalSchema.
        raw_ohlcv: Partitioned OHLCV dataset (ticker → lazy loader).
        params: Backtest parameter block from params_backtest.yml.

    Returns:
        Tuple of (equity_curve, trades, positions) DataFrames for catalog persistence.
    """
    logger.info("Loading %d OHLCV partitions.", len(raw_ohlcv))
    ohlcv = pd.concat(
        [loader() for loader in raw_ohlcv.values()],
        ignore_index=True,
    )

    bt = Backtester(
        initial_capital=float(params["initial_capital"]),
        cost_bps=float(params["cost_bps"]),
        execution=params["execution"],
    )

    result = bt.run(strategy_signals, ohlcv)

    summary = result.summary()
    logger.info(
        "Backtest complete. Total return: %.2f%%, Sharpe: %.2f, Max DD: %.2f%%",
        summary["total_return"] * 100,
        summary["sharpe_ratio"],
        summary["max_drawdown"] * 100,
    )

    return result.equity_curve.to_frame("value"), result.trades, result.positions


def holdings_to_signals(holdings_dict: dict) -> pd.DataFrame:
    """Convert a date-keyed holdings dict to backtester signal format.

    Handles both ``dict[str, pd.DataFrame]`` (MemoryDataset output from a
    price-strategy node) and ``dict[str, Callable]`` (PartitionedDataset loaders).
    Normalises weights to sum exactly to 1.0 per date so SignalSchema passes.
    """
    frames = []
    for v in holdings_dict.values():
        df = v() if callable(v) else v
        if isinstance(df, pd.DataFrame) and not df.empty:
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["date", "ticker", "position", "weight"])
    all_holdings = pd.concat(frames, ignore_index=True)
    signals = all_holdings[["date", "ticker", "weight"]].copy()
    signals["date"] = pd.to_datetime(signals["date"])
    signals["position"] = 1
    signals["weight"] = signals.groupby("date")["weight"].transform(
        lambda w: w / w.sum()
    )
    return signals[["date", "ticker", "position", "weight"]]


_TRADING_DAYS_PER_YEAR = 252


def compare_backtests(**equity_curves: pd.DataFrame) -> pd.DataFrame:
    """Combine per-strategy equity curves into a comparison summary table.

    Logs a formatted table and returns a DataFrame with one row per strategy,
    sorted by Sharpe ratio descending.
    """
    rows = []
    for name, equity_df in equity_curves.items():
        if equity_df is None or equity_df.empty:
            continue
        curve = (
            equity_df["value"].dropna()
            if "value" in equity_df.columns
            else equity_df.squeeze().dropna()
        )
        if len(curve) < 2:
            continue
        rets = curve.pct_change().dropna()
        total = curve.iloc[-1] / curve.iloc[0] - 1
        n_years = len(rets) / _TRADING_DAYS_PER_YEAR
        ann = (1 + total) ** (1 / n_years) - 1 if n_years > 0 else float("nan")
        vol = rets.std() * (_TRADING_DAYS_PER_YEAR**0.5)
        sharpe = ann / vol if vol > 0 else 0.0
        max_dd = ((curve - curve.cummax()) / curve.cummax()).min()
        rows.append(
            {
                "strategy": name,
                "total_return_pct": round(total * 100, 2),
                "ann_return_pct": round(ann * 100, 2),
                "ann_volatility_pct": round(vol * 100, 2),
                "sharpe_ratio": round(sharpe, 2),
                "max_drawdown_pct": round(max_dd * 100, 2),
                "n_trading_days": len(rets),
            }
        )

    if not rows:
        return pd.DataFrame()

    result = (
        pd.DataFrame(rows)
        .sort_values("sharpe_ratio", ascending=False)
        .reset_index(drop=True)
    )
    logger.info("\nBacktest comparison:\n%s", result.to_string(index=False))
    return result
