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
