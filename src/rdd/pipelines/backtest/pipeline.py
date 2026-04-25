"""Backtest pipeline."""

from kedro.pipeline import Pipeline, node

from rdd.pipelines.backtest.nodes import run_backtest


def create_pipeline(**_kwargs) -> Pipeline:
    """Create the backtest pipeline."""
    return Pipeline(
        nodes=[
            node(
                func=run_backtest,
                inputs=["strategy_signals", "raw_ohlcv", "params:backtest"],
                outputs=["backtest_equity_curve", "backtest_trades", "backtest_positions"],
                name="run_backtest",
            ),
        ]
    )
