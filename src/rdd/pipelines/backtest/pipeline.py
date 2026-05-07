"""Backtest pipeline."""

from kedro.pipeline import Pipeline, node

from rdd.pipelines.backtest.nodes import (
    compare_backtests,
    holdings_to_signals,
    run_backtest,
)
from rdd.pipelines.strategies.price_strategies.nodes import (
    compute_52w_high_holdings,
    compute_adx_holdings,
    compute_cross_sect_momentum_holdings,
    compute_donchian_holdings,
    compute_obv_holdings,
)

_PRICE_STRATEGIES = [
    ("donchian_breakout", compute_donchian_holdings),
    ("high_52w", compute_52w_high_holdings),
    ("cross_sect_momentum", compute_cross_sect_momentum_holdings),
    ("obv_momentum", compute_obv_holdings),
    ("adx_trend", compute_adx_holdings),
]


def create_pipeline(**_kwargs) -> Pipeline:
    """Create the backtest pipeline (single strategy via strategy_signals MemoryDataset)."""
    return Pipeline(
        nodes=[
            node(
                func=run_backtest,
                inputs=["strategy_signals", "raw_ohlcv_existing", "params:backtest"],
                outputs=[
                    "backtest_equity_curve",
                    "backtest_trades",
                    "backtest_positions",
                ],
                name="run_backtest",
            ),
        ]
    )


def create_price_strategies_pipeline(**_kwargs) -> Pipeline:
    """Run a full historical backtest for each of the five price strategies.

    Signal generation uses ``params:price_strategies_backtest`` which sets
    ``start_date`` to the OHLCV history start (2023-01-01), producing weekly
    signals across the full available price history.

    Outputs per strategy: equity curve CSV, trades CSV, positions parquet.
    Final node: comparison table sorted by Sharpe ratio.
    """
    strategy_nodes = []
    equity_curve_inputs: dict[str, str] = {}

    for strategy_name, strategy_fn in _PRICE_STRATEGIES:
        strategy_nodes += [
            node(
                func=strategy_fn,
                inputs=["raw_ohlcv_existing", "params:price_strategies_backtest"],
                outputs=f"{strategy_name}.holdings_bt",
                name=f"generate_{strategy_name}_holdings",
            ),
            node(
                func=holdings_to_signals,
                inputs=f"{strategy_name}.holdings_bt",
                outputs=f"{strategy_name}.signals_bt",
                name=f"convert_{strategy_name}_signals",
            ),
            node(
                func=run_backtest,
                inputs=[
                    f"{strategy_name}.signals_bt",
                    "raw_ohlcv_existing",
                    "params:backtest",
                ],
                outputs=[
                    f"{strategy_name}.backtest_equity_curve",
                    f"{strategy_name}.backtest_trades",
                    f"{strategy_name}.backtest_positions",
                ],
                name=f"run_{strategy_name}_backtest",
            ),
        ]
        equity_curve_inputs[strategy_name] = f"{strategy_name}.backtest_equity_curve"

    compare_node = node(
        func=compare_backtests,
        inputs=equity_curve_inputs,
        outputs="backtest_comparison",
        name="compare_price_strategy_backtests",
    )

    return Pipeline([*strategy_nodes, compare_node])
