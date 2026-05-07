"""Price-only strategy pipelines — one node per strategy."""

from kedro.pipeline import Pipeline, node

from rdd.pipelines.strategies.price_strategies.nodes import (
    compute_52w_high_holdings,
    compute_adx_holdings,
    compute_cross_sect_momentum_holdings,
    compute_donchian_holdings,
    compute_obv_holdings,
)


def create_pipeline(**_kwargs) -> Pipeline:
    """Create the price_strategies pipeline."""
    return Pipeline(
        nodes=[
            node(
                func=compute_donchian_holdings,
                inputs=["raw_ohlcv", "params:price_strategies"],
                outputs="donchian_breakout.holdings",
                name="compute_donchian_holdings",
            ),
            node(
                func=compute_52w_high_holdings,
                inputs=["raw_ohlcv", "params:price_strategies"],
                outputs="high_52w.holdings",
                name="compute_52w_high_holdings",
            ),
            node(
                func=compute_cross_sect_momentum_holdings,
                inputs=["raw_ohlcv", "params:price_strategies"],
                outputs="cross_sect_momentum.holdings",
                name="compute_cross_sect_momentum_holdings",
            ),
            node(
                func=compute_obv_holdings,
                inputs=["raw_ohlcv", "params:price_strategies"],
                outputs="obv_momentum.holdings",
                name="compute_obv_holdings",
            ),
            node(
                func=compute_adx_holdings,
                inputs=["raw_ohlcv", "params:price_strategies"],
                outputs="adx_trend.holdings",
                name="compute_adx_holdings",
            ),
        ]
    )
