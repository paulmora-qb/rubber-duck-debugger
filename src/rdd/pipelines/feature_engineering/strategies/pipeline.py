"""Strategies pipeline — derives trading signals from ingested OHLCV data."""

from kedro.pipeline import Pipeline, node

from rdd.pipelines.feature_engineering.strategies.nodes import (
    assemble_stock_analyses,
    compute_mean_reversion_signals,
    compute_momentum_signals,
    compute_trend_signals,
    compute_volatility_signals,
)


def create_pipeline(**_kwargs) -> Pipeline:
    """Create the strategies pipeline."""
    return Pipeline(
        nodes=[
            node(
                func=compute_momentum_signals,
                inputs=["raw_ohlcv", "params:strategies"],
                outputs="momentum_signals",
                name="compute_momentum_signals",
            ),
            node(
                func=compute_trend_signals,
                inputs=["raw_ohlcv", "params:strategies"],
                outputs="trend_signals",
                name="compute_trend_signals",
            ),
            node(
                func=compute_mean_reversion_signals,
                inputs=["raw_ohlcv", "params:strategies"],
                outputs="mean_reversion_signals",
                name="compute_mean_reversion_signals",
            ),
            node(
                func=compute_volatility_signals,
                inputs=["raw_ohlcv", "params:strategies"],
                outputs="volatility_signals",
                name="compute_volatility_signals",
            ),
            node(
                func=assemble_stock_analyses,
                inputs=[
                    "momentum_signals",
                    "trend_signals",
                    "mean_reversion_signals",
                    "volatility_signals",
                ],
                outputs="stock_analyses",
                name="assemble_stock_analyses",
            ),
        ]
    )
