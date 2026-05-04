"""Portfolio performance pipeline — variant-per-strategy factory.

Pattern mirrors CustomerOne: ``create_pipeline(variants)`` loops over strategy
names and instantiates the same base pipeline under each namespace, then appends
shared compile + email nodes that operate across all variants.
"""

from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from rdd.pipelines.strategies.portfolio_performance.nodes import (
    compile_report,
    compute_performance_metrics,
    compute_strategy_returns,
    send_performance_email,
)

_BASE_NODES = [
    node(
        func=compute_strategy_returns,
        inputs=["holdings_existing", "ohlcv_existing"],
        outputs="daily_returns",
        name="compute_strategy_returns",
    ),
    node(
        func=compute_performance_metrics,
        inputs="daily_returns",
        outputs="metrics",
        name="compute_performance_metrics",
    ),
]


def _variant_pipeline(namespace: str) -> Pipeline:
    """Instantiate the base nodes under *namespace*, sharing ohlcv_existing."""
    return pipeline(
        Pipeline(_BASE_NODES),
        inputs={"ohlcv_existing": "raw_ohlcv_existing"},
        namespace=namespace,
    )


def create_pipeline(variants: list[str] | None = None, **_kwargs) -> Pipeline:
    """Create the portfolio_performance pipeline for all strategy *variants*.

    Args:
        variants: List of strategy names.  Each must have a catalog entry
            ``{variant}.holdings_existing`` (NullablePartitionedDataset).
            Defaults to ``["claude_fundamental"]``.

    Returns:
        Combined pipeline that computes per-strategy metrics then emails a
        weekly comparison report.
    """
    if not variants:
        variants = ["claude_fundamental"]

    variant_pipelines: Pipeline = sum(
        (_variant_pipeline(v) for v in variants),
        Pipeline([]),
    )

    shared_nodes = Pipeline(
        [
            node(
                func=compile_report,
                inputs={v: f"{v}.metrics" for v in variants},
                outputs="performance_report",
                name="compile_report",
            ),
            node(
                func=send_performance_email,
                inputs=["performance_report", "params:portfolio_performance"],
                outputs=None,
                name="send_performance_email",
            ),
        ]
    )

    return variant_pipelines + shared_nodes
