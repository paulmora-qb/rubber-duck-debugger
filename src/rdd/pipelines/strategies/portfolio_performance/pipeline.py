"""Portfolio performance pipeline — variant-per-strategy factory."""

from __future__ import annotations

from kedro.pipeline import Pipeline, node, pipeline

from rdd.pipelines.strategies.portfolio_performance.nodes import (
    compile_report,
    compute_benchmark_returns,
    compute_performance_metrics,
    compute_strategy_returns,
    send_performance_email,
)

_BASE_NODES = [
    node(
        func=compute_strategy_returns,
        inputs=["holdings_existing", "ohlcv_existing", "params:portfolio_performance"],
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
        parameters={"params:portfolio_performance": "params:portfolio_performance"},
        namespace=namespace,
    )


def create_pipeline(variants: list[str] | None = None, **_kwargs) -> Pipeline:
    """Create the portfolio_performance pipeline for all strategy *variants*.

    Args:
        variants: List of strategy names.  Each must have a catalog entry
            ``{variant}.holdings_existing`` (NullablePartitionedDataset).
            Defaults to ``["ai_fundamental_screen"]``.

    Returns:
        Combined pipeline that computes per-strategy metrics, benchmark returns,
        then emails a weekly report with cumulative-return chart, drawdown chart,
        holdings breakdown, and KPI table.
    """
    if not variants:
        variants = ["ai_fundamental_screen"]

    variant_pipelines: Pipeline = sum(
        (_variant_pipeline(v) for v in variants),
        Pipeline([]),
    )

    email_inputs: dict[str, str] = {
        "report": "performance_report",
        "params": "params:portfolio_performance",
        "benchmark_returns": "benchmark_returns",
        "company_info": "raw_company_info_existing",
    }
    for v in variants:
        email_inputs[f"{v}_returns"] = f"{v}.daily_returns"
        email_inputs[f"{v}_holdings"] = f"{v}.holdings_existing"

    shared_nodes = Pipeline(
        [
            node(
                func=compute_benchmark_returns,
                inputs=["raw_ohlcv_existing", "params:portfolio_performance"],
                outputs="benchmark_returns",
                name="compute_benchmark_returns",
            ),
            node(
                func=compile_report,
                inputs={v: f"{v}.metrics" for v in variants},
                outputs="performance_report",
                name="compile_report",
            ),
            node(
                func=send_performance_email,
                inputs=email_inputs,
                outputs=None,
                name="send_performance_email",
            ),
        ]
    )

    return variant_pipelines + shared_nodes
