"""Earnings history ingestion pipeline."""

from kedro.pipeline import Pipeline, node

from rdd.pipelines.earnings_history.nodes import ingest_earnings_history


def create_pipeline(**_kwargs) -> Pipeline:
    """Create the earnings history ingestion pipeline."""
    return Pipeline(
        nodes=[
            node(
                func=ingest_earnings_history,
                inputs=[
                    "ticker_universe",
                    "raw_earnings_history_existing",
                    "params:earnings_history",
                ],
                outputs="raw_earnings_history",
                name="ingest_earnings_history",
            ),
        ]
    )
