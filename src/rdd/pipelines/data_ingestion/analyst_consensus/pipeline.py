"""Analyst consensus ingestion pipeline."""

from kedro.pipeline import Pipeline, node

from rdd.pipelines.data_ingestion.analyst_consensus.nodes import (
    ingest_analyst_consensus,
)


def create_pipeline(**_kwargs) -> Pipeline:
    """Create the analyst consensus ingestion pipeline."""
    return Pipeline(
        nodes=[
            node(
                func=ingest_analyst_consensus,
                inputs=[
                    "ticker_universe",
                    "raw_analyst_consensus_existing",
                    "params:analyst_consensus",
                ],
                outputs="raw_analyst_consensus",
                name="ingest_analyst_consensus",
            ),
        ]
    )
