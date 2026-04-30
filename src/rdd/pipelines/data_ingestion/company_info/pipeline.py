"""Company information ingestion pipeline — fetches metadata snapshots from yfinance."""

from kedro.pipeline import Pipeline, node

from rdd.pipelines.data_ingestion.company_info.nodes import ingest_company_info


def create_pipeline(**_kwargs) -> Pipeline:
    """Create the company info ingestion pipeline."""
    return Pipeline(
        nodes=[
            node(
                func=ingest_company_info,
                inputs=[
                    "ticker_universe",
                    "raw_company_info_existing",
                    "params:company_info",
                ],
                outputs="raw_company_info",
                name="ingest_company_info",
            ),
        ]
    )
