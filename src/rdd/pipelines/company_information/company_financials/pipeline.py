"""Company financials ingestion pipeline — quarterly and annual statements from yfinance."""

from kedro.pipeline import Pipeline, node

from rdd.pipelines.company_information.company_financials.nodes import (
    ingest_company_financials,
)


def create_pipeline(**_kwargs) -> Pipeline:
    """Create the company financials ingestion pipeline."""
    return Pipeline(
        nodes=[
            node(
                func=ingest_company_financials,
                inputs=[
                    "ticker_universe",
                    "raw_company_financials_quarterly_existing",
                    "raw_company_financials_annual_existing",
                    "params:company_financials",
                ],
                outputs=[
                    "raw_company_financials_quarterly",
                    "raw_company_financials_annual",
                ],
                name="ingest_company_financials",
            ),
        ]
    )
