"""Company news ingestion pipeline — fetches news articles from Finnhub."""

from kedro.pipeline import Pipeline, node

from rdd.pipelines.company_information.company_news.nodes import ingest_company_news


def create_pipeline(**_kwargs) -> Pipeline:
    """Create the company news ingestion pipeline."""
    return Pipeline(
        nodes=[
            node(
                func=ingest_company_news,
                inputs=[
                    "ticker_universe",
                    "raw_company_news_existing",
                    "params:company_news",
                ],
                outputs="raw_company_news",
                name="ingest_company_news",
            ),
        ]
    )
