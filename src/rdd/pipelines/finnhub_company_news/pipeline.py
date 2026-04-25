"""Finnhub company news pipeline — fetches per-ticker news articles."""

from kedro.pipeline import Pipeline, node

from rdd.pipelines.data_ingestion.nodes import fetch_ticker_universe
from rdd.pipelines.finnhub_company_news.nodes import fetch_company_news


def create_pipeline(**_kwargs) -> Pipeline:
    """Create the Finnhub company news pipeline.

    Node 1 reuses ``fetch_ticker_universe`` from the data_ingestion pipeline
    so this pipeline is self-contained and can be run independently.
    """
    return Pipeline(
        nodes=[
            node(
                func=fetch_ticker_universe,
                inputs="params:data_ingestion",
                outputs="ticker_universe_cn",
                name="fetch_ticker_universe_cn",
            ),
            node(
                func=fetch_company_news,
                inputs=[
                    "ticker_universe_cn",
                    "raw_finnhub_company_news_existing",
                    "params:finnhub_company_news",
                ],
                outputs="raw_finnhub_company_news",
                name="fetch_company_news",
            ),
        ]
    )
