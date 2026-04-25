"""Finnhub market news pipeline — fetches macro news articles by category."""

from kedro.pipeline import Pipeline, node

from rdd.pipelines.finnhub_news.nodes import fetch_market_news


def create_pipeline(**_kwargs) -> Pipeline:
    """Create the Finnhub market news pipeline."""
    return Pipeline(
        nodes=[
            node(
                func=fetch_market_news,
                inputs=["raw_finnhub_market_news_existing", "params:finnhub_news"],
                outputs="raw_finnhub_market_news",
                name="fetch_market_news",
            ),
        ]
    )
