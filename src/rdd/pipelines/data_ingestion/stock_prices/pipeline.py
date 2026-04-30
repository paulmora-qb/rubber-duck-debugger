"""Data ingestion pipeline — fetches OHLCV data from yfinance."""

from kedro.pipeline import Pipeline, node

from rdd.pipelines.data_ingestion.stock_prices.nodes import (
    fetch_ticker_universe,
    ingest_ohlcv,
)


def create_pipeline(**_kwargs) -> Pipeline:
    """Create the data ingestion pipeline."""
    return Pipeline(
        nodes=[
            node(
                func=fetch_ticker_universe,
                inputs="params:stock_prices",
                outputs="ticker_universe",
                name="fetch_ticker_universe",
            ),
            node(
                func=ingest_ohlcv,
                inputs=[
                    "ticker_universe",
                    "raw_ohlcv_existing",
                    "params:stock_prices",
                ],
                outputs="raw_ohlcv",
                name="ingest_ohlcv",
            ),
        ]
    )
