"""Integration tests for the company_info pipeline.

Runs the pipeline end-to-end via SequentialRunner with a fully in-memory
DataCatalog.  All yfinance calls are mocked at the module boundary.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest
from kedro.io import DataCatalog, MemoryDataset
from kedro.runner import SequentialRunner

from rdd.pipelines.company_info.pipeline import create_pipeline

_SAMPLE_INFO: dict = {
    "longName": "Apple Inc.",
    "sector": "Technology",
    "industry": "Consumer Electronics",
    "marketCap": 3_000_000_000_000,
    "fullTimeEmployees": 161_000,
    "country": "United States",
    "currency": "USD",
    "exchange": "NMS",
}


@pytest.fixture
def pipeline():
    return create_pipeline()


@pytest.fixture
def params():
    return {"refresh_days": 7}


@pytest.fixture
def in_memory_catalog(params):
    return DataCatalog(
        {
            "ticker_universe": MemoryDataset(data=["AAPL", "MSFT"]),
            "raw_company_info_existing": MemoryDataset(data={}),
            "raw_company_info": MemoryDataset(),
            "params:company_info": MemoryDataset(data=params),
        }
    )


def test_pipeline_runs_successfully(mocker, pipeline, in_memory_catalog) -> None:
    mock_ticker = MagicMock()
    mock_ticker.info = _SAMPLE_INFO
    mocker.patch("rdd.pipelines.company_info.nodes.yf.Ticker", return_value=mock_ticker)

    SequentialRunner().run(pipeline, in_memory_catalog)

    result = in_memory_catalog.load("raw_company_info")
    assert isinstance(result, dict)
    assert len(result) > 0


def test_pipeline_output_schema(mocker, pipeline, in_memory_catalog) -> None:
    mock_ticker = MagicMock()
    mock_ticker.info = _SAMPLE_INFO
    mocker.patch("rdd.pipelines.company_info.nodes.yf.Ticker", return_value=mock_ticker)

    SequentialRunner().run(pipeline, in_memory_catalog)

    result = in_memory_catalog.load("raw_company_info")
    expected_cols = {
        "ticker",
        "name",
        "sector",
        "industry",
        "market_cap",
        "employees",
        "country",
        "currency",
        "exchange",
        "fetched_at",
    }
    for key, df in result.items():
        assert expected_cols.issubset(df.columns), f"{key}: missing columns"
        assert not df.empty, f"{key}: empty DataFrame"


def test_pipeline_skips_fresh_snapshots(
    mocker, pipeline, params, company_info_df
) -> None:
    ticker = "AAPL"
    fresh_df = company_info_df.copy()
    fresh_df["fetched_at"] = pd.Timestamp.now("UTC").tz_convert(None)

    catalog = DataCatalog(
        {
            "ticker_universe": MemoryDataset(data=[ticker]),
            "raw_company_info_existing": MemoryDataset(
                data={ticker.lower(): lambda: fresh_df}
            ),
            "raw_company_info": MemoryDataset(),
            "params:company_info": MemoryDataset(data=params),
        }
    )
    yf_spy = mocker.patch("rdd.pipelines.company_info.nodes.yf.Ticker")

    SequentialRunner().run(pipeline, catalog)

    yf_spy.assert_not_called()
