"""Integration tests for the company_news pipeline.

Runs the pipeline end-to-end via SequentialRunner with a fully in-memory
DataCatalog.  All Finnhub calls are mocked at the module boundary.
"""

from __future__ import annotations

import pandas as pd
import pytest
from kedro.io import DataCatalog, MemoryDataset
from kedro.runner import SequentialRunner

from rdd.pipelines.data_ingestion.company_news.pipeline import create_pipeline


@pytest.fixture(autouse=True)
def no_sleep(mocker):
    mocker.patch("rdd.pipelines.data_ingestion.company_news.nodes.time.sleep")


def _make_articles(ticker: str, n: int = 3) -> list[dict]:
    base_ts = int(pd.Timestamp("2024-01-02").timestamp())
    return [
        {
            "datetime": base_ts + i * 86_400,
            "headline": f"Headline {i}",
            "summary": f"Summary {i}",
            "source": "Reuters",
            "url": f"https://example.com/{i}",
            "category": "company news",
        }
        for i in range(n)
    ]


@pytest.fixture(autouse=True)
def set_api_key(monkeypatch) -> None:
    monkeypatch.setenv("FINNHUB_API_KEY", "test-key")


@pytest.fixture
def pipeline():
    return create_pipeline()


@pytest.fixture
def params():
    return {"start_date": "2023-01-01"}


@pytest.fixture
def in_memory_catalog(params):
    return DataCatalog(
        {
            "ticker_universe": MemoryDataset(data=["AAPL", "MSFT"]),
            "raw_company_news_existing": MemoryDataset(data={}),
            "raw_company_news": MemoryDataset(),
            "params:company_news": MemoryDataset(data=params),
        }
    )


def test_pipeline_runs_successfully(mocker, pipeline, in_memory_catalog) -> None:
    mock_client = mocker.MagicMock()
    mock_client.company_news.side_effect = lambda t, **_: _make_articles(t)
    mocker.patch(
        "rdd.pipelines.data_ingestion.company_news.nodes.finnhub.Client",
        return_value=mock_client,
    )

    SequentialRunner().run(pipeline, in_memory_catalog)

    result = in_memory_catalog.load("raw_company_news")
    assert isinstance(result, dict)
    assert "aapl" in result
    assert "msft" in result


def test_pipeline_output_schema(mocker, pipeline, in_memory_catalog) -> None:
    mock_client = mocker.MagicMock()
    mock_client.company_news.side_effect = lambda t, **_: _make_articles(t)
    mocker.patch(
        "rdd.pipelines.data_ingestion.company_news.nodes.finnhub.Client",
        return_value=mock_client,
    )

    SequentialRunner().run(pipeline, in_memory_catalog)

    result = in_memory_catalog.load("raw_company_news")
    expected_cols = {
        "ticker",
        "published_at",
        "headline",
        "summary",
        "source",
        "url",
        "category",
    }
    for key, df in result.items():
        assert expected_cols.issubset(df.columns), f"{key}: missing columns"
        assert not df.empty, f"{key}: empty DataFrame"


def test_pipeline_with_existing_data_does_incremental_fetch(
    mocker, pipeline, params, company_news_df
) -> None:
    ticker = "AAPL"
    existing_df = company_news_df.copy()
    existing_df["ticker"] = ticker
    last_date = pd.Timestamp(existing_df["published_at"].max())

    catalog = DataCatalog(
        {
            "ticker_universe": MemoryDataset(data=[ticker]),
            "raw_company_news_existing": MemoryDataset(
                data={ticker.lower(): lambda: existing_df}
            ),
            "raw_company_news": MemoryDataset(),
            "params:company_news": MemoryDataset(data=params),
        }
    )

    new_ts = int((last_date + pd.Timedelta(days=2)).timestamp())
    new_articles = [
        {
            "datetime": new_ts + i * 86_400,
            "headline": f"New {i}",
            "summary": "s",
            "source": "AP",
            "url": "u",
            "category": "c",
        }
        for i in range(2)
    ]
    mock_client = mocker.MagicMock()
    mock_client.company_news.return_value = new_articles
    mocker.patch(
        "rdd.pipelines.data_ingestion.company_news.nodes.finnhub.Client",
        return_value=mock_client,
    )

    SequentialRunner().run(pipeline, catalog)

    result = catalog.load("raw_company_news")
    assert ticker.lower() in result
    assert len(result[ticker.lower()]) > len(existing_df)
    call_kwargs = mock_client.company_news.call_args[1]
    assert pd.Timestamp(call_kwargs["_from"]) > last_date
