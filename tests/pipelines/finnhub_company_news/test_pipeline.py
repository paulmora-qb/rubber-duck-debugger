"""Integration tests for the finnhub_company_news pipeline.

Runs both nodes end-to-end via SequentialRunner with a fully in-memory
DataCatalog. All network calls are mocked at the module boundary.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock

import pandas as pd
import pytest
from kedro.io import DataCatalog, MemoryDataset
from kedro.runner import SequentialRunner

from rdd.pipelines.finnhub_company_news.pipeline import create_pipeline
from tests.conftest import make_company_news_df

# ---------------------------------------------------------------------------
# HTML / API mock helpers
# ---------------------------------------------------------------------------

_SP500_HTML = """
<html><body>
<table id="constituents">
<tr><th>Symbol</th><th>Security</th></tr>
<tr><td>AAPL</td><td>Apple Inc.</td></tr>
<tr><td>MSFT</td><td>Microsoft</td></tr>
</table>
</body></html>
"""


def _mock_http(html: str) -> MagicMock:
    resp = MagicMock()
    resp.text = html
    resp.raise_for_status.return_value = None
    return resp


def _raw_article(article_id: int, ticker: str, dt: pd.Timestamp) -> dict:
    return {
        "id": article_id,
        "datetime": int(dt.timestamp()),
        "headline": f"Headline {article_id}",
        "summary": f"Summary {article_id}",
        "source": "Yahoo",
        "url": f"https://finance.yahoo.com/{article_id}",
        "image": None,
        "related": ticker,
        "category": "company",
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def pipeline():
    return create_pipeline()


@pytest.fixture
def di_params():
    return {
        "start_date": "2024-01-01",
        "batch_size": 10,
        "index_sources": {"sp500": True, "nasdaq100": False},
    }


@pytest.fixture
def cn_params():
    return {"start_date": "2024-01-01", "sleep_seconds": 0}


@pytest.fixture(autouse=True)
def set_api_key(mocker):
    mocker.patch.dict(os.environ, {"FINNHUB_API_KEY": "test_key"})


@pytest.fixture
def mock_requests(mocker):
    return mocker.patch(
        "rdd.pipelines.data_ingestion.nodes.requests.get",
        return_value=_mock_http(_SP500_HTML),
    )


@pytest.fixture
def mock_finnhub(mocker):
    mock_cls = mocker.patch("rdd.pipelines.finnhub_company_news.nodes.finnhub.Client")
    return mock_cls.return_value


@pytest.fixture
def in_memory_catalog(di_params, cn_params):
    return DataCatalog(
        {
            "params:data_ingestion": MemoryDataset(data=di_params),
            "params:finnhub_company_news": MemoryDataset(data=cn_params),
            "ticker_universe_cn": MemoryDataset(),
            "raw_finnhub_company_news_existing": MemoryDataset(data={}),
            "raw_finnhub_company_news": MemoryDataset(),
        }
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_pipeline_runs_successfully(
    mock_requests, mock_finnhub, pipeline, in_memory_catalog
) -> None:
    mock_finnhub.company_news.return_value = [
        _raw_article(1, "AAPL", pd.Timestamp("2024-01-15 10:00:00")),
        _raw_article(2, "AAPL", pd.Timestamp("2024-01-15 11:00:00")),
    ]

    SequentialRunner().run(pipeline, in_memory_catalog)

    result = in_memory_catalog.load("raw_finnhub_company_news")
    assert isinstance(result, dict)
    assert len(result) > 0


def test_pipeline_output_has_expected_columns(
    mock_requests, mock_finnhub, pipeline, in_memory_catalog
) -> None:
    mock_finnhub.company_news.return_value = [
        _raw_article(1, "AAPL", pd.Timestamp("2024-01-15 10:00:00"))
    ]

    SequentialRunner().run(pipeline, in_memory_catalog)

    result = in_memory_catalog.load("raw_finnhub_company_news")
    expected = {"article_id", "ticker", "datetime", "headline", "summary", "source", "url"}
    for df in result.values():
        assert expected.issubset(df.columns)
        assert "image" not in df.columns
        assert "category" not in df.columns


def test_pipeline_partitions_by_ticker(
    mock_requests, mock_finnhub, pipeline, in_memory_catalog
) -> None:
    mock_finnhub.company_news.side_effect = [
        [_raw_article(1, "AAPL", pd.Timestamp("2024-01-15 10:00:00"))],
        [_raw_article(2, "MSFT", pd.Timestamp("2024-01-15 11:00:00"))],
    ]

    SequentialRunner().run(pipeline, in_memory_catalog)

    result = in_memory_catalog.load("raw_finnhub_company_news")
    assert "aapl" in result
    assert "msft" in result


def test_pipeline_with_existing_data_merges(
    mock_requests, mock_finnhub, pipeline, di_params, cn_params
) -> None:
    existing_df = make_company_news_df(n=2, ticker="AAPL", start_id=500)
    mock_finnhub.company_news.side_effect = [
        [_raw_article(1000, "AAPL", pd.Timestamp("2024-01-15 10:00:00"))],
        [],
    ]

    catalog = DataCatalog(
        {
            "params:data_ingestion": MemoryDataset(data=di_params),
            "params:finnhub_company_news": MemoryDataset(data=cn_params),
            "ticker_universe_cn": MemoryDataset(),
            "raw_finnhub_company_news_existing": MemoryDataset(
                data={"aapl": lambda: existing_df}
            ),
            "raw_finnhub_company_news": MemoryDataset(),
        }
    )

    SequentialRunner().run(pipeline, catalog)

    result = catalog.load("raw_finnhub_company_news")
    assert len(result["aapl"]) == 3
    assert set(result["aapl"]["article_id"]) == {500, 501, 1000}
