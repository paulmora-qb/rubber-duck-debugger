"""Integration tests for the finnhub_news pipeline.

Runs the full pipeline via SequentialRunner with a fully in-memory DataCatalog.
The Finnhub client is mocked at the module boundary — no network calls.
"""

from __future__ import annotations

import os

import pandas as pd
import pytest
from kedro.io import DataCatalog, MemoryDataset
from kedro.runner import SequentialRunner

from rdd.pipelines.finnhub_news.pipeline import create_pipeline
from tests.conftest import make_market_news_df


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _raw_article(article_id: int, dt: pd.Timestamp, category: str = "general") -> dict:
    return {
        "id": article_id,
        "datetime": int(dt.timestamp()),
        "headline": f"Headline {article_id}",
        "summary": f"Summary {article_id}",
        "source": "Reuters",
        "url": f"https://reuters.com/{article_id}",
        "image": None,
        "category": category,
        "related": "",
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def pipeline():
    return create_pipeline()


@pytest.fixture
def params():
    return {"categories": ["general"]}


@pytest.fixture(autouse=True)
def set_api_key(mocker):
    mocker.patch.dict(os.environ, {"FINNHUB_API_KEY": "test_key"})


@pytest.fixture
def mock_client(mocker):
    mock_cls = mocker.patch("rdd.pipelines.finnhub_news.nodes.finnhub.Client")
    return mock_cls.return_value


@pytest.fixture
def in_memory_catalog(params):
    return DataCatalog(
        {
            "params:finnhub_news": MemoryDataset(data=params),
            "raw_finnhub_market_news_existing": MemoryDataset(data={}),
            "raw_finnhub_market_news": MemoryDataset(),
        }
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_pipeline_runs_successfully(mock_client, pipeline, in_memory_catalog) -> None:
    articles = [
        _raw_article(1, pd.Timestamp("2024-01-15 10:00:00")),
        _raw_article(2, pd.Timestamp("2024-01-15 11:00:00")),
    ]
    mock_client.general_news.return_value = articles

    SequentialRunner().run(pipeline, in_memory_catalog)

    result = in_memory_catalog.load("raw_finnhub_market_news")
    assert isinstance(result, dict)
    assert len(result) > 0


def test_pipeline_output_has_date_partitions(
    mock_client, pipeline, in_memory_catalog
) -> None:
    articles = [
        _raw_article(1, pd.Timestamp("2024-01-15 09:00:00")),
        _raw_article(2, pd.Timestamp("2024-01-16 10:00:00")),
    ]
    mock_client.general_news.return_value = articles

    SequentialRunner().run(pipeline, in_memory_catalog)

    result = in_memory_catalog.load("raw_finnhub_market_news")
    assert "2024-01-15" in result
    assert "2024-01-16" in result


def test_pipeline_output_contains_expected_columns(
    mock_client, pipeline, in_memory_catalog
) -> None:
    mock_client.general_news.return_value = [
        _raw_article(1, pd.Timestamp("2024-01-15 10:00:00"))
    ]

    SequentialRunner().run(pipeline, in_memory_catalog)

    result = in_memory_catalog.load("raw_finnhub_market_news")
    expected_cols = {
        "article_id",
        "datetime",
        "headline",
        "summary",
        "source",
        "url",
        "image",
        "category",
    }
    for partition_df in result.values():
        assert expected_cols.issubset(partition_df.columns)
        assert not partition_df.empty


def test_pipeline_with_existing_data_merges(mock_client, pipeline, params) -> None:
    existing_df = make_market_news_df(n=2, base_date="2024-01-15", start_id=500)
    new_articles = [_raw_article(1000, pd.Timestamp("2024-01-15 12:00:00"))]
    mock_client.general_news.return_value = new_articles

    catalog = DataCatalog(
        {
            "params:finnhub_news": MemoryDataset(data=params),
            "raw_finnhub_market_news_existing": MemoryDataset(
                data={"2024-01-15": lambda: existing_df}
            ),
            "raw_finnhub_market_news": MemoryDataset(),
        }
    )

    SequentialRunner().run(pipeline, catalog)

    result = catalog.load("raw_finnhub_market_news")
    merged = result["2024-01-15"]
    assert len(merged) == 3
    assert set(merged["article_id"]) == {500, 501, 1000}
