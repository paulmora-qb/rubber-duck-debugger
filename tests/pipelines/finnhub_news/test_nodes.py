"""Unit tests for finnhub_news nodes.

Network is disabled globally via --disable-socket.
The Finnhub client is mocked at the module boundary.
"""

from __future__ import annotations

import os
from collections.abc import Callable

import pandas as pd
import pytest

from rdd.pipelines.finnhub_news.nodes import _parse_articles, fetch_market_news
from tests.conftest import make_market_news_df

# ---------------------------------------------------------------------------
# Raw article fixture helpers
# ---------------------------------------------------------------------------


def _raw_article(
    article_id: int = 1000,
    dt: pd.Timestamp | None = None,
    category: str = "general",
) -> dict:
    if dt is None:
        dt = pd.Timestamp("2024-01-15 10:00:00")
    return {
        "id": article_id,
        "datetime": int(dt.timestamp()),
        "headline": f"Headline {article_id}",
        "summary": f"Summary {article_id}",
        "source": "Reuters",
        "url": f"https://reuters.com/{article_id}",
        "image": "https://img.reuters.com/img.jpg",
        "category": category,
        "related": "",
    }


def _make_raw_articles(
    n: int = 3,
    base_id: int = 1000,
    base_date: str = "2024-01-15",
) -> list[dict]:
    base = pd.Timestamp(base_date)
    return [_raw_article(base_id + i, base + pd.Timedelta(hours=i)) for i in range(n)]


# ---------------------------------------------------------------------------
# _parse_articles
# ---------------------------------------------------------------------------


class TestParseArticles:
    def test_empty_list_returns_correct_columns(self):
        df = _parse_articles([], category="general")
        assert df.empty
        assert set(df.columns) == {
            "article_id",
            "datetime",
            "headline",
            "summary",
            "source",
            "url",
            "image",
            "category",
        }

    def test_normalises_fields(self):
        raw = [_raw_article(42, pd.Timestamp("2024-01-15 09:00:00"), "general")]
        df = _parse_articles(raw, category="general")
        assert len(df) == 1
        assert df.loc[0, "article_id"] == 42
        assert df.loc[0, "category"] == "general"
        assert isinstance(df.loc[0, "datetime"], pd.Timestamp)

    def test_null_summary_becomes_empty_string(self):
        raw = [_raw_article(1)]
        raw[0]["summary"] = None
        df = _parse_articles(raw, category="general")
        assert df.loc[0, "summary"] == ""

    def test_null_image_becomes_none(self):
        raw = [_raw_article(1)]
        raw[0]["image"] = None
        df = _parse_articles(raw, category="general")
        assert df.loc[0, "image"] is None

    def test_datetime_converts_unix_to_timestamp(self):
        unix_ts = 1705312800  # 2024-01-15 10:00:00 UTC
        raw = [_raw_article(1, pd.Timestamp(unix_ts, unit="s"), "general")]
        df = _parse_articles(raw, category="general")
        assert df.loc[0, "datetime"] == pd.Timestamp(unix_ts, unit="s")

    def test_multiple_articles_all_parsed(self):
        raw = _make_raw_articles(n=5)
        df = _parse_articles(raw, category="general")
        assert len(df) == 5
        assert list(df["article_id"]) == [1000, 1001, 1002, 1003, 1004]


# ---------------------------------------------------------------------------
# fetch_market_news
# ---------------------------------------------------------------------------


class TestFetchMarketNews:
    @pytest.fixture
    def params(self) -> dict:
        return {"categories": ["general"]}

    @pytest.fixture
    def mock_client(self, mocker):
        """Patch finnhub.Client so no real HTTP connections are made."""
        mock_cls = mocker.patch("rdd.pipelines.finnhub_news.nodes.finnhub.Client")
        return mock_cls.return_value

    @pytest.fixture(autouse=True)
    def set_api_key(self, mocker):
        mocker.patch.dict(os.environ, {"FINNHUB_API_KEY": "test_key"})

    def test_happy_path_writes_date_partition(self, mock_client, params):
        raw = _make_raw_articles(n=2, base_date="2024-01-15")
        mock_client.general_news.return_value = raw

        result = fetch_market_news({}, params)

        assert "2024-01-15" in result
        assert len(result["2024-01-15"]) == 2

    def test_merges_with_existing_partition(self, mock_client, params):
        existing_df = make_market_news_df(n=2, base_date="2024-01-15", start_id=500)
        raw = _make_raw_articles(n=2, base_id=1000, base_date="2024-01-15")
        mock_client.general_news.return_value = raw

        existing: dict[str, Callable[[], pd.DataFrame]] = {
            "2024-01-15": lambda: existing_df
        }
        result = fetch_market_news(existing, params)

        merged = result["2024-01-15"]
        assert len(merged) == 4
        assert set(merged["article_id"]) == {500, 501, 1000, 1001}

    def test_deduplicates_by_article_id(self, mock_client, params):
        existing_df = make_market_news_df(n=2, base_date="2024-01-15", start_id=1000)
        # New fetch returns the same two articles
        raw = _make_raw_articles(n=2, base_id=1000, base_date="2024-01-15")
        mock_client.general_news.return_value = raw

        existing: dict[str, Callable[[], pd.DataFrame]] = {
            "2024-01-15": lambda: existing_df
        }
        result = fetch_market_news(existing, params)

        assert len(result["2024-01-15"]) == 2

    def test_empty_response_returns_existing(self, mock_client, params):
        mock_client.general_news.return_value = []
        existing_df = make_market_news_df(n=3, base_date="2024-01-10")
        existing: dict[str, Callable[[], pd.DataFrame]] = {
            "2024-01-10": lambda: existing_df
        }
        result = fetch_market_news(existing, params)

        assert "2024-01-10" in result
        assert len(result["2024-01-10"]) == 3

    def test_carries_forward_unaffected_partitions(self, mock_client, params):
        # New fetch is for 2024-01-15; existing has 2024-01-10
        old_df = make_market_news_df(n=2, base_date="2024-01-10", start_id=500)
        raw = _make_raw_articles(n=2, base_id=1000, base_date="2024-01-15")
        mock_client.general_news.return_value = raw

        existing: dict[str, Callable[[], pd.DataFrame]] = {
            "2024-01-10": lambda: old_df
        }
        result = fetch_market_news(existing, params)

        assert "2024-01-10" in result
        assert "2024-01-15" in result

    def test_articles_spanning_multiple_dates(self, mock_client, params):
        raw = [
            _raw_article(1, pd.Timestamp("2024-01-15 10:00:00")),
            _raw_article(2, pd.Timestamp("2024-01-16 09:00:00")),
        ]
        mock_client.general_news.return_value = raw

        result = fetch_market_news({}, params)

        assert "2024-01-15" in result
        assert "2024-01-16" in result
        assert len(result["2024-01-15"]) == 1
        assert len(result["2024-01-16"]) == 1

    def test_missing_api_key_raises(self, mocker, params):
        env_without_key = {k: v for k, v in os.environ.items() if k != "FINNHUB_API_KEY"}
        mocker.patch.dict(os.environ, env_without_key, clear=True)
        with pytest.raises(KeyError):
            fetch_market_news({}, params)

    def test_multiple_categories_combined(self, mock_client, params):
        params["categories"] = ["general", "forex"]
        mock_client.general_news.side_effect = [
            _make_raw_articles(n=2, base_id=1000, base_date="2024-01-15"),
            _make_raw_articles(n=1, base_id=2000, base_date="2024-01-15"),
        ]
        result = fetch_market_news({}, params)

        assert len(result["2024-01-15"]) == 3
