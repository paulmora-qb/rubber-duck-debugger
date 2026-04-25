"""Unit tests for finnhub_company_news nodes.

Network is disabled globally via --disable-socket.
The Finnhub client and requests.get are mocked at the module boundary.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from unittest.mock import MagicMock

import pandas as pd
import pytest

from rdd.pipelines.finnhub_company_news.nodes import _parse_articles, fetch_company_news
from tests.conftest import make_company_news_df

# ---------------------------------------------------------------------------
# Raw article fixture helpers (match real Finnhub company_news response shape)
# ---------------------------------------------------------------------------


def _raw_article(
    article_id: int = 1000,
    ticker: str = "AAPL",
    dt: pd.Timestamp | None = None,
) -> dict:
    if dt is None:
        dt = pd.Timestamp("2024-01-15 10:00:00")
    return {
        "id": article_id,
        "datetime": int(dt.timestamp()),
        "headline": f"Headline {article_id} about {ticker}",
        "summary": f"Summary paragraph for {ticker} article {article_id}.",
        "source": "Yahoo",
        "url": f"https://finance.yahoo.com/news/{ticker.lower()}-{article_id}",
        "image": "https://s.yimg.com/generic.png",
        "related": ticker,
        "category": "company",
    }


def _make_raw_articles(
    n: int = 3,
    ticker: str = "AAPL",
    base_id: int = 1000,
    base_date: str = "2024-01-15",
) -> list[dict]:
    base = pd.Timestamp(base_date)
    return [_raw_article(base_id + i, ticker, base + pd.Timedelta(hours=i)) for i in range(n)]


# ---------------------------------------------------------------------------
# _parse_articles
# ---------------------------------------------------------------------------


class TestParseArticles:
    def test_empty_list_returns_correct_columns(self):
        df = _parse_articles([], ticker="AAPL")
        assert df.empty
        assert set(df.columns) == {
            "article_id", "ticker", "datetime", "headline", "summary", "source", "url"
        }

    def test_ticker_is_stored(self):
        raw = [_raw_article(1, "MSFT")]
        df = _parse_articles(raw, ticker="MSFT")
        assert df.loc[0, "ticker"] == "MSFT"

    def test_related_field_is_dropped(self):
        raw = [_raw_article(1, "AAPL")]
        df = _parse_articles(raw, ticker="AAPL")
        assert "related" not in df.columns

    def test_image_field_is_dropped(self):
        raw = [_raw_article(1, "AAPL")]
        df = _parse_articles(raw, ticker="AAPL")
        assert "image" not in df.columns

    def test_category_field_is_dropped(self):
        raw = [_raw_article(1, "AAPL")]
        df = _parse_articles(raw, ticker="AAPL")
        assert "category" not in df.columns

    def test_datetime_converts_unix(self):
        unix_ts = 1705312800
        raw = [_raw_article(1, "AAPL", pd.Timestamp(unix_ts, unit="s"))]
        df = _parse_articles(raw, ticker="AAPL")
        assert df.loc[0, "datetime"] == pd.Timestamp(unix_ts, unit="s")

    def test_null_summary_becomes_none(self):
        raw = [_raw_article(1)]
        raw[0]["summary"] = None
        df = _parse_articles(raw, ticker="AAPL")
        assert df.loc[0, "summary"] is None

    def test_multiple_articles_parsed(self):
        raw = _make_raw_articles(n=5)
        df = _parse_articles(raw, ticker="AAPL")
        assert len(df) == 5
        assert list(df["article_id"]) == [1000, 1001, 1002, 1003, 1004]


# ---------------------------------------------------------------------------
# fetch_company_news
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


class TestFetchCompanyNews:
    @pytest.fixture
    def params(self) -> dict:
        return {"start_date": "2024-01-01", "sleep_seconds": 0}

    @pytest.fixture
    def mock_client(self, mocker):
        mock_cls = mocker.patch("rdd.pipelines.finnhub_company_news.nodes.finnhub.Client")
        return mock_cls.return_value

    @pytest.fixture(autouse=True)
    def set_api_key(self, mocker):
        mocker.patch.dict(os.environ, {"FINNHUB_API_KEY": "test_key"})

    def test_happy_path_writes_partition_per_ticker(self, mock_client, params):
        mock_client.company_news.return_value = _make_raw_articles(n=3, ticker="AAPL")
        result = fetch_company_news(["AAPL"], {}, params)

        assert "aapl" in result
        assert len(result["aapl"]) == 3

    def test_partition_key_is_lowercase(self, mock_client, params):
        mock_client.company_news.return_value = _make_raw_articles(n=2, ticker="MSFT")
        result = fetch_company_news(["MSFT"], {}, params)

        assert "msft" in result
        assert "MSFT" not in result

    def test_merges_with_existing_partition(self, mock_client, params):
        existing_df = make_company_news_df(n=2, ticker="AAPL", start_id=500)
        mock_client.company_news.return_value = _make_raw_articles(n=2, ticker="AAPL", base_id=1000)

        existing: dict[str, Callable[[], pd.DataFrame]] = {"aapl": lambda: existing_df}
        result = fetch_company_news(["AAPL"], existing, params)

        assert len(result["aapl"]) == 4
        assert set(result["aapl"]["article_id"]) == {500, 501, 1000, 1001}

    def test_deduplicates_by_article_id(self, mock_client, params):
        existing_df = make_company_news_df(n=2, ticker="AAPL", start_id=1000)
        mock_client.company_news.return_value = _make_raw_articles(n=2, ticker="AAPL", base_id=1000)

        existing: dict[str, Callable[[], pd.DataFrame]] = {"aapl": lambda: existing_df}
        result = fetch_company_news(["AAPL"], existing, params)

        assert len(result["aapl"]) == 2

    def test_up_to_date_ticker_skips_api_call(self, mock_client, params, mocker):
        fake_today = pd.Timestamp("2024-01-08")
        mocker.patch.object(pd.Timestamp, "today", return_value=fake_today)
        existing_df = make_company_news_df(n=3, ticker="AAPL", base_date="2024-01-08")

        existing: dict[str, Callable[[], pd.DataFrame]] = {"aapl": lambda: existing_df}
        fetch_company_news(["AAPL"], existing, params)

        mock_client.company_news.assert_not_called()

    def test_empty_api_response_skips_ticker(self, mock_client, params):
        mock_client.company_news.return_value = []
        result = fetch_company_news(["AAPL"], {}, params)
        assert "aapl" not in result

    def test_api_failure_skips_ticker(self, mock_client, params):
        mock_client.company_news.side_effect = Exception("API error")
        result = fetch_company_news(["AAPL"], {}, params)
        assert "aapl" not in result

    def test_api_failure_preserves_existing(self, mock_client, params):
        existing_df = make_company_news_df(n=3, ticker="AAPL")
        mock_client.company_news.side_effect = Exception("API error")

        existing: dict[str, Callable[[], pd.DataFrame]] = {"aapl": lambda: existing_df}
        result = fetch_company_news(["AAPL"], existing, params)

        assert "aapl" in result
        assert len(result["aapl"]) == 3

    def test_multiple_tickers_processed(self, mock_client, params):
        mock_client.company_news.side_effect = [
            _make_raw_articles(n=2, ticker="AAPL"),
            _make_raw_articles(n=3, ticker="MSFT", base_id=2000),
        ]
        result = fetch_company_news(["AAPL", "MSFT"], {}, params)

        assert "aapl" in result
        assert "msft" in result
        assert len(result["aapl"]) == 2
        assert len(result["msft"]) == 3

    def test_missing_api_key_raises(self, mocker, params):
        env = {k: v for k, v in os.environ.items() if k != "FINNHUB_API_KEY"}
        mocker.patch.dict(os.environ, env, clear=True)
        with pytest.raises(KeyError):
            fetch_company_news(["AAPL"], {}, params)

    def test_incremental_start_date_is_after_existing_max(self, mock_client, params):
        existing_df = make_company_news_df(n=3, ticker="AAPL", base_date="2024-01-10")
        mock_client.company_news.return_value = []

        existing: dict[str, Callable[[], pd.DataFrame]] = {"aapl": lambda: existing_df}
        fetch_company_news(["AAPL"], existing, params)

        call_kwargs = mock_client.company_news.call_args
        from_date = pd.Timestamp(call_kwargs.kwargs["_from"])
        assert from_date > pd.Timestamp("2024-01-10")
