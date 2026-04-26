"""Unit tests for company_news nodes.

Network is disabled globally via --disable-socket.  All Finnhub calls are
patched with pytest-mock.  FINNHUB_API_KEY is injected via monkeypatch.
"""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd
import pytest

from rdd.pipelines.company_news.nodes import _articles_to_df, ingest_company_news


def _make_articles(ticker: str, n: int = 3, base_ts: int | None = None) -> list[dict]:
    if base_ts is None:
        base_ts = int(pd.Timestamp("2024-01-02").timestamp())
    return [
        {
            "datetime": base_ts + i * 86_400,
            "headline": f"Headline {i}",
            "summary": f"Summary {i}",
            "source": "Reuters",
            "url": f"https://example.com/{ticker}/{i}",
            "category": "company news",
        }
        for i in range(n)
    ]


@pytest.fixture(autouse=True)
def set_api_key(monkeypatch) -> None:
    monkeypatch.setenv("FINNHUB_API_KEY", "test-key")


@pytest.fixture
def base_params() -> dict:
    return {"start_date": "2023-01-01"}


class TestArticlesToDf:
    def test_returns_expected_columns(self) -> None:
        articles = _make_articles("AAPL", n=2)
        df = _articles_to_df("AAPL", articles)

        assert set(df.columns) == {
            "ticker",
            "published_at",
            "headline",
            "summary",
            "source",
            "url",
            "category",
        }
        assert len(df) == 2
        assert df["ticker"].iloc[0] == "AAPL"

    def test_empty_articles_returns_empty_df(self) -> None:
        df = _articles_to_df("AAPL", [])
        assert df.empty
        assert "ticker" in df.columns

    def test_unix_timestamp_converted_to_timestamp(self) -> None:
        ts = int(pd.Timestamp("2024-06-01").timestamp())
        articles = [
            {
                "datetime": ts,
                "headline": "h",
                "summary": "s",
                "source": "s",
                "url": "u",
                "category": "c",
            }
        ]
        df = _articles_to_df("AAPL", articles)
        assert isinstance(df["published_at"].iloc[0], pd.Timestamp)


class TestIngestCompanyNews:
    def test_first_run_fetches_articles(self, mocker, base_params) -> None:
        articles = _make_articles("AAPL", n=3)
        mock_client = mocker.MagicMock()
        mock_client.company_news.return_value = articles
        mocker.patch(
            "rdd.pipelines.company_news.nodes.finnhub.Client", return_value=mock_client
        )

        result = ingest_company_news(["AAPL"], {}, base_params)

        assert "aapl" in result
        assert len(result["aapl"]) == 3

    def test_incremental_fetches_from_last_date(
        self, mocker, base_params, company_news_df
    ) -> None:
        ticker = "AAPL"
        existing_df = company_news_df.copy()
        existing_df["ticker"] = ticker
        last_date = pd.Timestamp(existing_df["published_at"].max())

        existing: dict[str, Callable[[], pd.DataFrame]] = {
            ticker.lower(): lambda: existing_df
        }

        new_ts = int((last_date + pd.Timedelta(days=2)).timestamp())
        new_articles = _make_articles(ticker, n=2, base_ts=new_ts)
        mock_client = mocker.MagicMock()
        mock_client.company_news.return_value = new_articles
        mocker.patch(
            "rdd.pipelines.company_news.nodes.finnhub.Client", return_value=mock_client
        )

        result = ingest_company_news([ticker], existing, base_params)

        call_kwargs = mock_client.company_news.call_args
        fetch_from = pd.Timestamp(call_kwargs[1]["_from"])
        assert fetch_from > last_date

        assert len(result[ticker.lower()]) > len(existing_df)

    def test_up_to_date_ticker_skips_api_call(
        self, mocker, base_params, company_news_df
    ) -> None:
        ticker = "AAPL"
        fresh_df = company_news_df.copy()
        fresh_df["ticker"] = ticker
        fresh_df["published_at"] = pd.date_range(
            end=pd.Timestamp.now("UTC").tz_convert(None),
            periods=len(fresh_df),
            freq="D",
        )

        existing: dict[str, Callable[[], pd.DataFrame]] = {
            ticker.lower(): lambda: fresh_df
        }
        mock_client = mocker.MagicMock()
        mocker.patch(
            "rdd.pipelines.company_news.nodes.finnhub.Client", return_value=mock_client
        )

        ingest_company_news([ticker], existing, base_params)

        mock_client.company_news.assert_not_called()

    def test_failed_api_call_is_skipped(self, mocker, base_params) -> None:
        mock_client = mocker.MagicMock()
        mock_client.company_news.side_effect = RuntimeError("API error")
        mocker.patch(
            "rdd.pipelines.company_news.nodes.finnhub.Client", return_value=mock_client
        )

        result = ingest_company_news(["AAPL"], {}, base_params)

        assert result == {}

    def test_existing_data_preserved_on_api_failure(
        self, mocker, base_params, company_news_df
    ) -> None:
        ticker = "AAPL"
        existing_df = company_news_df.copy()
        existing_df["ticker"] = ticker

        existing: dict[str, Callable[[], pd.DataFrame]] = {
            ticker.lower(): lambda: existing_df
        }
        mock_client = mocker.MagicMock()
        mock_client.company_news.side_effect = RuntimeError("API error")
        mocker.patch(
            "rdd.pipelines.company_news.nodes.finnhub.Client", return_value=mock_client
        )

        result = ingest_company_news([ticker], existing, base_params)

        assert ticker.lower() in result
        assert len(result[ticker.lower()]) == len(existing_df)

    def test_deduplication_on_merge(self, mocker, base_params, company_news_df) -> None:
        ticker = "AAPL"
        existing_df = company_news_df.copy()
        existing_df["ticker"] = ticker

        existing: dict[str, Callable[[], pd.DataFrame]] = {
            ticker.lower(): lambda: existing_df
        }

        # Return the same articles as existing — they should be deduplicated
        same_ts = int(existing_df["published_at"].iloc[0].timestamp())
        duplicate_articles = [
            {
                "datetime": same_ts,
                "headline": existing_df["headline"].iloc[0],
                "summary": "x",
                "source": "x",
                "url": "x",
                "category": "x",
            }
        ]
        mock_client = mocker.MagicMock()
        mock_client.company_news.return_value = duplicate_articles
        mocker.patch(
            "rdd.pipelines.company_news.nodes.finnhub.Client", return_value=mock_client
        )

        result = ingest_company_news([ticker], existing, base_params)

        assert len(result[ticker.lower()]) == len(existing_df)
