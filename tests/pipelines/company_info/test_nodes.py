"""Unit tests for company_info nodes.

Network is disabled globally via --disable-socket.  All yfinance calls are
patched with pytest-mock.
"""

from __future__ import annotations

from collections.abc import Callable
from unittest.mock import MagicMock

import pandas as pd
import pytest

from rdd.pipelines.company_info.nodes import _extract_info, ingest_company_info

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
def base_params() -> dict:
    return {"refresh_days": 7}


class TestExtractInfo:
    def test_maps_known_fields(self) -> None:
        row = _extract_info("AAPL", _SAMPLE_INFO)

        assert row["ticker"] == "AAPL"
        assert row["name"] == "Apple Inc."
        assert row["sector"] == "Technology"
        assert row["industry"] == "Consumer Electronics"
        assert row["market_cap"] == 3_000_000_000_000
        assert row["employees"] == 161_000
        assert row["country"] == "United States"
        assert row["currency"] == "USD"
        assert row["exchange"] == "NMS"
        assert isinstance(row["fetched_at"], pd.Timestamp)

    def test_missing_fields_become_none(self) -> None:
        row = _extract_info("AAPL", {})

        assert row["name"] is None
        assert row["sector"] is None
        assert row["market_cap"] is None


class TestIngestCompanyInfo:
    def test_first_run_fetches_all_tickers(self, mocker, base_params) -> None:
        mock_ticker = MagicMock()
        mock_ticker.info = _SAMPLE_INFO
        mocker.patch(
            "rdd.pipelines.company_info.nodes.yf.Ticker", return_value=mock_ticker
        )

        result = ingest_company_info(["AAPL", "MSFT"], {}, base_params)

        assert "aapl" in result
        assert "msft" in result
        assert len(result["aapl"]) == 1
        assert result["aapl"]["ticker"].iloc[0] == "AAPL"

    def test_fresh_snapshot_is_not_refetched(
        self, mocker, base_params, company_info_df
    ) -> None:
        ticker = "AAPL"
        fresh_df = company_info_df.copy()
        fresh_df["fetched_at"] = pd.Timestamp.now("UTC").tz_convert(None)

        existing: dict[str, Callable[[], pd.DataFrame]] = {
            ticker.lower(): lambda: fresh_df
        }
        yf_spy = mocker.patch("rdd.pipelines.company_info.nodes.yf.Ticker")

        result = ingest_company_info([ticker], existing, base_params)

        yf_spy.assert_not_called()
        assert ticker.lower() in result

    def test_stale_snapshot_is_refetched(
        self, mocker, base_params, company_info_df
    ) -> None:
        ticker = "AAPL"
        stale_df = company_info_df.copy()
        stale_df["fetched_at"] = pd.Timestamp("2000-01-01")

        existing: dict[str, Callable[[], pd.DataFrame]] = {
            ticker.lower(): lambda: stale_df
        }
        mock_ticker = MagicMock()
        mock_ticker.info = _SAMPLE_INFO
        mocker.patch(
            "rdd.pipelines.company_info.nodes.yf.Ticker", return_value=mock_ticker
        )

        result = ingest_company_info([ticker], existing, base_params)

        assert result["aapl"]["sector"].iloc[0] == "Technology"

    def test_failed_fetch_is_skipped(self, mocker, base_params) -> None:
        mocker.patch(
            "rdd.pipelines.company_info.nodes.yf.Ticker",
            side_effect=RuntimeError("API error"),
        )

        result = ingest_company_info(["AAPL"], {}, base_params)

        assert result == {}

    def test_output_contains_expected_columns(self, mocker, base_params) -> None:
        mock_ticker = MagicMock()
        mock_ticker.info = _SAMPLE_INFO
        mocker.patch(
            "rdd.pipelines.company_info.nodes.yf.Ticker", return_value=mock_ticker
        )

        result = ingest_company_info(["AAPL"], {}, base_params)

        expected = {
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
        assert expected.issubset(result["aapl"].columns)
