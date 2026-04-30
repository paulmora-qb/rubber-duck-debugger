"""Unit tests for earnings_history nodes.

Network is disabled globally via --disable-socket.  All yfinance calls are
patched with pytest-mock.
"""

from __future__ import annotations

from collections.abc import Callable
from unittest.mock import MagicMock, PropertyMock

import pandas as pd
import pytest

from rdd.pipelines.data_ingestion.earnings_history.nodes import (
    _fetch_earnings_history,
    ingest_earnings_history,
)

_FRESH_TS = pd.Timestamp.now("UTC").tz_convert(None)
_STALE_TS = pd.Timestamp("2000-01-01")

# Sample earnings_history DataFrame as returned by yfinance (index = Earnings Date)
_EARNINGS_INDEX = pd.DatetimeIndex(
    [pd.Timestamp("2024-09-30"), pd.Timestamp("2024-06-30")],
    name="Earnings Date",
)
_EARNINGS_RAW = pd.DataFrame(
    {
        "EPS Estimate": [1.60, 1.42],
        "Reported EPS": [1.64, 1.45],
        "Surprise(%)": [2.5, 2.11],
    },
    index=_EARNINGS_INDEX,
)


@pytest.fixture
def base_params() -> dict:
    return {"refresh_days": 30}


def _make_mock_ticker(earnings_df: pd.DataFrame = _EARNINGS_RAW) -> MagicMock:
    mock = MagicMock()
    type(mock).earnings_history = PropertyMock(return_value=earnings_df)
    return mock


def _make_fresh_df(ticker: str = "AAPL") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": ticker,
                "earnings_date": pd.Timestamp("2024-09-30"),
                "eps_estimate": 1.60,
                "reported_eps": 1.64,
                "surprise_pct": 2.5,
                "fetched_at": _FRESH_TS,
            }
        ]
    )


def _make_stale_df(ticker: str = "AAPL") -> pd.DataFrame:
    df = _make_fresh_df(ticker)
    df["fetched_at"] = _STALE_TS
    return df


class TestFetchEarningsHistory:
    def test_returns_dataframe_with_expected_columns(self, mocker) -> None:
        mocker.patch(
            "rdd.pipelines.data_ingestion.earnings_history.nodes.yf.Ticker",
            return_value=_make_mock_ticker(),
        )

        df = _fetch_earnings_history("AAPL")

        assert df is not None
        assert "ticker" in df.columns
        assert df["ticker"].iloc[0] == "AAPL"
        assert "earnings_date" in df.columns
        assert "eps_estimate" in df.columns
        assert "reported_eps" in df.columns
        assert "surprise_pct" in df.columns
        assert "fetched_at" in df.columns

    def test_earnings_date_is_tz_naive(self, mocker) -> None:
        mocker.patch(
            "rdd.pipelines.data_ingestion.earnings_history.nodes.yf.Ticker",
            return_value=_make_mock_ticker(),
        )

        df = _fetch_earnings_history("AAPL")

        assert df is not None
        assert df["earnings_date"].dt.tz is None

    def test_correct_number_of_rows(self, mocker) -> None:
        mocker.patch(
            "rdd.pipelines.data_ingestion.earnings_history.nodes.yf.Ticker",
            return_value=_make_mock_ticker(),
        )

        df = _fetch_earnings_history("AAPL")

        assert df is not None
        assert len(df) == 2

    def test_numeric_columns_are_float(self, mocker) -> None:
        mocker.patch(
            "rdd.pipelines.data_ingestion.earnings_history.nodes.yf.Ticker",
            return_value=_make_mock_ticker(),
        )

        df = _fetch_earnings_history("AAPL")

        assert df is not None
        assert df["eps_estimate"].dtype == float
        assert df["reported_eps"].dtype == float
        assert df["surprise_pct"].dtype == float

    def test_exception_returns_none(self, mocker) -> None:
        mocker.patch(
            "rdd.pipelines.data_ingestion.earnings_history.nodes.yf.Ticker",
            side_effect=RuntimeError("API error"),
        )

        df = _fetch_earnings_history("AAPL")

        assert df is None

    def test_none_earnings_history_returns_none(self, mocker) -> None:
        mock = MagicMock()
        type(mock).earnings_history = PropertyMock(return_value=None)
        mocker.patch(
            "rdd.pipelines.data_ingestion.earnings_history.nodes.yf.Ticker",
            return_value=mock,
        )

        df = _fetch_earnings_history("AAPL")

        assert df is None

    def test_empty_earnings_history_returns_none(self, mocker) -> None:
        mock = MagicMock()
        type(mock).earnings_history = PropertyMock(return_value=pd.DataFrame())
        mocker.patch(
            "rdd.pipelines.data_ingestion.earnings_history.nodes.yf.Ticker",
            return_value=mock,
        )

        df = _fetch_earnings_history("AAPL")

        assert df is None


class TestIngestEarningsHistory:
    def test_first_run_fetches_all_tickers(self, mocker, base_params) -> None:
        mocker.patch(
            "rdd.pipelines.data_ingestion.earnings_history.nodes.yf.Ticker",
            return_value=_make_mock_ticker(),
        )

        result = ingest_earnings_history(["AAPL", "MSFT"], {}, base_params)

        assert "aapl" in result
        assert "msft" in result

    def test_fresh_data_not_refetched(self, mocker, base_params) -> None:
        fresh_df = _make_fresh_df()
        existing: dict[str, Callable[[], pd.DataFrame]] = {"aapl": lambda: fresh_df}
        yf_spy = mocker.patch(
            "rdd.pipelines.data_ingestion.earnings_history.nodes.yf.Ticker"
        )

        result = ingest_earnings_history(["AAPL"], existing, base_params)

        yf_spy.assert_not_called()
        assert "aapl" in result

    def test_stale_data_triggers_refetch(self, mocker, base_params) -> None:
        stale_df = _make_stale_df()
        existing: dict[str, Callable[[], pd.DataFrame]] = {"aapl": lambda: stale_df}
        mocker.patch(
            "rdd.pipelines.data_ingestion.earnings_history.nodes.yf.Ticker",
            return_value=_make_mock_ticker(),
        )

        result = ingest_earnings_history(["AAPL"], existing, base_params)

        assert "aapl" in result
        # Should have the freshly fetched data (2 rows)
        assert len(result["aapl"]) == 2

    def test_failed_fetch_is_skipped_gracefully(self, mocker, base_params) -> None:
        mocker.patch(
            "rdd.pipelines.data_ingestion.earnings_history.nodes.yf.Ticker",
            side_effect=RuntimeError("API error"),
        )

        result = ingest_earnings_history(["AAPL"], {}, base_params)

        assert result == {}

    def test_failed_existing_load_triggers_refetch(self, mocker, base_params) -> None:
        def bad_loader() -> pd.DataFrame:
            raise OSError("disk error")

        existing: dict[str, Callable[[], pd.DataFrame]] = {"aapl": bad_loader}
        mocker.patch(
            "rdd.pipelines.data_ingestion.earnings_history.nodes.yf.Ticker",
            return_value=_make_mock_ticker(),
        )

        result = ingest_earnings_history(["AAPL"], existing, base_params)

        assert "aapl" in result

    def test_output_keys_are_lowercase(self, mocker, base_params) -> None:
        mocker.patch(
            "rdd.pipelines.data_ingestion.earnings_history.nodes.yf.Ticker",
            return_value=_make_mock_ticker(),
        )

        result = ingest_earnings_history(["AAPL", "MSFT"], {}, base_params)

        for key in result:
            assert key == key.lower()

    def test_fetched_at_is_set(self, mocker, base_params) -> None:
        mocker.patch(
            "rdd.pipelines.data_ingestion.earnings_history.nodes.yf.Ticker",
            return_value=_make_mock_ticker(),
        )

        result = ingest_earnings_history(["AAPL"], {}, base_params)

        assert "aapl" in result
        df = result["aapl"]
        assert "fetched_at" in df.columns
        assert isinstance(df["fetched_at"].iloc[0], pd.Timestamp)
