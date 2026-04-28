"""Unit tests for analyst_consensus nodes.

Network is disabled globally via --disable-socket.  All yfinance calls are
patched with pytest-mock.
"""

from __future__ import annotations

from collections.abc import Callable
from unittest.mock import MagicMock

import pandas as pd
import pytest

from rdd.pipelines.analyst_consensus.nodes import (
    _fetch_analyst_consensus,
    ingest_analyst_consensus,
)

_FRESH_TS = pd.Timestamp.now("UTC").tz_convert(None)
_STALE_TS = pd.Timestamp("2000-01-01")

_MOCK_INFO = {
    "recommendationKey": "buy",
    "recommendationMean": 1.8,
    "numberOfAnalystOpinions": 42,
    "targetMeanPrice": 210.0,
    "targetHighPrice": 250.0,
    "targetLowPrice": 180.0,
    "targetMedianPrice": 205.0,
    "currentPrice": 195.0,
}


@pytest.fixture
def base_params() -> dict:
    return {"refresh_days": 1}


def _make_mock_ticker(info: dict = _MOCK_INFO) -> MagicMock:
    mock = MagicMock()
    mock.info = info
    return mock


def _make_fresh_df(ticker: str = "AAPL") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": ticker,
                "recommendation_key": "buy",
                "fetched_at": _FRESH_TS,
            }
        ]
    )


def _make_stale_df(ticker: str = "AAPL") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": ticker,
                "recommendation_key": "hold",
                "fetched_at": _STALE_TS,
            }
        ]
    )


class TestFetchAnalystConsensus:
    def test_returns_dataframe_with_expected_columns(self, mocker) -> None:
        mocker.patch(
            "rdd.pipelines.analyst_consensus.nodes.yf.Ticker",
            return_value=_make_mock_ticker(),
        )

        df = _fetch_analyst_consensus("AAPL")

        assert df is not None
        assert "ticker" in df.columns
        assert df["ticker"].iloc[0] == "AAPL"
        assert "recommendation_key" in df.columns
        assert df["recommendation_key"].iloc[0] == "buy"
        assert "recommendation_mean" in df.columns
        assert "analyst_count" in df.columns
        assert "target_mean_price" in df.columns
        assert "fetched_at" in df.columns

    def test_fetched_at_is_timestamp(self, mocker) -> None:
        mocker.patch(
            "rdd.pipelines.analyst_consensus.nodes.yf.Ticker",
            return_value=_make_mock_ticker(),
        )

        df = _fetch_analyst_consensus("AAPL")

        assert df is not None
        assert isinstance(df["fetched_at"].iloc[0], pd.Timestamp)

    def test_exception_returns_none(self, mocker) -> None:
        mocker.patch(
            "rdd.pipelines.analyst_consensus.nodes.yf.Ticker",
            side_effect=RuntimeError("API error"),
        )

        df = _fetch_analyst_consensus("AAPL")

        assert df is None

    def test_missing_fields_produce_none_values(self, mocker) -> None:
        mocker.patch(
            "rdd.pipelines.analyst_consensus.nodes.yf.Ticker",
            return_value=_make_mock_ticker({}),  # empty info
        )

        df = _fetch_analyst_consensus("AAPL")

        assert df is not None
        assert df["recommendation_key"].iloc[0] is None


class TestIngestAnalystConsensus:
    def test_first_run_fetches_all_tickers(self, mocker, base_params) -> None:
        mocker.patch(
            "rdd.pipelines.analyst_consensus.nodes.yf.Ticker",
            return_value=_make_mock_ticker(),
        )

        result = ingest_analyst_consensus(["AAPL", "MSFT"], {}, base_params)

        assert "aapl" in result
        assert "msft" in result

    def test_fresh_data_not_refetched(self, mocker, base_params) -> None:
        fresh_df = _make_fresh_df()
        existing: dict[str, Callable[[], pd.DataFrame]] = {"aapl": lambda: fresh_df}
        yf_spy = mocker.patch("rdd.pipelines.analyst_consensus.nodes.yf.Ticker")

        result = ingest_analyst_consensus(["AAPL"], existing, base_params)

        yf_spy.assert_not_called()
        assert "aapl" in result

    def test_stale_data_triggers_refetch(self, mocker, base_params) -> None:
        stale_df = _make_stale_df()
        existing: dict[str, Callable[[], pd.DataFrame]] = {"aapl": lambda: stale_df}
        mocker.patch(
            "rdd.pipelines.analyst_consensus.nodes.yf.Ticker",
            return_value=_make_mock_ticker(),
        )

        result = ingest_analyst_consensus(["AAPL"], existing, base_params)

        assert "aapl" in result
        assert result["aapl"]["recommendation_key"].iloc[0] == "buy"

    def test_missing_data_triggers_fetch(self, mocker, base_params) -> None:
        mocker.patch(
            "rdd.pipelines.analyst_consensus.nodes.yf.Ticker",
            return_value=_make_mock_ticker(),
        )

        result = ingest_analyst_consensus(["AAPL"], {}, base_params)

        assert "aapl" in result

    def test_failed_fetch_is_skipped_gracefully(self, mocker, base_params) -> None:
        mocker.patch(
            "rdd.pipelines.analyst_consensus.nodes.yf.Ticker",
            side_effect=RuntimeError("API error"),
        )

        result = ingest_analyst_consensus(["AAPL"], {}, base_params)

        assert result == {}

    def test_failed_existing_load_triggers_refetch(self, mocker, base_params) -> None:
        def bad_loader() -> pd.DataFrame:
            raise OSError("disk error")

        existing: dict[str, Callable[[], pd.DataFrame]] = {"aapl": bad_loader}
        mocker.patch(
            "rdd.pipelines.analyst_consensus.nodes.yf.Ticker",
            return_value=_make_mock_ticker(),
        )

        result = ingest_analyst_consensus(["AAPL"], existing, base_params)

        assert "aapl" in result

    def test_output_keys_are_lowercase(self, mocker, base_params) -> None:
        mocker.patch(
            "rdd.pipelines.analyst_consensus.nodes.yf.Ticker",
            return_value=_make_mock_ticker(),
        )

        result = ingest_analyst_consensus(["AAPL", "MSFT"], {}, base_params)

        for key in result:
            assert key == key.lower()
