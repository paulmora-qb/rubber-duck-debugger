"""Unit tests for data_ingestion nodes.

Network is disabled globally via --disable-socket.  All external calls
(requests.get, yf.download) are patched with pytest-mock.
"""

from __future__ import annotations

from collections.abc import Callable
from unittest.mock import MagicMock

import pandas as pd
import pytest

from rdd.pipelines.data_ingestion.stock_prices.nodes import (
    _wide_to_long,
    fetch_ticker_universe,
    ingest_ohlcv,
)

# ---------------------------------------------------------------------------
# HTML fixtures for Wikipedia mocks
# ---------------------------------------------------------------------------

_SP500_HTML = """
<html><body>
<table id="constituents">
<tr><th>Symbol</th><th>Security</th></tr>
<tr><td>AAPL</td><td>Apple Inc.</td></tr>
<tr><td>BRK.B</td><td>Berkshire Hathaway B</td></tr>
<tr><td>MSFT</td><td>Microsoft</td></tr>
</table>
</body></html>
"""

_NASDAQ100_HTML = """
<html><body>
<table>
<tr><th>Company</th><th>Ticker</th><th>Industry</th></tr>
<tr><td>Apple Inc.</td><td>AAPL</td><td>Technology</td></tr>
<tr><td>Alphabet</td><td>GOOGL</td><td>Technology</td></tr>
</table>
</body></html>
"""


def _mock_response(html: str) -> MagicMock:
    resp = MagicMock()
    resp.text = html
    resp.raise_for_status.return_value = None
    return resp


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_yf_download(tickers: list[str], dates: pd.DatetimeIndex) -> pd.DataFrame:
    """Build a minimal yfinance-style multi-ticker wide DataFrame."""
    close = 100.0
    data: dict[tuple[str, str], list[float]] = {}
    for ticker in tickers:
        data[("Open", ticker)] = [close * 1.005] * len(dates)
        data[("High", ticker)] = [close * 1.01] * len(dates)
        data[("Low", ticker)] = [close * 0.99] * len(dates)
        data[("Close", ticker)] = [close] * len(dates)
        data[("Adj Close", ticker)] = [close * 0.98] * len(dates)
        data[("Volume", ticker)] = [1_000_000.0] * len(dates)
    cols = pd.MultiIndex.from_tuples(data.keys(), names=["Price", "Ticker"])
    df = pd.DataFrame(list(data.values()), index=cols).T
    df.index = dates
    df.index.name = "Date"
    return df


@pytest.fixture
def base_params() -> dict:
    return {
        "start_date": "2023-01-01",
        "batch_size": 100,
        "index_sources": {"sp500": True, "nasdaq100": False},
    }


# ---------------------------------------------------------------------------
# fetch_ticker_universe
# ---------------------------------------------------------------------------


class TestFetchTickerUniverse:
    def test_sp500_only_returns_sorted_list(self, mocker, base_params) -> None:
        mocker.patch(
            "rdd.pipelines.data_ingestion.stock_prices.nodes.requests.get",
            return_value=_mock_response(_SP500_HTML),
        )
        result = fetch_ticker_universe(base_params)

        assert isinstance(result, list)
        assert result == sorted(result)
        assert "AAPL" in result
        assert "MSFT" in result

    def test_dot_normalisation(self, mocker, base_params) -> None:
        mocker.patch(
            "rdd.pipelines.data_ingestion.stock_prices.nodes.requests.get",
            return_value=_mock_response(_SP500_HTML),
        )
        result = fetch_ticker_universe(base_params)

        # BRK.B in the HTML → BRK-B in the output
        assert "BRK-B" in result
        assert "BRK.B" not in result

    def test_deduplication_across_sources(self, mocker) -> None:
        params = {
            "start_date": "2023-01-01",
            "batch_size": 100,
            "index_sources": {"sp500": True, "nasdaq100": True},
        }

        def _side_effect(url, **_kwargs):
            if "S%26P" in url:
                return _mock_response(_SP500_HTML)
            return _mock_response(_NASDAQ100_HTML)

        mocker.patch(
            "rdd.pipelines.data_ingestion.stock_prices.nodes.requests.get",
            side_effect=_side_effect,
        )

        result = fetch_ticker_universe(params)
        # AAPL appears in both sources — must appear exactly once
        assert result.count("AAPL") == 1


# ---------------------------------------------------------------------------
# _wide_to_long
# ---------------------------------------------------------------------------


class TestWideToLong:
    def test_multi_ticker_produces_long_format(self) -> None:
        tickers = ["AAPL", "MSFT"]
        dates = pd.date_range("2024-01-02", periods=3, freq="B")
        wide = _make_yf_download(tickers, dates)

        result = _wide_to_long(wide, tickers)

        assert set(result.columns) >= {
            "ticker",
            "date",
            "open",
            "high",
            "low",
            "close",
            "adj_close",
            "volume",
        }
        assert set(result["ticker"].unique()) == {"AAPL", "MSFT"}
        assert len(result) == len(tickers) * len(dates)

    def test_single_ticker_produces_long_format(self) -> None:
        tickers = ["AAPL"]
        dates = pd.date_range("2024-01-02", periods=3, freq="B")
        # Single-ticker yfinance download has plain (non-MultiIndex) columns
        df = pd.DataFrame(
            {
                "Open": [100.5] * 3,
                "High": [102.0] * 3,
                "Low": [98.0] * 3,
                "Close": [100.0] * 3,
                "Adj Close": [99.5] * 3,
                "Volume": [1_000_000.0] * 3,
            },
            index=dates,
        )
        df.index.name = "Date"

        result = _wide_to_long(df, tickers)

        assert "ticker" in result.columns
        assert result["ticker"].iloc[0] == "AAPL"
        assert len(result) == 3


# ---------------------------------------------------------------------------
# ingest_ohlcv
# ---------------------------------------------------------------------------


class TestIngestOHLCV:
    def test_first_run_downloads_from_start_date(self, mocker, base_params) -> None:
        tickers = ["AAPL", "MSFT"]
        dates = pd.date_range("2023-01-03", periods=3, freq="B")
        mock_dl = _make_yf_download(tickers, dates)

        mocker.patch(
            "rdd.pipelines.data_ingestion.stock_prices.nodes.yf.download",
            return_value=mock_dl,
        )

        result = ingest_ohlcv(tickers, {}, base_params)

        assert "aapl" in result
        assert "msft" in result
        assert len(result["aapl"]) == 3

    def test_incremental_resumes_from_last_date(
        self, mocker, base_params, ohlcv_df
    ) -> None:
        ticker = "AAPL"
        # Existing data ends 2024-01-05 — node should ask for data from 2024-01-06
        existing_df = ohlcv_df.copy()
        existing_df["ticker"] = ticker

        existing_ohlcv: dict[str, Callable[[], pd.DataFrame]] = {
            ticker.lower(): lambda: existing_df
        }

        new_dates = pd.date_range("2024-01-08", periods=2, freq="B")
        mock_dl = _make_yf_download([ticker], new_dates)
        dl_spy = mocker.patch(
            "rdd.pipelines.data_ingestion.stock_prices.nodes.yf.download",
            return_value=mock_dl,
        )

        result = ingest_ohlcv([ticker], existing_ohlcv, base_params)

        # Verify incremental start date was pushed past the existing max date
        call_kwargs = dl_spy.call_args.kwargs
        last_existing = pd.Timestamp(existing_df["date"].max())
        assert pd.Timestamp(call_kwargs["start"]) > last_existing

        # Output contains both old and new rows
        assert ticker.lower() in result
        assert len(result[ticker.lower()]) > len(existing_df)

    def test_up_to_date_skips_download(self, mocker, base_params, ohlcv_df) -> None:
        ticker = "AAPL"
        # Use a fixed weekday "today" to avoid weekend date_range edge cases
        # (date_range(end=saturday, freq='B') generates n-1 business days).
        fake_today = pd.Timestamp("2024-01-08")  # Monday
        mocker.patch.object(pd.Timestamp, "today", return_value=fake_today)

        up_to_date_df = ohlcv_df.copy()
        up_to_date_df["ticker"] = ticker
        up_to_date_df["date"] = pd.date_range(
            end=fake_today, periods=len(up_to_date_df), freq="B"
        )

        existing_ohlcv: dict[str, Callable[[], pd.DataFrame]] = {
            ticker.lower(): lambda: up_to_date_df
        }
        dl_spy = mocker.patch(
            "rdd.pipelines.data_ingestion.stock_prices.nodes.yf.download"
        )

        ingest_ohlcv([ticker], existing_ohlcv, base_params)

        dl_spy.assert_not_called()
