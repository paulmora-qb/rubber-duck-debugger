"""Unit tests for company_financials nodes.

Network is disabled globally via --disable-socket.  All yfinance calls are
patched with pytest-mock.
"""

from __future__ import annotations

from collections.abc import Callable
from unittest.mock import MagicMock, PropertyMock

import pandas as pd
import pytest

from rdd.pipelines.company_financials.nodes import (
    _ALL_METRIC_COLS,
    _BALANCE_FIELDS,
    _INCOME_FIELDS,
    _extract_statement,
    _fetch_financials,
    _merge_statements,
    ingest_company_financials,
)

_DATE = pd.Timestamp("2024-09-30")
_DATE2 = pd.Timestamp("2024-06-30")

_INCOME_RAW = pd.DataFrame(
    {
        _DATE: {"Total Revenue": 100_000, "Net Income": 20_000, "EBITDA": 30_000},
        _DATE2: {"Total Revenue": 90_000, "Net Income": 18_000, "EBITDA": 27_000},
    }
)

_BALANCE_RAW = pd.DataFrame(
    {
        _DATE: {"Total Assets": 300_000, "Total Debt": 50_000},
        _DATE2: {"Total Assets": 280_000, "Total Debt": 55_000},
    }
)

_CASHFLOW_RAW = pd.DataFrame(
    {
        _DATE: {"Free Cash Flow": 25_000, "Operating Cash Flow": 35_000},
        _DATE2: {"Free Cash Flow": 22_000, "Operating Cash Flow": 31_000},
    }
)


@pytest.fixture
def base_params() -> dict:
    return {"refresh_days": 7}


def _make_mock_ticker(
    quarterly: bool | None = None,
    income: pd.DataFrame = _INCOME_RAW,
    balance: pd.DataFrame = _BALANCE_RAW,
    cashflow: pd.DataFrame = _CASHFLOW_RAW,
) -> MagicMock:
    """Build a mock yf.Ticker. quarterly=None sets both quarterly and annual props."""
    mock = MagicMock()
    if quarterly is None or quarterly:
        type(mock).quarterly_financials = PropertyMock(return_value=income)
        type(mock).quarterly_balance_sheet = PropertyMock(return_value=balance)
        type(mock).quarterly_cashflow = PropertyMock(return_value=cashflow)
    if quarterly is None or not quarterly:
        type(mock).financials = PropertyMock(return_value=income)
        type(mock).balance_sheet = PropertyMock(return_value=balance)
        type(mock).cashflow = PropertyMock(return_value=cashflow)
    return mock


class TestExtractStatement:
    def test_transposes_and_renames(self) -> None:
        df = _extract_statement(_INCOME_RAW, _INCOME_FIELDS)

        assert "period_end" in df.columns
        assert "total_revenue" in df.columns
        assert "net_income" in df.columns
        assert len(df) == 2

    def test_empty_input_returns_empty(self) -> None:
        df = _extract_statement(pd.DataFrame(), _INCOME_FIELDS)

        assert df.empty

    def test_none_input_returns_empty(self) -> None:
        df = _extract_statement(None, _INCOME_FIELDS)

        assert df.empty

    def test_period_end_is_tz_naive(self) -> None:
        df = _extract_statement(_INCOME_RAW, _INCOME_FIELDS)

        assert df["period_end"].dt.tz is None


class TestMergeStatements:
    def test_merges_on_period_end(self) -> None:
        income = _extract_statement(_INCOME_RAW, _INCOME_FIELDS)
        balance = _extract_statement(_BALANCE_RAW, _BALANCE_FIELDS)
        merged = _merge_statements(income, balance)

        assert "total_revenue" in merged.columns
        assert "total_assets" in merged.columns
        assert len(merged) == 2


class TestFetchFinancials:
    def test_quarterly_fetch_returns_dataframe(self, mocker) -> None:
        mock = _make_mock_ticker(quarterly=True)
        mocker.patch(
            "rdd.pipelines.company_financials.nodes.yf.Ticker",
            return_value=mock,
        )

        df = _fetch_financials("AAPL", quarterly=True)

        assert df is not None
        assert "ticker" in df.columns
        assert df["ticker"].iloc[0] == "AAPL"
        assert "period_end" in df.columns
        assert "total_revenue" in df.columns

    def test_annual_fetch_returns_dataframe(self, mocker) -> None:
        mock = _make_mock_ticker(quarterly=False)
        mocker.patch(
            "rdd.pipelines.company_financials.nodes.yf.Ticker",
            return_value=mock,
        )

        df = _fetch_financials("AAPL", quarterly=False)

        assert df is not None
        assert len(df) == 2

    def test_exception_returns_none(self, mocker) -> None:
        mocker.patch(
            "rdd.pipelines.company_financials.nodes.yf.Ticker",
            side_effect=RuntimeError("API error"),
        )

        df = _fetch_financials("AAPL", quarterly=True)

        assert df is None

    def test_all_metric_cols_present(self, mocker) -> None:
        mock = _make_mock_ticker(quarterly=True)
        mocker.patch(
            "rdd.pipelines.company_financials.nodes.yf.Ticker",
            return_value=mock,
        )

        df = _fetch_financials("AAPL", quarterly=True)

        assert df is not None
        for col in _ALL_METRIC_COLS:
            assert col in df.columns, f"Missing column: {col}"

    def test_fetched_at_is_set(self, mocker) -> None:
        mock = _make_mock_ticker(quarterly=True)
        mocker.patch(
            "rdd.pipelines.company_financials.nodes.yf.Ticker",
            return_value=mock,
        )

        df = _fetch_financials("AAPL", quarterly=True)

        assert df is not None
        assert "fetched_at" in df.columns
        assert isinstance(df["fetched_at"].iloc[0], pd.Timestamp)


class TestIngestCompanyFinancials:
    def test_first_run_fetches_all_tickers(self, mocker, base_params) -> None:
        mock = _make_mock_ticker(quarterly=None)
        mocker.patch(
            "rdd.pipelines.company_financials.nodes.yf.Ticker",
            return_value=mock,
        )

        quarterly, annual = ingest_company_financials(
            ["AAPL", "MSFT"], {}, {}, base_params
        )

        assert "aapl" in quarterly
        assert "msft" in quarterly
        assert "aapl" in annual
        assert "msft" in annual

    def test_fresh_data_not_refetched(self, mocker, base_params) -> None:
        fresh = pd.DataFrame(
            {
                "ticker": ["AAPL"],
                "period_end": [pd.Timestamp("2024-09-30")],
                "fetched_at": [pd.Timestamp.now("UTC").tz_convert(None)],
            }
        )

        existing_q: dict[str, Callable[[], pd.DataFrame]] = {"aapl": lambda: fresh}
        existing_a: dict[str, Callable[[], pd.DataFrame]] = {"aapl": lambda: fresh}
        yf_spy = mocker.patch("rdd.pipelines.company_financials.nodes.yf.Ticker")

        ingest_company_financials(["AAPL"], existing_q, existing_a, base_params)

        yf_spy.assert_not_called()

    def test_stale_data_is_refetched(self, mocker, base_params) -> None:
        stale = pd.DataFrame(
            {
                "ticker": ["AAPL"],
                "period_end": [pd.Timestamp("2024-09-30")],
                "fetched_at": [pd.Timestamp("2000-01-01")],
            }
        )
        existing_q: dict[str, Callable[[], pd.DataFrame]] = {"aapl": lambda: stale}
        existing_a: dict[str, Callable[[], pd.DataFrame]] = {"aapl": lambda: stale}

        mock = _make_mock_ticker(quarterly=None)
        mocker.patch(
            "rdd.pipelines.company_financials.nodes.yf.Ticker",
            return_value=mock,
        )

        quarterly, _ = ingest_company_financials(
            ["AAPL"], existing_q, existing_a, base_params
        )

        assert "aapl" in quarterly
        assert quarterly["aapl"]["ticker"].iloc[0] == "AAPL"

    def test_failed_fetch_is_skipped(self, mocker, base_params) -> None:
        mocker.patch(
            "rdd.pipelines.company_financials.nodes.yf.Ticker",
            side_effect=RuntimeError("API error"),
        )

        quarterly, annual = ingest_company_financials(["AAPL"], {}, {}, base_params)

        assert quarterly == {}
        assert annual == {}
