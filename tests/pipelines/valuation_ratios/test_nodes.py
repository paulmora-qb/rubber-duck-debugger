"""Unit tests for valuation_ratios nodes.

Network is disabled globally via --disable-socket.
No external calls are made — ratios are computed from in-memory DataFrames.
"""

from __future__ import annotations

import math
from collections.abc import Callable

import pandas as pd
import pytest

from rdd.pipelines.valuation_ratios.nodes import (
    _RATIO_COLS,
    _compute_ratios,
    _safe_div,
    compute_valuation_ratios,
)

_DATE = pd.Timestamp("2024-09-30")

# A realistic quarterly financials row
_QUARTER_ROW = pd.Series(
    {
        "ticker": "AAPL",
        "period_end": _DATE,
        "total_revenue": 100_000.0,
        "gross_profit": 45_000.0,
        "operating_income": 30_000.0,
        "net_income": 25_000.0,
        "ebitda": 35_000.0,
        "total_assets": 500_000.0,
        "equity": 80_000.0,
        "total_debt": 50_000.0,
        "cash_and_equivalents": 20_000.0,
        "free_cash_flow": 22_000.0,
    }
)

_MARKET_CAP = 2_000_000.0


def _make_fin_loader(series: pd.Series) -> Callable[[], pd.DataFrame]:
    df = pd.DataFrame([series])
    return lambda: df


def _make_info_loader(market_cap: float | None) -> Callable[[], pd.DataFrame]:
    df = pd.DataFrame(
        [{"ticker": "AAPL", "market_cap": market_cap, "fetched_at": pd.Timestamp.now()}]
    )
    return lambda: df


class TestSafeDiv:
    def test_normal_division(self) -> None:
        assert _safe_div(10.0, 2.0) == pytest.approx(5.0)

    def test_zero_denominator_returns_nan(self) -> None:
        assert math.isnan(_safe_div(10.0, 0.0))

    def test_none_numerator_returns_nan(self) -> None:
        assert math.isnan(_safe_div(None, 2.0))

    def test_none_denominator_returns_nan(self) -> None:
        assert math.isnan(_safe_div(10.0, None))

    def test_nan_numerator_returns_nan(self) -> None:
        assert math.isnan(_safe_div(float("nan"), 2.0))


class TestComputeRatios:
    def test_pe_ratio_correct(self) -> None:
        row = _compute_ratios("AAPL", _QUARTER_ROW, _MARKET_CAP)
        # pe_ratio = market_cap / (net_income * 4) = 2_000_000 / 100_000 = 20.0
        assert row["pe_ratio"] == pytest.approx(20.0)

    def test_pb_ratio_correct(self) -> None:
        row = _compute_ratios("AAPL", _QUARTER_ROW, _MARKET_CAP)
        # pb_ratio = market_cap / equity = 2_000_000 / 80_000 = 25.0
        assert row["pb_ratio"] == pytest.approx(25.0)

    def test_ev_ebitda_correct(self) -> None:
        row = _compute_ratios("AAPL", _QUARTER_ROW, _MARKET_CAP)
        # ev = 2_000_000 + 50_000 - 20_000 = 2_030_000
        # ev_ebitda = 2_030_000 / (35_000 * 4) = 2_030_000 / 140_000 ≈ 14.5
        assert row["ev_ebitda"] == pytest.approx(2_030_000 / 140_000)

    def test_gross_margin_correct(self) -> None:
        row = _compute_ratios("AAPL", _QUARTER_ROW, _MARKET_CAP)
        # gross_margin = 45_000 / 100_000 = 0.45
        assert row["gross_margin"] == pytest.approx(0.45)

    def test_operating_margin_correct(self) -> None:
        row = _compute_ratios("AAPL", _QUARTER_ROW, _MARKET_CAP)
        assert row["operating_margin"] == pytest.approx(0.30)

    def test_net_margin_correct(self) -> None:
        row = _compute_ratios("AAPL", _QUARTER_ROW, _MARKET_CAP)
        assert row["net_margin"] == pytest.approx(0.25)

    def test_roe_correct(self) -> None:
        row = _compute_ratios("AAPL", _QUARTER_ROW, _MARKET_CAP)
        # roe = (25_000 * 4) / 80_000 = 1.25
        assert row["roe"] == pytest.approx(1.25)

    def test_roa_correct(self) -> None:
        row = _compute_ratios("AAPL", _QUARTER_ROW, _MARKET_CAP)
        # roa = (25_000 * 4) / 500_000 = 0.2
        assert row["roa"] == pytest.approx(0.2)

    def test_debt_to_equity_correct(self) -> None:
        row = _compute_ratios("AAPL", _QUARTER_ROW, _MARKET_CAP)
        # d/e = 50_000 / 80_000 = 0.625
        assert row["debt_to_equity"] == pytest.approx(0.625)

    def test_fcf_yield_correct(self) -> None:
        row = _compute_ratios("AAPL", _QUARTER_ROW, _MARKET_CAP)
        # fcf_yield = (22_000 * 4) / 2_000_000 = 0.044
        assert row["free_cash_flow_yield"] == pytest.approx(0.044)

    def test_none_market_cap_produces_nan_ratios(self) -> None:
        row = _compute_ratios("AAPL", _QUARTER_ROW, None)
        assert math.isnan(row["pe_ratio"])
        assert math.isnan(row["pb_ratio"])
        # ev-based ratio also NaN
        assert math.isnan(row["ev_ebitda"])
        # Margin ratios should still work
        assert row["gross_margin"] == pytest.approx(0.45)

    def test_missing_net_income_pe_is_nan(self) -> None:
        row_series = _QUARTER_ROW.copy()
        row_series["net_income"] = float("nan")
        row = _compute_ratios("AAPL", row_series, _MARKET_CAP)
        assert math.isnan(row["pe_ratio"])

    def test_zero_equity_pb_is_nan(self) -> None:
        row_series = _QUARTER_ROW.copy()
        row_series["equity"] = 0.0
        row = _compute_ratios("AAPL", row_series, _MARKET_CAP)
        assert math.isnan(row["pb_ratio"])


class TestComputeValuationRatios:
    def test_returns_dict_with_ticker_key(self) -> None:
        fin = {"aapl": _make_fin_loader(_QUARTER_ROW)}
        info = {"aapl": _make_info_loader(_MARKET_CAP)}

        result = compute_valuation_ratios(fin, info)

        assert "aapl" in result
        df = result["aapl"]
        assert len(df) == 1

    def test_all_ratio_columns_present(self) -> None:
        fin = {"aapl": _make_fin_loader(_QUARTER_ROW)}
        info = {"aapl": _make_info_loader(_MARKET_CAP)}

        result = compute_valuation_ratios(fin, info)
        df = result["aapl"]

        for col in _RATIO_COLS:
            assert col in df.columns, f"Missing column: {col}"

    def test_missing_company_info_produces_nan_market_cap(self) -> None:
        fin = {"aapl": _make_fin_loader(_QUARTER_ROW)}
        info: dict = {}  # no company info

        result = compute_valuation_ratios(fin, info)

        assert "aapl" in result
        df = result["aapl"]
        assert math.isnan(df["market_cap"].iloc[0])

    def test_empty_financials_ticker_skipped(self) -> None:
        def _empty_loader() -> pd.DataFrame:
            return pd.DataFrame()

        fin = {"aapl": _empty_loader}
        info = {"aapl": _make_info_loader(_MARKET_CAP)}

        result = compute_valuation_ratios(fin, info)

        assert "aapl" not in result

    def test_ticker_missing_from_financials_not_in_output(self) -> None:
        fin: dict = {}  # no financials at all
        info = {"aapl": _make_info_loader(_MARKET_CAP)}

        result = compute_valuation_ratios(fin, info)

        assert result == {}

    def test_failed_financials_load_is_skipped(self) -> None:
        def bad_loader() -> pd.DataFrame:
            raise OSError("disk error")

        fin = {"aapl": bad_loader}
        info = {"aapl": _make_info_loader(_MARKET_CAP)}

        result = compute_valuation_ratios(fin, info)

        assert result == {}

    def test_multiple_tickers(self) -> None:
        msft_row = _QUARTER_ROW.copy()
        msft_row["ticker"] = "MSFT"
        fin = {
            "aapl": _make_fin_loader(_QUARTER_ROW),
            "msft": _make_fin_loader(msft_row),
        }
        info = {
            "aapl": _make_info_loader(_MARKET_CAP),
            "msft": _make_info_loader(3_000_000.0),
        }

        result = compute_valuation_ratios(fin, info)

        assert "aapl" in result
        assert "msft" in result

    def test_fetched_at_is_set(self) -> None:
        fin = {"aapl": _make_fin_loader(_QUARTER_ROW)}
        info = {"aapl": _make_info_loader(_MARKET_CAP)}

        result = compute_valuation_ratios(fin, info)
        df = result["aapl"]

        assert "fetched_at" in df.columns
        assert isinstance(df["fetched_at"].iloc[0], pd.Timestamp)
