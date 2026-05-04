"""Unit tests for portfolio_construction nodes."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest

from rdd.pipelines.strategies.claude_fundamental.nodes import (
    _build_ticker_brief,
    construct_portfolio,
    rebalance_portfolio,
    score_tickers,
)

# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def set_api_key(monkeypatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")


@pytest.fixture
def base_params() -> dict[str, Any]:
    return {
        "model_screening": "claude-haiku-4-5-20251001",
        "model_selection": "claude-sonnet-4-6",
        "model_rebalancing": "claude-haiku-4-5-20251001",
        "max_tokens_screening": 512,
        "max_tokens_selection": 4096,
        "max_tokens_rebalancing": 2048,
        "top_n_candidates": 10,
        "max_holdings": 3,
        "max_industry_weight": 0.35,
        "min_position_weight": 0.05,
        "max_position_weight": 0.50,
        "continuity_bonus": 0.08,
    }


def _make_info_df(
    ticker: str = "AAPL",
    sector: str = "Technology",
    industry: str = "Consumer Electronics",
    market_cap: float = 3e12,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": ticker,
                "name": f"{ticker} Inc.",
                "sector": sector,
                "industry": industry,
                "market_cap": market_cap,
                "employees": 100_000.0,
                "country": "United States",
                "currency": "USD",
                "exchange": "NMS",
                "fetched_at": pd.Timestamp("2024-01-02"),
            }
        ]
    )


def _make_valuation_df(ticker: str = "AAPL") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": ticker,
                "period_end": pd.Timestamp("2024-01-01"),
                "pe_ratio": 28.5,
                "pb_ratio": 4.2,
                "ev_ebitda": 18.0,
                "gross_margin": 0.44,
                "operating_margin": 0.30,
                "free_cash_flow_yield": 0.035,
                "market_cap": 3e12,
                "roe": 0.15,
                "roa": 0.08,
                "debt_to_equity": 0.3,
                "net_margin": 0.25,
                "fetched_at": pd.Timestamp("2024-01-02"),
            }
        ]
    )


def _make_consensus_df(ticker: str = "AAPL") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": ticker,
                "recommendation_key": "buy",
                "recommendation_mean": 2.1,
                "analyst_count": 35.0,
                "target_mean_price": 250.0,
                "target_high_price": 300.0,
                "target_low_price": 200.0,
                "target_median_price": 250.0,
                "current_price": 220.0,
                "fetched_at": pd.Timestamp("2024-01-02"),
            }
        ]
    )


def _make_earnings_df(ticker: str = "AAPL") -> pd.DataFrame:
    dates = pd.date_range("2023-01-01", periods=4, freq="QE")
    return pd.DataFrame(
        {
            "ticker": [ticker] * 4,
            "earnings_date": dates,
            "eps_estimate": [1.0, 1.1, 1.2, 1.3],
            "reported_eps": [1.05, 1.15, 1.18, 1.35],
            "surprise_pct": [5.0, 4.5, -1.7, 3.8],
            "fetched_at": pd.Timestamp("2024-01-02"),
        }
    )


def _make_financials_df(ticker: str = "AAPL") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": ticker,
                "period_end": pd.Timestamp("2023-12-31"),
                "total_revenue": 120e9,
                "gross_profit": 55e9,
                "operating_income": 35e9,
                "net_income": 30e9,
                "ebitda": 40e9,
                "ebit": 35e9,
                "diluted_eps": 1.3,
                "basic_eps": 1.35,
                "cost_of_revenue": 65e9,
                "research_and_development": 8e9,
                "selling_general_and_administration": 12e9,
                "pretax_income": 32e9,
                "tax_provision": 2e9,
                "total_assets": 300e9,
                "total_liabilities": 250e9,
                "equity": 50e9,
                "total_debt": 100e9,
                "cash_and_equivalents": 30e9,
                "net_debt": 70e9,
                "working_capital": 20e9,
                "net_ppe": 40e9,
                "accounts_receivable": 15e9,
                "inventory": 5e9,
                "accounts_payable": 10e9,
                "long_term_debt": 80e9,
                "free_cash_flow": 25e9,
                "operating_cash_flow": 32e9,
                "capital_expenditure": -7e9,
                "cash_dividends_paid": -5e9,
                "stock_based_compensation": 3e9,
                "depreciation_and_amortization": 5e9,
                "net_long_term_debt_issuance": -10e9,
                "repurchase_of_capital_stock": -20e9,
                "fetched_at": pd.Timestamp("2024-01-02"),
            }
        ]
    )


def _mock_claude(mocker, response: str) -> MagicMock:
    mock_client = MagicMock()
    mock_msg = MagicMock()
    mock_msg.content = [MagicMock(text=response)]
    mock_client.messages.create.return_value = mock_msg
    mocker.patch(
        "rdd.pipelines.strategies.claude_fundamental.nodes.anthropic.Anthropic",
        return_value=mock_client,
    )
    return mock_client


# ── _build_ticker_brief ───────────────────────────────────────────────────────


class TestBuildTickerBrief:
    def test_all_sources_present(self) -> None:
        brief = _build_ticker_brief(
            ticker="AAPL",
            info=_make_info_df(),
            valuation=_make_valuation_df(),
            consensus=_make_consensus_df(),
            earnings=_make_earnings_df(),
            financials=_make_financials_df(),
            strategy_signals={
                "ticker": "AAPL",
                "signals": [
                    {"strategy": "momentum", "direction": "bullish", "metrics": {}},
                    {"strategy": "trend", "direction": "bearish", "metrics": {}},
                ],
            },
            news={
                "bull_report": "We rate AAPL a BUY (conviction 4/5) with a price target of $280."
            },
        )
        assert "AAPL" in brief
        assert "Technology" in brief
        assert "momentum: bullish" in brief
        assert "P/E" in brief
        assert "Revenue" in brief
        assert "buy" in brief
        assert "conviction 4/5" in brief

    def test_all_sources_none(self) -> None:
        brief = _build_ticker_brief(
            ticker="AAPL",
            info=None,
            valuation=None,
            consensus=None,
            earnings=None,
            financials=None,
            strategy_signals=None,
            news=None,
        )
        assert "AAPL" in brief

    def test_empty_dataframes(self) -> None:
        brief = _build_ticker_brief(
            ticker="MSFT",
            info=pd.DataFrame(),
            valuation=pd.DataFrame(),
            consensus=pd.DataFrame(),
            earnings=pd.DataFrame(),
            financials=pd.DataFrame(),
            strategy_signals=None,
            news=None,
        )
        assert "MSFT" in brief


# ── score_tickers ─────────────────────────────────────────────────────────────


class TestScoreTickers:
    def test_scores_tickers_successfully(self, mocker, base_params) -> None:
        _mock_claude(
            mocker,
            '{"score": 7.5, "verdict": "BUY", "thesis": "Strong fundamentals."}',
        )
        result = score_tickers(
            valuation_ratios={"aapl": lambda: _make_valuation_df()},
            analyst_consensus={"aapl": lambda: _make_consensus_df()},
            earnings_history={"aapl": lambda: _make_earnings_df()},
            company_info={"aapl": lambda: _make_info_df()},
            company_financials={"aapl": lambda: _make_financials_df()},
            stock_analyses={},
            news_analysis={},
            params=base_params,
        )
        assert "aapl" in result
        assert result["aapl"]["score"] == 7.5
        assert result["aapl"]["verdict"] == "BUY"
        assert result["aapl"]["ticker"] == "AAPL"

    def test_api_failure_skips_ticker(self, mocker, base_params) -> None:
        mock_client = MagicMock()
        mock_client.messages.create.side_effect = RuntimeError("API error")
        mocker.patch(
            "rdd.pipelines.strategies.claude_fundamental.nodes.anthropic.Anthropic",
            return_value=mock_client,
        )
        result = score_tickers(
            valuation_ratios={"aapl": lambda: _make_valuation_df()},
            analyst_consensus={},
            earnings_history={},
            company_info={"aapl": lambda: _make_info_df()},
            company_financials={},
            stock_analyses={},
            news_analysis={},
            params=base_params,
        )
        assert result == {}

    def test_malformed_json_skips_ticker(self, mocker, base_params) -> None:
        _mock_claude(mocker, "not valid json at all")
        result = score_tickers(
            valuation_ratios={"aapl": lambda: _make_valuation_df()},
            analyst_consensus={},
            earnings_history={},
            company_info={},
            company_financials={},
            stock_analyses={},
            news_analysis={},
            params=base_params,
        )
        assert result == {}

    def test_uses_news_analysis_partition(self, mocker, base_params) -> None:
        _mock_claude(
            mocker,
            '{"score": 9.0, "verdict": "STRONG_BUY", "thesis": "News positive."}',
        )
        news_entry = {
            "bull_report": "We rate AAPL a BUY with a 12-month target of $300 (conviction 5/5).",
            "bear_report": "",
        }
        result = score_tickers(
            valuation_ratios={},
            analyst_consensus={},
            earnings_history={},
            company_info={},
            company_financials={},
            stock_analyses={},
            news_analysis={"aapl": lambda: news_entry},
            params=base_params,
        )
        assert "aapl" in result


# ── construct_portfolio ───────────────────────────────────────────────────────


class TestConstructPortfolio:
    def _portfolio_response(self) -> str:
        return """{
            "holdings": [
                {"ticker": "AAPL", "weight": 0.40, "sector": "Technology",
                 "industry": "Consumer Electronics", "thesis": "Strong earnings."},
                {"ticker": "MSFT", "weight": 0.35, "sector": "Technology",
                 "industry": "Software", "thesis": "Cloud growth."},
                {"ticker": "JNJ", "weight": 0.25, "sector": "Healthcare",
                 "industry": "Drug Manufacturers", "thesis": "Stable dividends."}
            ],
            "industry_breakdown": {
                "Consumer Electronics": 0.40,
                "Software": 0.35,
                "Drug Manufacturers": 0.25
            },
            "portfolio_thesis": "Diversified across tech and healthcare."
        }"""

    def test_returns_valid_portfolio(self, mocker, base_params) -> None:
        _mock_claude(mocker, self._portfolio_response())
        scores = {
            "aapl": {
                "ticker": "AAPL",
                "score": 9.0,
                "verdict": "STRONG_BUY",
                "thesis": "t",
            },
            "msft": {"ticker": "MSFT", "score": 8.5, "verdict": "BUY", "thesis": "t"},
            "jnj": {"ticker": "JNJ", "score": 7.0, "verdict": "BUY", "thesis": "t"},
        }
        result = construct_portfolio(
            ticker_scores=scores,
            company_info={"aapl": lambda: _make_info_df()},
            params=base_params,
        )
        assert "holdings" in result
        assert len(result["holdings"]) == 3
        assert "generated_at" in result

    def test_includes_generated_at(self, mocker, base_params) -> None:
        _mock_claude(mocker, self._portfolio_response())
        result = construct_portfolio(
            ticker_scores={
                "aapl": {
                    "ticker": "AAPL",
                    "score": 8.0,
                    "verdict": "BUY",
                    "thesis": "t",
                }
            },
            company_info={},
            params=base_params,
        )
        assert result["generated_at"].endswith("Z")


# ── rebalance_portfolio ───────────────────────────────────────────────────────


class TestRebalancePortfolio:
    def _proposed(self) -> dict:
        return {
            "holdings": [
                {
                    "ticker": "AAPL",
                    "weight": 0.50,
                    "sector": "Technology",
                    "industry": "Consumer Electronics",
                    "thesis": "Growth.",
                },
                {
                    "ticker": "NVDA",
                    "weight": 0.50,
                    "sector": "Technology",
                    "industry": "Semiconductors",
                    "thesis": "AI upside.",
                },
            ]
        }

    def test_first_run_all_buys(self, base_params) -> None:
        result = rebalance_portfolio(
            portfolio_allocation=self._proposed(),
            live_portfolio={},
            ticker_scores={},
            params=base_params,
        )
        actions = {t["ticker"]: t["action"] for t in result["trades"]}
        assert actions["AAPL"] == "BUY"
        assert actions["NVDA"] == "BUY"
        assert result["estimated_turnover_pct"] == 100.0

    def test_normal_rebalance_calls_claude(self, mocker, base_params) -> None:
        mock_client = _mock_claude(
            mocker,
            """{
                "trades": [
                    {"ticker": "AAPL", "action": "HOLD",
                     "current_weight": 0.50, "target_weight": 0.50,
                     "reasoning": "Unchanged."},
                    {"ticker": "META", "action": "SELL",
                     "current_weight": 0.50, "target_weight": null,
                     "reasoning": "Score dropped."},
                    {"ticker": "NVDA", "action": "BUY",
                     "current_weight": null, "target_weight": 0.50,
                     "reasoning": "New entry."}
                ],
                "estimated_turnover_pct": 50.0
            }""",
        )
        live = {
            "holdings": [
                {
                    "ticker": "AAPL",
                    "weight": 0.50,
                    "sector": "Technology",
                    "industry": "Consumer Electronics",
                    "thesis": "Growth.",
                },
                {
                    "ticker": "META",
                    "weight": 0.50,
                    "sector": "Technology",
                    "industry": "Social Media",
                    "thesis": "Ads.",
                },
            ]
        }
        scores = {
            "aapl": {"ticker": "AAPL", "score": 8.0, "verdict": "BUY", "thesis": "t"},
            "meta": {"ticker": "META", "score": 5.0, "verdict": "HOLD", "thesis": "t"},
            "nvda": {
                "ticker": "NVDA",
                "score": 9.0,
                "verdict": "STRONG_BUY",
                "thesis": "t",
            },
        }
        result = rebalance_portfolio(
            portfolio_allocation=self._proposed(),
            live_portfolio=live,
            ticker_scores=scores,
            params=base_params,
        )
        assert mock_client.messages.create.called
        assert result["estimated_turnover_pct"] == 50.0
        assert "rebalanced_at" in result

    def test_continuity_bonus_applied_to_existing(self, mocker, base_params) -> None:
        """Existing holdings should have their scores boosted in the prompt text."""
        mock_client = _mock_claude(
            mocker,
            '{"trades": [], "estimated_turnover_pct": 0.0}',
        )
        live = {
            "holdings": [
                {
                    "ticker": "AAPL",
                    "weight": 1.0,
                    "sector": "Technology",
                    "industry": "Consumer Electronics",
                    "thesis": "t",
                },
            ]
        }
        scores = {
            "aapl": {"ticker": "AAPL", "score": 7.0, "verdict": "BUY", "thesis": "t"},
        }
        rebalance_portfolio(
            portfolio_allocation={"holdings": []},
            live_portfolio=live,
            ticker_scores=scores,
            params=base_params,
        )
        prompt_sent = mock_client.messages.create.call_args[1]["messages"][0]["content"]
        # Score 7.0 + 0.08*10 = 7.8 should appear in the prompt
        assert "7.8" in prompt_sent
