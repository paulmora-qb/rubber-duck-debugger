"""Integration tests for the ai_fundamental_screen pipeline."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest
from kedro.io import DataCatalog, MemoryDataset
from kedro.runner import SequentialRunner

from rdd.pipelines.strategies.ai_fundamental_screen.pipeline import create_pipeline


@pytest.fixture(autouse=True)
def set_api_key(monkeypatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")


@pytest.fixture
def pipeline():
    return create_pipeline(variant="monthly")


@pytest.fixture
def params() -> dict[str, Any]:
    return {
        "strategy_name": "ai_fundamental_screen_monthly",
        "model_screening": "claude-haiku-4-5-20251001",
        "model_selection": "claude-sonnet-4-6",
        "model_rebalancing": "claude-haiku-4-5-20251001",
        "max_tokens_screening": 512,
        "max_tokens_selection": 4096,
        "max_tokens_rebalancing": 2048,
        "top_n_candidates": 5,
        "max_holdings": 2,
        "max_industry_weight": 0.50,
        "min_position_weight": 0.10,
        "max_position_weight": 0.90,
        "continuity_bonus": 0.08,
    }


def _mock_claude_responses(mocker) -> None:
    score_resp = '{"score": 8.0, "verdict": "BUY", "thesis": "Strong fundamentals."}'
    select_resp = """{
        "holdings": [
            {"ticker": "AAPL", "weight": 0.60, "sector": "Technology",
             "industry": "Consumer Electronics", "thesis": "Leading hardware."},
            {"ticker": "MSFT", "weight": 0.40, "sector": "Technology",
             "industry": "Software", "thesis": "Cloud growth."}
        ],
        "industry_breakdown": {"Consumer Electronics": 0.60, "Software": 0.40},
        "portfolio_thesis": "Tech-focused portfolio."
    }"""
    rebalance_resp = """{
        "trades": [
            {"ticker": "AAPL", "action": "BUY", "current_weight": null,
             "target_weight": 0.60, "reasoning": "New entry."},
            {"ticker": "MSFT", "action": "BUY", "current_weight": null,
             "target_weight": 0.40, "reasoning": "New entry."}
        ],
        "estimated_turnover_pct": 100.0
    }"""

    responses = [score_resp, score_resp, select_resp, rebalance_resp]
    call_count = {"n": 0}

    def _side_effect(**kwargs):
        resp = responses[min(call_count["n"], len(responses) - 1)]
        call_count["n"] += 1
        mock_msg = mocker.MagicMock()
        mock_msg.content = [mocker.MagicMock(text=resp)]
        return mock_msg

    mock_client = mocker.MagicMock()
    mock_client.messages.create.side_effect = _side_effect
    mocker.patch(
        "rdd.pipelines.strategies.ai_fundamental_screen.nodes.anthropic.Anthropic",
        return_value=mock_client,
    )


def _make_catalog(params: dict) -> DataCatalog:
    info_df = pd.DataFrame(
        [
            {
                "ticker": t,
                "name": f"{t} Inc.",
                "sector": "Technology",
                "industry": "Consumer Electronics",
                "market_cap": 1e12,
                "employees": 50_000.0,
                "country": "US",
                "currency": "USD",
                "exchange": "NMS",
                "fetched_at": pd.Timestamp("2024-01-02"),
            }
            for t in ["AAPL", "MSFT"]
        ]
    )
    return DataCatalog(
        {
            "raw_valuation_ratios_existing": MemoryDataset(data={}),
            "raw_analyst_consensus": MemoryDataset(data={}),
            "raw_earnings_history": MemoryDataset(data={}),
            "raw_company_info": MemoryDataset(
                data={
                    "aapl": lambda: info_df[info_df["ticker"] == "AAPL"],
                    "msft": lambda: info_df[info_df["ticker"] == "MSFT"],
                }
            ),
            "raw_company_financials_quarterly_existing": MemoryDataset(data={}),
            "stock_analyses": MemoryDataset(data={}),
            "raw_news_analysis_existing": MemoryDataset(data={}),
            "live_portfolio": MemoryDataset(data={}),
            "ai_fundamental_screen_monthly_ticker_scores": MemoryDataset(),
            "ai_fundamental_screen_monthly_allocation": MemoryDataset(),
            "ai_fundamental_screen_monthly_trades": MemoryDataset(),
            "ai_fundamental_screen_monthly.holdings": MemoryDataset(),
            "params:ai_fundamental_screen_monthly": MemoryDataset(data=params),
        }
    )


def test_pipeline_runs_successfully(mocker, pipeline, params) -> None:
    _mock_claude_responses(mocker)
    catalog = _make_catalog(params)
    SequentialRunner().run(pipeline, catalog)

    trades = catalog.load("ai_fundamental_screen_monthly_trades")
    assert "trades" in trades
    assert "rebalanced_at" in trades


def test_pipeline_first_run_all_buys(mocker, pipeline, params) -> None:
    _mock_claude_responses(mocker)
    catalog = _make_catalog(params)
    SequentialRunner().run(pipeline, catalog)

    trades = catalog.load("ai_fundamental_screen_monthly_trades")
    assert all(t["action"] == "BUY" for t in trades["trades"])


def test_portfolio_allocation_has_required_keys(mocker, pipeline, params) -> None:
    _mock_claude_responses(mocker)
    catalog = _make_catalog(params)
    # Run only up to construct_portfolio so the intermediate dataset isn't
    # released when rebalance_portfolio consumes it.
    partial = pipeline.filter(to_nodes=["construct_portfolio"])
    SequentialRunner().run(partial, catalog)

    allocation = catalog.load("ai_fundamental_screen_monthly_allocation")
    assert "holdings" in allocation
    assert "generated_at" in allocation
