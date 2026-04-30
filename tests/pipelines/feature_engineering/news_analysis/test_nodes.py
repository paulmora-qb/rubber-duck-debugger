"""Unit tests for news_analysis nodes.

Network is disabled globally via --disable-socket. All Anthropic API calls are
patched with pytest-mock. ANTHROPIC_API_KEY is injected via monkeypatch.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from rdd.pipelines.feature_engineering.news_analysis.nodes import (
    _REQUIRED_KEYS,
    _articles_to_text,
    _is_fresh,
    analyze_news,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_ANALYSIS = {
    "ticker": "AAPL",
    "analysis_date": "2024-01-15T10:00:00Z",
    "article_count": 3,
    "lookback_days": 7,
    "bull_report": "Apple is well-positioned for growth driven by strong hardware innovation...",
    "bear_report": "Apple faces significant headwinds including slowing China sales and a stretched valuation...",
}


def _make_news_df(
    ticker: str = "AAPL",
    n: int = 3,
    days_ago: int = 2,
) -> pd.DataFrame:
    """Return a small news DataFrame with recent articles."""
    base = pd.Timestamp.now("UTC").tz_convert(None) - pd.Timedelta(days=days_ago)
    return pd.DataFrame(
        {
            "ticker": [ticker] * n,
            "published_at": pd.date_range(base, periods=n, freq="D"),
            "headline": [f"Headline {i}" for i in range(n)],
            "summary": [f"Summary {i}" for i in range(n)],
            "source": ["Reuters"] * n,
            "url": [f"https://example.com/{i}" for i in range(n)],
            "category": ["company news"] * n,
        }
    )


def _mock_anthropic(
    mocker, bull_text: str = "Bull report text.", bear_text: str = "Bear report text."
) -> Any:
    """Patch anthropic.Anthropic to return alternating bull/bear plain-text responses."""
    mock_client = mocker.MagicMock()
    bull_resp = mocker.MagicMock()
    bull_resp.content = [mocker.MagicMock()]
    bull_resp.content[0].text = bull_text
    bear_resp = mocker.MagicMock()
    bear_resp.content = [mocker.MagicMock()]
    bear_resp.content[0].text = bear_text
    mock_client.messages.create.side_effect = [bull_resp, bear_resp] * 20
    mocker.patch(
        "rdd.pipelines.feature_engineering.news_analysis.nodes.anthropic.Anthropic",
        return_value=mock_client,
    )
    return mock_client


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def set_api_key(monkeypatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")


@pytest.fixture
def base_params() -> dict:
    return {
        "lookback_days": 7,
        "model": "claude-haiku-4-5-20251001",
        "refresh_hours": 6,
    }


# ---------------------------------------------------------------------------
# Tests for helper functions
# ---------------------------------------------------------------------------


class TestArticlesToText:
    def test_returns_text_and_count_for_recent_articles(self) -> None:
        df = _make_news_df(days_ago=2)
        text, count = _articles_to_text(df, lookback_days=7)

        assert count == 3
        assert "Headline" in text

    def test_excludes_articles_outside_lookback(self) -> None:
        df = _make_news_df(days_ago=10)
        text, count = _articles_to_text(df, lookback_days=7)

        assert count == 0
        assert text == ""

    def test_empty_df_returns_empty(self) -> None:
        df = pd.DataFrame(
            columns=[
                "ticker",
                "published_at",
                "headline",
                "summary",
                "source",
                "url",
                "category",
            ]
        )
        text, count = _articles_to_text(df, lookback_days=7)

        assert count == 0
        assert text == ""


class TestIsFresh:
    def test_fresh_analysis_returns_true(self) -> None:
        analysis = {
            "analysis_date": pd.Timestamp.now("UTC").tz_convert(None).isoformat()
        }
        cutoff = pd.Timestamp.now("UTC").tz_convert(None) - pd.Timedelta(hours=6)
        assert _is_fresh(analysis, cutoff) is True

    def test_stale_analysis_returns_false(self) -> None:
        analysis = {"analysis_date": "2000-01-01T00:00:00"}
        cutoff = pd.Timestamp.now("UTC").tz_convert(None) - pd.Timedelta(hours=6)
        assert _is_fresh(analysis, cutoff) is False

    def test_missing_date_returns_false(self) -> None:
        assert _is_fresh({}, pd.Timestamp.now()) is False

    def test_bad_date_string_returns_false(self) -> None:
        assert _is_fresh({"analysis_date": "not-a-date"}, pd.Timestamp.now()) is False


# ---------------------------------------------------------------------------
# Tests for analyze_news node
# ---------------------------------------------------------------------------


class TestAnalyzeNews:
    def test_no_news_for_ticker_is_skipped(self, mocker, base_params) -> None:
        mock_client = _mock_anthropic(mocker)

        result = analyze_news(
            company_news={"aapl": lambda: pd.DataFrame()},
            existing={},
            params=base_params,
        )

        assert result == {}
        mock_client.messages.create.assert_not_called()

    def test_fresh_analysis_is_not_refetched(self, mocker, base_params) -> None:
        mock_client = _mock_anthropic(mocker)
        fresh = _BASE_ANALYSIS.copy()
        fresh["analysis_date"] = pd.Timestamp.now("UTC").tz_convert(None).isoformat()

        result = analyze_news(
            company_news={"aapl": lambda: _make_news_df()},
            existing={"aapl": lambda: fresh},
            params=base_params,
        )

        assert "aapl" in result
        mock_client.messages.create.assert_not_called()

    def test_stale_analysis_triggers_two_claude_calls(
        self, mocker, base_params
    ) -> None:
        mock_client = _mock_anthropic(mocker)
        stale = _BASE_ANALYSIS.copy()
        stale["analysis_date"] = "2000-01-01T00:00:00"

        analyze_news(
            company_news={"aapl": lambda: _make_news_df()},
            existing={"aapl": lambda: stale},
            params=base_params,
        )

        assert mock_client.messages.create.call_count == 2

    def test_missing_existing_triggers_two_claude_calls(
        self, mocker, base_params
    ) -> None:
        mock_client = _mock_anthropic(mocker)

        analyze_news(
            company_news={"aapl": lambda: _make_news_df()},
            existing={},
            params=base_params,
        )

        assert mock_client.messages.create.call_count == 2

    def test_output_has_all_required_keys(self, mocker, base_params) -> None:
        _mock_anthropic(mocker)

        result = analyze_news(
            company_news={"aapl": lambda: _make_news_df()},
            existing={},
            params=base_params,
        )

        assert "aapl" in result
        for key in _REQUIRED_KEYS:
            assert key in result["aapl"], f"Missing required key: {key}"

    def test_bull_and_bear_reports_are_strings(self, mocker, base_params) -> None:
        _mock_anthropic(
            mocker,
            bull_text="Bullish report content.",
            bear_text="Bearish report content.",
        )

        result = analyze_news(
            company_news={"aapl": lambda: _make_news_df()},
            existing={},
            params=base_params,
        )

        assert isinstance(result["aapl"]["bull_report"], str)
        assert isinstance(result["aapl"]["bear_report"], str)
        assert result["aapl"]["bull_report"] == "Bullish report content."
        assert result["aapl"]["bear_report"] == "Bearish report content."

    def test_claude_api_failure_does_not_crash_pipeline(
        self, mocker, base_params
    ) -> None:
        mock_client = mocker.MagicMock()
        mock_client.messages.create.side_effect = RuntimeError("API error")
        mocker.patch(
            "rdd.pipelines.feature_engineering.news_analysis.nodes.anthropic.Anthropic",
            return_value=mock_client,
        )

        result = analyze_news(
            company_news={
                "aapl": lambda: _make_news_df(),
                "googl": lambda: _make_news_df("GOOGL"),
            },
            existing={},
            params=base_params,
        )

        assert isinstance(result, dict)

    def test_articles_outside_lookback_causes_skip(self, mocker, base_params) -> None:
        mock_client = _mock_anthropic(mocker)

        result = analyze_news(
            company_news={"aapl": lambda: _make_news_df(days_ago=30)},
            existing={},
            params=base_params,
        )

        assert result == {}
        mock_client.messages.create.assert_not_called()

    def test_two_tickers_make_four_claude_calls(self, mocker, base_params) -> None:
        mock_client = _mock_anthropic(mocker)

        analyze_news(
            company_news={
                "aapl": lambda: _make_news_df("AAPL"),
                "googl": lambda: _make_news_df("GOOGL"),
            },
            existing={},
            params=base_params,
        )

        assert mock_client.messages.create.call_count == 4

    def test_existing_dict_fresh_analysis_passed_through(
        self, mocker, base_params
    ) -> None:
        mock_client = _mock_anthropic(mocker)
        fresh = _BASE_ANALYSIS.copy()
        fresh["analysis_date"] = pd.Timestamp.now("UTC").tz_convert(None).isoformat()

        result = analyze_news(
            company_news={"aapl": lambda: _make_news_df()},
            existing={"aapl": fresh},
            params=base_params,
        )

        assert "aapl" in result
        mock_client.messages.create.assert_not_called()
