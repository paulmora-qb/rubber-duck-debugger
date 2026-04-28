"""Unit tests for news_analysis nodes.

Network is disabled globally via --disable-socket. All Anthropic API calls are
patched with pytest-mock. ANTHROPIC_API_KEY is injected via monkeypatch.
"""

from __future__ import annotations

import json
from typing import Any

import pandas as pd
import pytest

from rdd.pipelines.news_analysis.nodes import (
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
    "sentiment_score": 0.4,
    "bull_thesis": "Strong earnings beat expectations...",
    "bear_thesis": "Rising competition in AI chips...",
    "discussion": (
        "Bull Analyst: The recent earnings show strong growth.\n"
        "Bear Analyst: However, margins are under pressure.\n"
        "Bull Analyst: Cash flow remains robust despite margin pressure."
    ),
    "overall_assessment": "Moderately bullish given strong fundamentals...",
    "key_topics": ["earnings", "AI", "competition"],
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


def _mock_anthropic(mocker, response_dict: dict[str, Any] | None = None) -> Any:
    """Patch anthropic.Anthropic and set up a realistic message response."""
    if response_dict is None:
        response_dict = _BASE_ANALYSIS.copy()
    mock_client = mocker.MagicMock()
    mock_response = mocker.MagicMock()
    mock_response.content = [mocker.MagicMock()]
    mock_response.content[0].text = json.dumps(response_dict)
    mock_client.messages.create.return_value = mock_response
    mocker.patch(
        "rdd.pipelines.news_analysis.nodes.anthropic.Anthropic",
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
        df = _make_news_df(days_ago=10)  # all articles older than 7 days
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
        """Tickers with no news data should be skipped without calling Claude."""
        mock_client = _mock_anthropic(mocker)

        result = analyze_news(
            company_news={"aapl": lambda: pd.DataFrame()},
            existing={},
            params=base_params,
        )

        assert result == {}
        mock_client.messages.create.assert_not_called()

    def test_fresh_analysis_is_not_refetched(self, mocker, base_params) -> None:
        """Tickers with a fresh existing analysis should be returned as-is."""
        mock_client = _mock_anthropic(mocker)
        fresh_analysis = _BASE_ANALYSIS.copy()
        fresh_analysis["analysis_date"] = (
            pd.Timestamp.now("UTC").tz_convert(None).isoformat()
        )

        result = analyze_news(
            company_news={"aapl": lambda: _make_news_df()},
            existing={"aapl": lambda: fresh_analysis},
            params=base_params,
        )

        assert "aapl" in result
        mock_client.messages.create.assert_not_called()

    def test_stale_analysis_triggers_claude_call(self, mocker, base_params) -> None:
        """Tickers with stale analysis should trigger a new Claude API call."""
        mock_client = _mock_anthropic(mocker)
        stale_analysis = _BASE_ANALYSIS.copy()
        stale_analysis["analysis_date"] = "2000-01-01T00:00:00"

        result = analyze_news(
            company_news={"aapl": lambda: _make_news_df()},
            existing={"aapl": lambda: stale_analysis},
            params=base_params,
        )

        assert "aapl" in result
        mock_client.messages.create.assert_called_once()

    def test_missing_existing_triggers_claude_call(self, mocker, base_params) -> None:
        """Tickers with no existing analysis should trigger a Claude API call."""
        _mock_anthropic(mocker)

        result = analyze_news(
            company_news={"aapl": lambda: _make_news_df()},
            existing={},
            params=base_params,
        )

        assert "aapl" in result

    def test_output_has_all_required_keys(self, mocker, base_params) -> None:
        """The analysis dict for each ticker must contain all required keys."""
        _mock_anthropic(mocker)

        result = analyze_news(
            company_news={"aapl": lambda: _make_news_df()},
            existing={},
            params=base_params,
        )

        assert "aapl" in result
        analysis = result["aapl"]
        for key in _REQUIRED_KEYS:
            assert key in analysis, f"Missing required key: {key}"

    def test_claude_api_failure_does_not_crash_pipeline(
        self, mocker, base_params
    ) -> None:
        """A Claude API failure for one ticker should not prevent others from completing."""
        mock_client = mocker.MagicMock()
        mock_client.messages.create.side_effect = RuntimeError("API error")
        mocker.patch(
            "rdd.pipelines.news_analysis.nodes.anthropic.Anthropic",
            return_value=mock_client,
        )

        # Two tickers: one will fail, but neither should crash the whole pipeline
        result = analyze_news(
            company_news={
                "aapl": lambda: _make_news_df("AAPL"),
                "msft": lambda: _make_news_df("MSFT"),
            },
            existing={},
            params=base_params,
        )

        # Both fail, but no exception is raised
        assert isinstance(result, dict)

    def test_claude_api_failure_one_ticker_others_succeed(
        self, mocker, base_params
    ) -> None:
        """When one ticker's Claude call fails, other tickers still complete."""
        call_count = 0

        def _side_effect(*_args, **_kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                msg = "API error for first ticker"
                raise RuntimeError(msg)
            # Second call succeeds
            mock_response = mocker.MagicMock()
            mock_response.content = [mocker.MagicMock()]
            msft_analysis = _BASE_ANALYSIS.copy()
            msft_analysis["ticker"] = "MSFT"
            mock_response.content[0].text = json.dumps(msft_analysis)
            return mock_response

        mock_client = mocker.MagicMock()
        mock_client.messages.create.side_effect = _side_effect
        mocker.patch(
            "rdd.pipelines.news_analysis.nodes.anthropic.Anthropic",
            return_value=mock_client,
        )

        result = analyze_news(
            company_news={
                "aapl": lambda: _make_news_df("AAPL"),
                "msft": lambda: _make_news_df("MSFT"),
            },
            existing={},
            params=base_params,
        )

        # AAPL failed, MSFT should succeed
        assert "msft" in result
        assert "aapl" not in result

    def test_articles_outside_lookback_causes_skip(self, mocker, base_params) -> None:
        """Tickers whose articles are all outside the lookback window are skipped."""
        mock_client = _mock_anthropic(mocker)

        # All articles are 30 days old; lookback is only 7 days
        old_df = _make_news_df(days_ago=30)

        result = analyze_news(
            company_news={"aapl": lambda: old_df},
            existing={},
            params=base_params,
        )

        assert result == {}
        mock_client.messages.create.assert_not_called()

    def test_sentiment_score_is_float(self, mocker, base_params) -> None:
        """The sentiment_score returned by Claude should be a float."""
        _mock_anthropic(mocker)

        result = analyze_news(
            company_news={"aapl": lambda: _make_news_df()},
            existing={},
            params=base_params,
        )

        assert isinstance(result["aapl"]["sentiment_score"], float)

    def test_key_topics_is_list(self, mocker, base_params) -> None:
        """key_topics should be a list."""
        _mock_anthropic(mocker)

        result = analyze_news(
            company_news={"aapl": lambda: _make_news_df()},
            existing={},
            params=base_params,
        )

        assert isinstance(result["aapl"]["key_topics"], list)

    def test_existing_callable_fresh_analysis_is_passed_through(
        self, mocker, base_params
    ) -> None:
        """Fresh existing analysis loaded via a callable should be returned as-is."""
        mock_client = _mock_anthropic(mocker)
        fresh = _BASE_ANALYSIS.copy()
        fresh["analysis_date"] = pd.Timestamp.now("UTC").tz_convert(None).isoformat()

        # existing is already the dict (not a callable) — also supported
        result = analyze_news(
            company_news={"aapl": lambda: _make_news_df()},
            existing={"aapl": fresh},
            params=base_params,
        )

        assert "aapl" in result
        mock_client.messages.create.assert_not_called()
