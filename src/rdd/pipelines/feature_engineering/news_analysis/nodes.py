"""Nodes for the news analysis pipeline (Anthropic Claude GenAI)."""

from __future__ import annotations

import json
import logging
import os
import subprocess
from collections.abc import Callable
from typing import Any

import anthropic
import pandas as pd

logger = logging.getLogger(__name__)

_REQUIRED_KEYS = {
    "ticker",
    "analysis_date",
    "article_count",
    "lookback_days",
    "bull_report",
    "bear_report",
}

_BULL_PROMPT = """\
You are a seasoned buy-side analyst making the bull case for {ticker}.

Below are {article_count} news articles from the past {lookback_days} days. \
Read them carefully and write a thorough bullish investment report. \
Cite specific articles (by headline or date) to support each point. \
Cover: key growth catalysts, positive developments, why bears are wrong, \
and what the news implies for the stock price.

News articles:
{articles_text}

Write the report in plain prose (no JSON, no bullet points). Be thorough.
"""

_BEAR_PROMPT = """\
You are a seasoned short-seller making the bear case for {ticker}.

Below are {article_count} news articles from the past {lookback_days} days. \
Read them carefully and write a thorough bearish investment report. \
Cite specific articles (by headline or date) to support each point. \
Cover: key risks and headwinds, negative developments, why bulls are wrong, \
and what the news implies for the stock price.

News articles:
{articles_text}

Write the report in plain prose (no JSON, no bullet points). Be thorough.
"""


def _resolve_api_key() -> str:
    """Return an Anthropic API key.

    Tries ANTHROPIC_API_KEY first; falls back to the apiKeyHelper script
    configured in ~/.claude/settings.json (McKinsey AI-gateway pattern).
    """
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if key:
        return key

    settings_path = os.path.expanduser("~/.claude/settings.json")
    try:
        with open(settings_path) as f:
            helper = json.load(f).get("apiKeyHelper", "")
    except Exception:
        helper = ""

    if not helper:
        msg = "ANTHROPIC_API_KEY is not set and no apiKeyHelper found in ~/.claude/settings.json."
        raise OSError(msg)

    result = subprocess.run([helper], capture_output=True, text=True, check=True)  # noqa: S603
    return result.stdout.strip()


def _make_client() -> anthropic.Anthropic:
    api_key = _resolve_api_key()
    base_url = os.environ.get("ANTHROPIC_BASE_URL")
    kwargs: dict[str, str] = {"api_key": api_key}
    if base_url:
        kwargs["base_url"] = base_url
    return anthropic.Anthropic(**kwargs)


def _articles_to_text(df: pd.DataFrame, lookback_days: int) -> tuple[str, int]:
    """Convert a news DataFrame to a numbered text block for the prompt.

    Returns the formatted text and the count of articles included.
    """
    cutoff = pd.Timestamp.now("UTC").tz_convert(None) - pd.Timedelta(days=lookback_days)
    recent = df[df["published_at"] >= cutoff].sort_values(
        "published_at", ascending=False
    )
    if recent.empty:
        return "", 0

    lines = []
    for i, (_, row) in enumerate(recent.iterrows(), start=1):
        headline = row.get("headline") or "(no headline)"
        summary = row.get("summary") or "(no summary)"
        pub = row.get("published_at", "")
        lines.append(f"{i}. [{pub}] {headline}\n   {summary}")
    return "\n\n".join(lines), len(recent)


def _is_fresh(analysis: dict[str, Any], cutoff: pd.Timestamp) -> bool:
    """Return True if the existing analysis was done after *cutoff*."""
    date_str = analysis.get("analysis_date", "")
    if not date_str:
        return False
    try:
        ts = pd.Timestamp(date_str).tz_localize(None)
        return ts >= cutoff
    except Exception:
        return False


def _call_report(
    client: anthropic.Anthropic,
    prompt: str,
    model: str,
    max_tokens: int,
) -> str:
    """Call the Claude API and return the plain-text report."""
    message = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text.strip()


def analyze_news(
    company_news: dict[str, Callable[[], pd.DataFrame]],
    existing: dict[str, Any],
    params: dict[str, Any],
) -> dict[str, Any]:
    """Produce a bull report and a bear report for each ticker using Claude.

    Makes two separate API calls per ticker — one dedicated bull analyst call
    and one dedicated bear analyst call — so each perspective gets the full
    output budget rather than sharing it.

    Skips tickers whose existing analysis is fresher than ``params.refresh_hours``.
    A Claude API failure for one ticker is logged and skipped — other tickers are
    still processed.

    Args:
        company_news: Mapping of ticker (lowercase) → lazy DataFrame loader.
        existing: Mapping of ticker (lowercase) → lazy JSON loader for existing analyses.
        params: ``news_analysis`` parameter block from ``params_news_analysis.yml``.

    Returns:
        Mapping of ticker (lowercase) → analysis dict to persist as JSON.
    """
    lookback_days = int(params["lookback_days"])
    model = str(params["model"])
    refresh_hours = float(params["refresh_hours"])
    max_tokens = int(params.get("max_tokens", 2048))

    cutoff = pd.Timestamp.now("UTC").tz_convert(None) - pd.Timedelta(
        hours=refresh_hours
    )

    client = _make_client()
    result: dict[str, Any] = {}

    for partition_key, loader in company_news.items():
        ticker = partition_key.upper()

        # --- fresh check ---------------------------------------------------
        if partition_key in existing:
            try:
                existing_entry = existing[partition_key]
                if callable(existing_entry):
                    existing_entry = existing_entry()
                if _is_fresh(existing_entry, cutoff):
                    logger.debug("Skipping %s — analysis is fresh.", ticker)
                    result[partition_key] = existing_entry
                    continue
            except Exception:
                logger.warning(
                    "Could not load existing analysis for %s.", ticker, exc_info=True
                )

        # --- load and filter news ------------------------------------------
        try:
            df = loader()
        except Exception:
            logger.warning("Could not load news data for %s — skipping.", ticker)
            continue

        if df is None or df.empty:
            logger.debug("No news data for %s — skipping.", ticker)
            continue

        articles_text, article_count = _articles_to_text(df, lookback_days)
        if not articles_text:
            logger.debug(
                "No recent articles for %s within lookback — skipping.", ticker
            )
            continue

        # --- two dedicated Claude calls ------------------------------------
        try:
            bull_report = _call_report(
                client=client,
                prompt=_BULL_PROMPT.format(
                    ticker=ticker,
                    article_count=article_count,
                    lookback_days=lookback_days,
                    articles_text=articles_text,
                ),
                model=model,
                max_tokens=max_tokens,
            )
            bear_report = _call_report(
                client=client,
                prompt=_BEAR_PROMPT.format(
                    ticker=ticker,
                    article_count=article_count,
                    lookback_days=lookback_days,
                    articles_text=articles_text,
                ),
                model=model,
                max_tokens=max_tokens,
            )
        except Exception:
            logger.warning(
                "Claude API call failed for %s — skipping.", ticker, exc_info=True
            )
            continue

        analysis = {
            "ticker": ticker,
            "analysis_date": pd.Timestamp.now("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
            "article_count": article_count,
            "lookback_days": lookback_days,
            "bull_report": bull_report,
            "bear_report": bear_report,
        }

        result[partition_key] = analysis
        logger.debug("Analysis complete for %s (%d articles).", ticker, article_count)

    logger.info("News analysis complete. %d tickers written.", len(result))
    return result
