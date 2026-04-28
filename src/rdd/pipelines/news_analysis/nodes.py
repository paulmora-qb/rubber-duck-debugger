"""Nodes for the news analysis pipeline (Anthropic Claude GenAI)."""

from __future__ import annotations

import json
import logging
import os
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
    "sentiment_score",
    "bull_thesis",
    "bear_thesis",
    "discussion",
    "overall_assessment",
    "key_topics",
}

_ANALYSIS_PROMPT = """\
You are a financial news analyst. Analyse the following recent news articles for {ticker} \
and produce a structured investment thesis with both bullish and bearish perspectives.

News articles (most recent {lookback_days} days):
{articles_text}

Return ONLY a valid JSON object with exactly these keys (no markdown, no code fences):
{{
  "ticker": "{ticker}",
  "analysis_date": "<ISO8601 UTC timestamp, e.g. 2024-01-15T10:00:00Z>",
  "article_count": <integer count of articles analysed>,
  "lookback_days": {lookback_days},
  "sentiment_score": <float between -1.0 (very bearish) and 1.0 (very bullish)>,
  "bull_thesis": "<bull analyst's key points citing specific news>",
  "bear_thesis": "<bear analyst's key points citing specific news>",
  "discussion": "<realistic back-and-forth labelled 'Bull Analyst:' and 'Bear Analyst:', \
each citing specific articles>",
  "overall_assessment": "<synthesised conclusion balancing both views>",
  "key_topics": ["<topic1>", "<topic2>", "<topic3>"]
}}

Rules:
- sentiment_score must reflect the net balance of the news (-1.0 to 1.0).
- discussion must contain at least two exchanges (Bull → Bear → Bull or Bear → Bull → Bear).
- key_topics must have 3-5 strings.
- Return ONLY the JSON — no other text.
"""


def _make_client() -> anthropic.Anthropic:
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        msg = "ANTHROPIC_API_KEY environment variable is not set."
        raise OSError(msg)
    return anthropic.Anthropic(api_key=api_key)


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


def _call_claude(
    client: anthropic.Anthropic,
    ticker: str,
    articles_text: str,
    article_count: int,
    lookback_days: int,
    model: str,
) -> dict[str, Any]:
    """Call the Claude API for one ticker and return the parsed analysis dict."""
    prompt = _ANALYSIS_PROMPT.format(
        ticker=ticker,
        lookback_days=lookback_days,
        articles_text=articles_text,
    )
    message = client.messages.create(
        model=model,
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}],
    )
    raw_text = message.content[0].text.strip()
    analysis = json.loads(raw_text)
    # Ensure article_count reflects what was actually passed in
    analysis["article_count"] = article_count
    return analysis


def analyze_news(
    company_news: dict[str, Callable[[], pd.DataFrame]],
    existing: dict[str, Any],
    params: dict[str, Any],
) -> dict[str, Any]:
    """Analyse recent company news with a Claude bull/bear agent discussion.

    For each ticker in *company_news*, loads recent articles and asks Claude to
    produce a structured JSON analysis with bullish and bearish theses plus a
    simulated discussion between the two analysts.

    Skips tickers whose existing analysis is fresher than ``params.refresh_hours``.
    A Claude API failure for one ticker is logged and skipped — other tickers are
    still processed.

    Args:
        company_news: Mapping of ticker (lowercase) → lazy DataFrame loader from
            Kedro's ``PartitionedDataset``.
        existing: Mapping of ticker (lowercase) → lazy JSON loader (or already-
            loaded dict) from the ``NullablePartitionedDataset`` for existing
            analyses.
        params: ``news_analysis`` parameter block from ``params_news_analysis.yml``.

    Returns:
        Mapping of ticker (lowercase) → analysis dict for the PartitionedDataset
        to persist as JSON files.
    """
    lookback_days = int(params["lookback_days"])
    model = str(params["model"])
    refresh_hours = float(params["refresh_hours"])

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

        # --- load news data ------------------------------------------------
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

        # --- call Claude ---------------------------------------------------
        try:
            analysis = _call_claude(
                client=client,
                ticker=ticker,
                articles_text=articles_text,
                article_count=article_count,
                lookback_days=lookback_days,
                model=model,
            )
        except Exception:
            logger.warning(
                "Claude API call failed for %s — skipping.", ticker, exc_info=True
            )
            continue

        # --- basic validation ----------------------------------------------
        missing = _REQUIRED_KEYS - analysis.keys()
        if missing:
            logger.warning(
                "Claude response for %s missing keys %s — skipping.", ticker, missing
            )
            continue

        result[partition_key] = analysis
        logger.debug("Analysis complete for %s (%d articles).", ticker, article_count)

    logger.info("News analysis complete. %d tickers written.", len(result))
    return result
