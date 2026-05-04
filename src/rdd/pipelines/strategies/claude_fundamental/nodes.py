"""Nodes for the portfolio_construction pipeline (Claude agent-driven).

Three-stage decision flow:
  1. score_tickers   — Haiku scores every ticker in the universe (~$0.80/month)
  2. construct_portfolio — Sonnet selects 10 holdings with conviction weights (~$0.03/month)
  3. rebalance_portfolio — Haiku decides which trades to execute (~$0.01/month)
"""

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
from collections.abc import Callable
from typing import Any

import anthropic
import pandas as pd

from rdd.schemas.portfolio_holdings import PortfolioHoldingsSchema

logger = logging.getLogger(__name__)


# ── API client ────────────────────────────────────────────────────────────────


def _resolve_api_key() -> str:
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
        msg = "ANTHROPIC_API_KEY not set and no apiKeyHelper found in ~/.claude/settings.json."
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


def _call_claude(
    client: anthropic.Anthropic,
    prompt: str,
    model: str,
    max_tokens: int,
) -> str:
    message = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text.strip()


def _parse_json_response(text: str) -> dict[str, Any]:
    """Parse a Claude response as JSON, stripping markdown fences if present."""
    clean = re.sub(r"```(?:json)?\s*", "", text).strip().rstrip("`").strip()
    return json.loads(clean)


# ── Ticker brief ──────────────────────────────────────────────────────────────


def _build_ticker_brief(  # noqa: PLR0912
    ticker: str,
    info: pd.DataFrame | None,
    valuation: pd.DataFrame | None,
    consensus: pd.DataFrame | None,
    earnings: pd.DataFrame | None,
    financials: pd.DataFrame | None,
    strategy_signals: dict[str, Any] | None,
    news: dict[str, Any] | None,
) -> str:
    """Compile all available data into a compact ~400-token text brief."""
    parts: list[str] = [f"TICKER: {ticker}"]

    if info is not None and not info.empty:
        row = info.iloc[0]
        mcap = row.get("market_cap")
        mcap_str = f"${mcap / 1e9:.1f}B" if pd.notna(mcap) else "n/a"
        parts.append(
            f"Company: {row.get('name', 'n/a')} | Sector: {row.get('sector', 'n/a')}"
            f" | Industry: {row.get('industry', 'n/a')} | Market cap: {mcap_str}"
        )

    if strategy_signals and "signals" in strategy_signals:
        sig_lines = [
            f"  {s['strategy']}: {s['direction']}" for s in strategy_signals["signals"]
        ]
        parts.append("Strategy signals:\n" + "\n".join(sig_lines))

    if valuation is not None and not valuation.empty:
        row = valuation.iloc[-1]
        vlines = []
        for col, label in [
            ("pe_ratio", "P/E"),
            ("ev_ebitda", "EV/EBITDA"),
            ("pb_ratio", "P/B"),
            ("gross_margin", "Gross margin"),
            ("operating_margin", "Op margin"),
            ("free_cash_flow_yield", "FCF yield"),
        ]:
            val = row.get(col)
            if pd.notna(val):
                vlines.append(f"  {label}: {val:.2f}")
        if vlines:
            parts.append("Valuation:\n" + "\n".join(vlines))

    if financials is not None and not financials.empty:
        latest = financials.sort_values("period_end").iloc[-1]
        flines = []
        for col, label in [
            ("total_revenue", "Revenue"),
            ("operating_income", "Op income"),
            ("free_cash_flow", "FCF"),
            ("net_debt", "Net debt"),
        ]:
            val = latest.get(col)
            if pd.notna(val):
                flines.append(f"  {label}: ${val / 1e9:.2f}B")
        if flines:
            parts.append(
                f"Latest quarter ({latest['period_end'].date()}):\n" + "\n".join(flines)
            )

    if consensus is not None and not consensus.empty:
        row = consensus.iloc[-1]
        current = row.get("current_price")
        target = row.get("target_mean_price")
        if pd.notna(current) and pd.notna(target) and current > 0:
            upside = f"{(target / current - 1) * 100:.1f}%"
        else:
            upside = "n/a"
        target_str = f"${target:.0f}" if pd.notna(target) else "n/a"
        parts.append(
            f"Analyst consensus: {row.get('recommendation_key', 'n/a')}"
            f" | Mean target: {target_str} | Upside: {upside}"
            f" | Analysts: {int(row.get('analyst_count', 0)) if pd.notna(row.get('analyst_count')) else 'n/a'}"
        )

    if earnings is not None and not earnings.empty:
        recent = earnings.sort_values("earnings_date").tail(4)
        surprises = [
            f"{row['surprise_pct']:+.1f}%"
            for _, row in recent.iterrows()
            if pd.notna(row.get("surprise_pct"))
        ]
        if surprises:
            parts.append(
                f"EPS surprises (newest first): {', '.join(reversed(surprises))}"
            )

    if news and isinstance(news, dict):
        bull = news.get("bull_report", "")
        conviction_match = re.search(r"conviction\s+(\d)/5", bull, re.IGNORECASE)
        conviction = conviction_match.group(1) if conviction_match else "n/a"
        target_match = re.search(r"price target[^$]*\$(\d+)", bull, re.IGNORECASE)
        price_target = f"${target_match.group(1)}" if target_match else "n/a"
        parts.append(
            f"News analysis: bull conviction {conviction}/5 | implied target {price_target}"
        )

    return "\n".join(parts)


# ── Prompts ───────────────────────────────────────────────────────────────────

_SCORE_PROMPT = """\
You are a quantitative analyst screening stocks for a long-only equity portfolio.

Review the data brief below for {ticker} and assign:
1. A score from 1.0 to 10.0 (10 = strongest buy candidate)
2. A verdict: STRONG_BUY, BUY, HOLD, or AVOID
3. A one-sentence investment thesis (max 30 words)

Respond with valid JSON only, no markdown fences:
{{"score": <float>, "verdict": "<str>", "thesis": "<str>"}}

Data brief:
{brief}
"""

_SELECT_PROMPT = """\
You are a portfolio manager constructing a 10-stock long-only equity portfolio.

HARD CONSTRAINTS:
- Exactly {max_holdings} holdings
- Weights must sum to exactly 1.0
- Each weight between {min_weight:.0%} and {max_weight:.0%}
- No single industry may exceed {max_industry_weight:.0%} of the total portfolio

CANDIDATES (top {n} by screening score, sorted descending):
{candidates_text}

Select the best {max_holdings} holdings. Higher-conviction picks should receive higher weights.
Write a one-sentence thesis per holding. Include a 2-3 sentence portfolio thesis explaining
the overall composition and the main themes.

Respond with valid JSON only, no markdown fences:
{{
  "holdings": [
    {{"ticker": "<str>", "weight": <float>, "sector": "<str>",
      "industry": "<str>", "thesis": "<str>"}}
  ],
  "industry_breakdown": {{"<industry>": <float>}},
  "portfolio_thesis": "<str>"
}}
"""

_REBALANCE_PROMPT = """\
You are a portfolio manager deciding which trades to execute this month.

LIVE PORTFOLIO (what is currently held):
{current_text}

PROPOSED PORTFOLIO (what the model wants):
{proposed_text}

Continuity note: existing holdings have had their scores boosted by {continuity_bonus:.2f}
points (on a 10-point scale) to represent avoided transaction costs. Only replace a current
holding if the new candidate meaningfully outperforms net of this friction.

For each ticker that appears in either portfolio, specify an action:
  BUY      - open a new position not currently held
  SELL     - exit a position entirely
  HOLD     - keep at current weight, no trade needed
  INCREASE - add to an existing position (provide target_weight)
  DECREASE - trim an existing position (provide target_weight)

Respond with valid JSON only, no markdown fences:
{{
  "trades": [
    {{"ticker": "<str>", "action": "<str>",
      "current_weight": <float or null>, "target_weight": <float or null>,
      "reasoning": "<str>"}}
  ],
  "estimated_turnover_pct": <float>
}}
"""


# ── Helpers ───────────────────────────────────────────────────────────────────


def _load_partitioned(dataset: dict[str, Any], key: str) -> pd.DataFrame | None:
    """Load one partition from a partitioned dataset, returning None on failure."""
    if key not in dataset:
        return None
    try:
        val = dataset[key]
        return val() if callable(val) else val
    except Exception:
        logger.warning("Could not load partition %s.", key)
        return None


# ── Node functions ────────────────────────────────────────────────────────────


def score_tickers(
    valuation_ratios: dict[str, Callable[[], pd.DataFrame]],
    analyst_consensus: dict[str, Callable[[], pd.DataFrame]],
    earnings_history: dict[str, Callable[[], pd.DataFrame]],
    company_info: dict[str, Callable[[], pd.DataFrame]],
    company_financials: dict[str, Callable[[], pd.DataFrame]],
    stock_analyses: dict[str, Any],
    news_analysis: dict[str, Any],
    params: dict[str, Any],
) -> dict[str, Any]:
    """Stage 1: call Claude Haiku once per ticker to score the full universe.

    Merges all available data sources into a compact ticker brief, then asks
    Claude to assign a score (1-10), a verdict (STRONG_BUY / BUY / HOLD / AVOID),
    and a one-sentence thesis. Tickers with no data at all are skipped.
    API failures per ticker are logged and that ticker is skipped.

    Args:
        valuation_ratios: Partitioned valuation ratio DataFrames per ticker.
        analyst_consensus: Partitioned analyst consensus DataFrames per ticker.
        earnings_history: Partitioned earnings history DataFrames per ticker.
        company_info: Partitioned company info DataFrames per ticker.
        company_financials: Partitioned quarterly financials DataFrames per ticker.
        stock_analyses: Plain dict of strategy signal dicts keyed by ticker.
        news_analysis: Partitioned news analysis JSON dicts per ticker.
        params: ``portfolio_construction`` parameter block.

    Returns:
        Dict keyed by lowercase ticker with score, verdict, thesis, scored_at.
    """
    model = str(params["model_screening"])
    max_tokens = int(params.get("max_tokens_screening", 512))

    all_keys: set[str] = set()
    for ds in (
        valuation_ratios,
        analyst_consensus,
        earnings_history,
        company_info,
        company_financials,
    ):
        all_keys.update(ds.keys())
    all_keys.update(k.lower() for k in stock_analyses)
    all_keys.update(k.lower() for k in news_analysis)

    client = _make_client()
    result: dict[str, Any] = {}

    for key in sorted(all_keys):
        ticker = key.upper()

        info_df = _load_partitioned(company_info, key)
        val_df = _load_partitioned(valuation_ratios, key)
        cons_df = _load_partitioned(analyst_consensus, key)
        earn_df = _load_partitioned(earnings_history, key)
        fin_df = _load_partitioned(company_financials, key)

        signals = stock_analyses.get(key) or stock_analyses.get(ticker)

        news_entry = None
        if key in news_analysis:
            try:
                raw = news_analysis[key]
                news_entry = raw() if callable(raw) else raw
            except Exception:
                logger.warning("Could not load news analysis for %s.", ticker)

        brief = _build_ticker_brief(
            ticker=ticker,
            info=info_df,
            valuation=val_df,
            consensus=cons_df,
            earnings=earn_df,
            financials=fin_df,
            strategy_signals=signals,
            news=news_entry,
        )

        try:
            raw_response = _call_claude(
                client,
                _SCORE_PROMPT.format(ticker=ticker, brief=brief),
                model,
                max_tokens,
            )
            scored = _parse_json_response(raw_response)
            result[key] = {
                "ticker": ticker,
                "score": float(scored["score"]),
                "verdict": str(scored["verdict"]),
                "thesis": str(scored["thesis"]),
                "scored_at": pd.Timestamp.now("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        except Exception:
            logger.warning("Scoring failed for %s — skipping.", ticker, exc_info=True)

    logger.info("Scored %d tickers.", len(result))
    return result


def construct_portfolio(
    ticker_scores: dict[str, Any],
    company_info: dict[str, Callable[[], pd.DataFrame]],
    params: dict[str, Any],
) -> dict[str, Any]:
    """Stage 2: single Claude Sonnet call to select 10 holdings with weights.

    Takes the top-N scored tickers, enriches each with sector/industry from
    company_info, and asks Claude to build a constraint-satisfying portfolio.
    Weights are conviction-proportional (5-20% per position, sum to 1.0).

    Args:
        ticker_scores: Output of score_tickers — dict keyed by lowercase ticker.
        company_info: Partitioned company info DataFrames per ticker.
        params: ``portfolio_construction`` parameter block.

    Returns:
        Portfolio dict with holdings, industry_breakdown, portfolio_thesis, generated_at.
    """
    model = str(params["model_selection"])
    max_tokens = int(params.get("max_tokens_selection", 4096))
    top_n = int(params.get("top_n_candidates", 30))
    max_holdings = int(params.get("max_holdings", 10))
    max_industry_weight = float(params.get("max_industry_weight", 0.35))
    min_weight = float(params.get("min_position_weight", 0.05))
    max_weight = float(params.get("max_position_weight", 0.20))

    ranked = sorted(ticker_scores.values(), key=lambda x: x["score"], reverse=True)[
        :top_n
    ]

    def _sector_info(key: str) -> tuple[str, str, str]:
        df = _load_partitioned(company_info, key)
        if df is None or df.empty:
            return "Unknown", "Unknown", "n/a"
        row = df.iloc[0]
        mcap = row.get("market_cap")
        mcap_str = f"${mcap / 1e9:.1f}B" if pd.notna(mcap) else "n/a"
        return (
            str(row.get("sector") or "Unknown"),
            str(row.get("industry") or "Unknown"),
            mcap_str,
        )

    candidate_lines = []
    for i, cand in enumerate(ranked, 1):
        sector, industry, mcap = _sector_info(cand["ticker"].lower())
        candidate_lines.append(
            f"{i:2}. {cand['ticker']:<6} score={cand['score']:.1f}"
            f" verdict={cand['verdict']}"
            f" | {sector} / {industry} | mkt cap {mcap}"
            f"\n    {cand['thesis']}"
        )

    prompt = _SELECT_PROMPT.format(
        max_holdings=max_holdings,
        min_weight=min_weight,
        max_weight=max_weight,
        max_industry_weight=max_industry_weight,
        n=len(ranked),
        candidates_text="\n".join(candidate_lines),
    )

    client = _make_client()
    raw = _call_claude(client, prompt, model, max_tokens)
    portfolio = _parse_json_response(raw)
    portfolio["generated_at"] = pd.Timestamp.now("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info(
        "Portfolio constructed: %d holdings.", len(portfolio.get("holdings", []))
    )
    return portfolio


def rebalance_portfolio(
    portfolio_allocation: dict[str, Any],
    live_portfolio: dict[str, Any],
    ticker_scores: dict[str, Any],
    params: dict[str, Any],
) -> dict[str, Any]:
    """Stage 3: single Claude Haiku call deciding which trades to execute.

    Compares the freshly constructed portfolio against the live portfolio
    (what is actually held). Applies a continuity bonus to existing holdings
    so the model prefers holding over replacing unless a new candidate clearly
    outperforms net of transaction friction.

    On the first run (live_portfolio == {}), all proposed holdings become BUY trades.

    Args:
        portfolio_allocation: Output of construct_portfolio for this run.
        live_portfolio: Current live holdings from NullableJSONDataset (empty on first run).
        ticker_scores: Output of score_tickers — used to surface adjusted scores.
        params: ``portfolio_construction`` parameter block.

    Returns:
        Dict with trades list, estimated_turnover_pct, rebalanced_at.
    """
    model = str(params["model_rebalancing"])
    max_tokens = int(params.get("max_tokens_rebalancing", 2048))
    continuity_bonus = float(params.get("continuity_bonus", 0.08))

    current_holdings = live_portfolio.get("holdings", [])

    if not current_holdings:
        trades = [
            {
                "ticker": h["ticker"],
                "action": "BUY",
                "current_weight": None,
                "target_weight": h["weight"],
                "reasoning": "Initial portfolio construction — no prior holdings.",
            }
            for h in portfolio_allocation.get("holdings", [])
        ]
        return {
            "trades": trades,
            "estimated_turnover_pct": 100.0,
            "rebalanced_at": pd.Timestamp.now("UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    current_tickers = {h["ticker"].upper() for h in current_holdings}
    adjusted_scores: dict[str, float] = {
        scored["ticker"]: min(
            10.0,
            scored["score"]
            + (
                continuity_bonus * 10
                if scored["ticker"].upper() in current_tickers
                else 0.0
            ),
        )
        for scored in ticker_scores.values()
    }

    def _holdings_text(holdings: list[dict[str, Any]]) -> str:
        lines = []
        for h in holdings:
            adj = adjusted_scores.get(h["ticker"], "n/a")
            adj_str = f"{adj:.1f}" if isinstance(adj, float) else str(adj)
            lines.append(
                f"  {h['ticker']:<6} weight={h['weight']:.1%}"
                f" | {h.get('sector', '?')} / {h.get('industry', '?')}"
                f" | adj score={adj_str}"
                f" | {h.get('thesis', '')}"
            )
        return "\n".join(lines)

    prompt = _REBALANCE_PROMPT.format(
        current_text=_holdings_text(current_holdings),
        proposed_text=_holdings_text(portfolio_allocation.get("holdings", [])),
        continuity_bonus=continuity_bonus * 10,
    )

    client = _make_client()
    raw = _call_claude(client, prompt, model, max_tokens)
    decision = _parse_json_response(raw)
    decision["rebalanced_at"] = pd.Timestamp.now("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")

    logger.info(
        "Rebalancing: %d trades, ~%.1f%% turnover.",
        len(decision.get("trades", [])),
        decision.get("estimated_turnover_pct", 0.0),
    )
    return decision


def record_holdings(
    portfolio_allocation: dict[str, Any],
    params: dict[str, Any],
) -> dict[str, pd.DataFrame]:
    """Convert a portfolio allocation into a schema-validated holdings snapshot.

    Emits a single date-keyed partition so the holdings history for this strategy
    grows by one row-group per run without overwriting prior snapshots.

    Args:
        portfolio_allocation: Output of construct_portfolio.
        params: ``portfolio_construction`` parameter block.  Must contain
            ``strategy_name`` (str).

    Returns:
        Dict mapping ``YYYY-MM-DD`` → long-format DataFrame conforming to
        :class:`~rdd.schemas.portfolio_holdings.PortfolioHoldingsSchema`.
    """
    strategy = str(params["strategy_name"])
    date = pd.Timestamp(portfolio_allocation.get("generated_at", pd.Timestamp.now("UTC"))).tz_localize(None).normalize()
    date_key = date.strftime("%Y-%m-%d")

    rows = [
        {
            "strategy": strategy,
            "date": date,
            "ticker": h["ticker"].upper(),
            "weight": float(h["weight"]),
        }
        for h in portfolio_allocation.get("holdings", [])
    ]
    df = PortfolioHoldingsSchema.validate(pd.DataFrame(rows))
    logger.info("Recorded %d holdings for %s on %s.", len(df), strategy, date_key)
    return {date_key: df}
