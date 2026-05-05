"""Nodes for the portfolio_performance pipeline.

Four-stage flow:
  1. compute_strategy_returns  — join holdings x OHLCV -> daily portfolio returns
  2. compute_performance_metrics — Sharpe, max drawdown, cumulative return
  3. compile_report             — merge all strategy metrics, returns, and holdings
  4. send_performance_email     — chart + breakdown + KPI table via SMTP
"""

from __future__ import annotations

import base64
import contextlib
import io
import logging
import math
import os
import smtplib
from collections.abc import Callable
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

import matplotlib as mpl
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

from rdd.schemas.portfolio_holdings import PortfolioHoldingsSchema

mpl.use("Agg")

logger = logging.getLogger(__name__)

_TRADING_DAYS_PER_YEAR = 252


# ── helpers ───────────────────────────────────────────────────────────────────


def _load_ohlcv(ohlcv_existing: dict[str, Callable[[], pd.DataFrame]]) -> pd.DataFrame:
    frames = []
    for loader in ohlcv_existing.values():
        with contextlib.suppress(Exception):
            frames.append(loader())
    if not frames:
        return pd.DataFrame(columns=["ticker", "date", "adj_close"])
    return pd.concat(frames, ignore_index=True)


def _load_holdings(
    holdings_existing: dict[str, Callable[[], pd.DataFrame]],
) -> pd.DataFrame:
    frames = []
    for loader in holdings_existing.values():
        with contextlib.suppress(Exception):
            frames.append(loader() if callable(loader) else loader)
    if not frames:
        return pd.DataFrame(columns=["strategy", "date", "ticker", "weight"])
    df = pd.concat(frames, ignore_index=True)
    return PortfolioHoldingsSchema.validate(df)


# ── node 1: compute_strategy_returns ─────────────────────────────────────────


def _transaction_cost_fraction(
    rebalance_dates: list,
    holdings: pd.DataFrame,
    prices: pd.DataFrame,
    params: dict,
) -> dict:
    """Return a {rebalance_date: cost_fraction} dict for all rebalances.

    Cost fraction = total_commissions / portfolio_size, ready to subtract from
    the portfolio return on each rebalance day.

    IBKR Pro Tiered model: max(min_commission, commission_per_share * shares)
    applied once per position changed (buys AND sells are each one leg).
    """
    commission_per_share = float(params.get("commission_per_share_usd", 0.0))
    min_commission = float(params.get("min_commission_per_order_usd", 0.0))
    portfolio_size = float(params.get("assumed_portfolio_size_usd", 100_000.0))
    broker_name = params.get("broker_name", "")

    if commission_per_share <= 0 or portfolio_size <= 0:
        return {}

    if broker_name:
        logger.info("Applying transaction costs: %s", broker_name)

    prices_wide = prices.pivot_table(
        index="date", columns="ticker", values="adj_close", aggfunc="last"
    )

    prev_weights: dict[str, float] = {}
    cost_by_date: dict = {}

    for rb_date in rebalance_dates:
        rb_w = (
            holdings[holdings["date"] == rb_date]
            .set_index("ticker")["weight"]
            .to_dict()
        )
        all_tickers = set(prev_weights) | set(rb_w)
        total_cost = 0.0

        for ticker in all_tickers:
            delta = abs(rb_w.get(ticker, 0.0) - prev_weights.get(ticker, 0.0))
            if delta < 1e-8:
                continue

            # Price on rebalance date, falling back to most-recent prior price.
            price = float("nan")
            if rb_date in prices_wide.index and ticker in prices_wide.columns:
                price = float(prices_wide.at[rb_date, ticker])
            if pd.isna(price):
                avail = prices[
                    (prices["ticker"] == ticker) & (prices["date"] <= rb_date)
                ]
                if not avail.empty:
                    price = float(avail["adj_close"].iloc[-1])
            if pd.isna(price) or price <= 0:
                continue

            shares = delta * portfolio_size / price
            total_cost += max(min_commission, commission_per_share * shares)

        cost_fraction = total_cost / portfolio_size
        cost_by_date[rb_date] = cost_fraction
        logger.debug(
            "Rebalance %s: total_commission=%.2f  cost_fraction=%.6f",
            rb_date.date(),
            total_cost,
            cost_fraction,
        )
        prev_weights = rb_w

    return cost_by_date


def compute_strategy_returns(
    holdings_existing: dict[str, Callable[[], pd.DataFrame]],
    ohlcv_existing: dict[str, Callable[[], pd.DataFrame]],
    params: dict | None = None,
) -> pd.DataFrame:
    """Compute daily portfolio returns using buy-and-hold between rebalances.

    Transaction costs are deducted on each rebalance date using broker
    parameters from ``params`` (``commission_per_share_usd``,
    ``min_commission_per_order_usd``, ``assumed_portfolio_size_usd``).

    Args:
        holdings_existing: Date-partitioned holdings DataFrames for one strategy.
        ohlcv_existing: Ticker-partitioned OHLCV DataFrames.
        params: Optional parameter dict.  ``lookback_months`` (default 3) caps
            how far back the returned series reaches.

    Returns:
        DataFrame with columns ``date``, ``portfolio_return``.
    """
    lookback_months: int = int((params or {}).get("lookback_months", 3))

    holdings = _load_holdings(holdings_existing)
    if holdings.empty:
        logger.warning("No holdings found — returning empty returns series.")
        return pd.DataFrame(columns=["date", "portfolio_return"])

    ohlcv = _load_ohlcv(ohlcv_existing)
    if ohlcv.empty:
        logger.warning("No OHLCV data found — returning empty returns series.")
        return pd.DataFrame(columns=["date", "portfolio_return"])

    # Apply lookback window — keep only rebalance dates within the window,
    # but retain the latest snapshot before the cutoff so the window always
    # starts with a known set of weights.
    holdings["date"] = pd.to_datetime(holdings["date"])
    cutoff = pd.Timestamp.now().normalize() - pd.DateOffset(months=lookback_months)
    recent_holdings = holdings[holdings["date"] >= cutoff]
    if recent_holdings.empty:
        # No rebalance inside the window — fall back to the most recent one
        latest_date = holdings["date"].max()
        recent_holdings = holdings[holdings["date"] == latest_date]
    holdings = recent_holdings

    ohlcv["date"] = pd.to_datetime(ohlcv["date"])
    ohlcv = ohlcv[ohlcv["date"] >= cutoff]

    prices = (
        ohlcv[["ticker", "date", "adj_close"]]
        .dropna(subset=["adj_close"])
        .sort_values(["ticker", "date"])
    )
    prices["daily_return"] = prices.groupby("ticker")["adj_close"].pct_change()
    returns_wide = prices.pivot(index="date", columns="ticker", values="daily_return")

    rebalance_dates = sorted(holdings["date"].unique())
    all_dates = returns_wide.index.sort_values()

    weight_frames = []
    for i, rb_date in enumerate(rebalance_dates):
        next_rb = (
            rebalance_dates[i + 1]
            if i + 1 < len(rebalance_dates)
            else all_dates.max() + pd.Timedelta(days=1)
        )
        mask = (all_dates >= rb_date) & (all_dates < next_rb)
        period_dates = all_dates[mask]
        if period_dates.empty:
            continue
        rb_weights = holdings[holdings["date"] == rb_date].set_index("ticker")["weight"]
        for d in period_dates:
            weight_frames.append({"date": d, **rb_weights.to_dict()})

    if not weight_frames:
        return pd.DataFrame(columns=["date", "portfolio_return"])

    weights_wide = pd.DataFrame(weight_frames).set_index("date").fillna(0.0)
    common_tickers = weights_wide.columns.intersection(returns_wide.columns)
    weights_aligned = weights_wide[common_tickers]
    returns_aligned = returns_wide[common_tickers].reindex(weights_aligned.index)

    portfolio_returns = (weights_aligned * returns_aligned).sum(axis=1).dropna()

    # Deduct transaction costs on each rebalance date.
    cost_by_date = _transaction_cost_fraction(
        rebalance_dates, holdings, prices, params or {}
    )
    for rb_date, cost_frac in cost_by_date.items():
        if rb_date in portfolio_returns.index:
            portfolio_returns[rb_date] -= cost_frac

    result = portfolio_returns.reset_index()
    result.columns = ["date", "portfolio_return"]
    logger.info("Computed %d daily return observations.", len(result))
    return result


# ── node 2: compute_performance_metrics ──────────────────────────────────────


def compute_performance_metrics(daily_returns: pd.DataFrame) -> pd.DataFrame:
    """Compute summary performance metrics from a daily returns series.

    Returns:
        Single-row DataFrame with columns: ``cumulative_return``,
        ``annualised_return``, ``annualised_volatility``, ``sharpe_ratio``,
        ``max_drawdown``, ``observation_days``.
    """
    if daily_returns.empty or "portfolio_return" not in daily_returns.columns:
        return pd.DataFrame(
            [
                {
                    "cumulative_return": float("nan"),
                    "annualised_return": float("nan"),
                    "annualised_volatility": float("nan"),
                    "sharpe_ratio": float("nan"),
                    "max_drawdown": float("nan"),
                    "observation_days": 0,
                }
            ]
        )

    r = daily_returns["portfolio_return"].dropna()
    n = len(r)
    cumulative = (1 + r).prod() - 1
    years = n / _TRADING_DAYS_PER_YEAR
    ann_return = (1 + cumulative) ** (1 / years) - 1 if years > 0 else float("nan")
    ann_vol = r.std() * (_TRADING_DAYS_PER_YEAR**0.5)
    sharpe = ann_return / ann_vol if ann_vol > 0 else float("nan")

    cum_series = (1 + r).cumprod()
    rolling_max = cum_series.cummax()
    drawdowns = cum_series / rolling_max - 1
    max_dd = drawdowns.min()

    return pd.DataFrame(
        [
            {
                "cumulative_return": round(cumulative, 6),
                "annualised_return": round(ann_return, 6),
                "annualised_volatility": round(ann_vol, 6),
                "sharpe_ratio": round(sharpe, 4),
                "max_drawdown": round(max_dd, 6),
                "observation_days": n,
            }
        ]
    )


# ── node 3: compile_report ────────────────────────────────────────────────────


def compile_report(**strategy_metrics: pd.DataFrame) -> pd.DataFrame:
    """Merge per-strategy metrics into a single comparison DataFrame.

    Args:
        **strategy_metrics: Keyword arguments keyed by strategy name, each a
            single-row metrics DataFrame from ``compute_performance_metrics``.

    Returns:
        DataFrame with one row per strategy and a ``strategy`` column.
    """
    rows = []
    for strategy, metrics_df in strategy_metrics.items():
        row = metrics_df.iloc[0].to_dict()
        row["strategy"] = strategy
        rows.append(row)
    report = pd.DataFrame(rows).set_index("strategy").reset_index()
    logger.info("Compiled performance report for %d strategies.", len(rows))
    return report


# ── node 4: send_performance_email ────────────────────────────────────────────


def _fmt_pct(val: float, digits: int = 2) -> str:
    if math.isnan(val):
        return "n/a"
    color = "green" if val >= 0 else "red"
    return f'<span style="color:{color}">{val * 100:+.{digits}f}%</span>'


def _fmt_float(val: float, digits: int = 2) -> str:
    if math.isnan(val):
        return "n/a"
    return f"{val:.{digits}f}"


def _chart_png_b64(daily_returns_by_strategy: dict[str, pd.DataFrame]) -> str:
    """Render a cumulative return line chart and return a base64-encoded PNG."""
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.axhline(0, color="#cccccc", linewidth=0.8)

    date_min, date_max = None, None
    for strategy, dr in daily_returns_by_strategy.items():
        if dr.empty or "portfolio_return" not in dr.columns:
            continue
        r = dr.set_index("date")["portfolio_return"].dropna().sort_index()
        cum = (1 + r).cumprod() - 1
        ax.plot(cum.index, cum * 100, linewidth=1.8, label=strategy)
        date_min = r.index.min() if date_min is None else min(date_min, r.index.min())
        date_max = r.index.max() if date_max is None else max(date_max, r.index.max())

    # Explicit month-year tick labels so the full date range is unambiguous.
    if date_min is not None and date_max is not None:
        span_days = (date_max - date_min).days
        if span_days > 365:
            ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        elif span_days > 90:
            ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
        else:
            ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))
        fig.autofmt_xdate(rotation=30, ha="right")

    ax.set_ylabel("Cumulative Return (%)")
    ax.set_title("Portfolio Performance — Cumulative Return")
    ax.legend(frameon=False)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120)
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode()


def _holdings_table_html(holdings_by_strategy: dict[str, pd.DataFrame]) -> str:
    """Build an HTML holdings breakdown section for all strategies."""
    if not holdings_by_strategy:
        return ""
    sections = []
    for strategy, df in holdings_by_strategy.items():
        if df.empty:
            continue
        latest_date = df["date"].max()
        latest = df[df["date"] == latest_date].sort_values("weight", ascending=False)
        rows = "".join(
            f"<tr><td>{r['ticker']}</td><td style='text-align:right'>{r['weight'] * 100:.1f}%</td></tr>"
            for _, r in latest.iterrows()
        )
        sections.append(
            f"<h3 style='margin-top:24px'>{strategy} — holdings as of {latest_date.date()}</h3>"
            f"<table border='1' cellpadding='5' cellspacing='0' style='border-collapse:collapse;font-family:monospace;min-width:260px'>"
            f"<thead style='background:#f0f0f0'><tr><th>Ticker</th><th>Weight</th></tr></thead>"
            f"<tbody>{rows}</tbody></table>"
        )
    return "\n".join(sections)


def _fmt_strategy_name(name: str) -> str:
    return name.replace("_", " ").title()


def _kpi_table_html(report: pd.DataFrame) -> str:
    rows_html = ""
    for _, row in report.iterrows():
        rows_html += (
            f"<tr>"
            f"<td><b>{_fmt_strategy_name(row['strategy'])}</b></td>"
            f"<td style='text-align:right'>{_fmt_pct(row['cumulative_return'])}</td>"
            f"<td style='text-align:right'>{_fmt_pct(row['annualised_return'])}</td>"
            f"<td style='text-align:right'>{_fmt_pct(row['annualised_volatility'])}</td>"
            f"<td style='text-align:right'>{_fmt_float(row['sharpe_ratio'])}</td>"
            f"<td style='text-align:right'>{_fmt_pct(row['max_drawdown'])}</td>"
            f"<td style='text-align:right'>{int(row['observation_days'])}</td>"
            f"</tr>\n"
        )
    return (
        "<table border='1' cellpadding='6' cellspacing='0' "
        "style='border-collapse:collapse;font-family:monospace;'>"
        "<thead style='background:#f0f0f0'>"
        "<tr><th>Strategy</th><th>Cum. Return</th><th>Ann. Return</th>"
        "<th>Ann. Volatility</th><th>Sharpe</th><th>Max Drawdown</th><th>Days</th></tr>"
        "</thead>"
        f"<tbody>{rows_html}</tbody></table>"
    )


def _descriptions_html(strategy_descriptions: dict[str, str]) -> str:
    if not strategy_descriptions:
        return ""
    items = "".join(
        f"<dt style='font-weight:bold;margin-top:12px'>{_fmt_strategy_name(name)}</dt>"
        f"<dd style='margin:4px 0 0 16px;color:#444'>{desc.strip()}</dd>"
        for name, desc in strategy_descriptions.items()
    )
    return (
        "<h3 style='margin-top:32px'>Strategy Descriptions</h3>"
        f"<dl style='font-family:Arial,sans-serif;font-size:13px;line-height:1.5'>{items}</dl>"
    )


def _build_html(
    report: pd.DataFrame,
    chart_b64: str,
    holdings_html: str,
    strategy_descriptions: dict[str, str] | None = None,
) -> str:
    img_tag = (
        f'<img src="data:image/png;base64,{chart_b64}" '
        f'style="max-width:100%;margin:16px 0" alt="Cumulative return chart"/>'
        if chart_b64
        else ""
    )
    desc_html = _descriptions_html(strategy_descriptions or {})
    return f"""
<html>
<body style="font-family:Arial,sans-serif;max-width:900px;margin:auto;padding:24px">
  <h2 style="border-bottom:2px solid #333;padding-bottom:8px">
    RDD Weekly Portfolio Performance
  </h2>

  <h3>Cumulative Return</h3>
  {img_tag}

  <h3>Key Performance Indicators</h3>
  {_kpi_table_html(report)}

  {holdings_html}

  {desc_html}

  <p style="color:#888;font-size:11px;margin-top:32px">
    Generated by RDD · {pd.Timestamp.now().strftime("%Y-%m-%d %H:%M UTC")}
  </p>
</body>
</html>
"""


def send_performance_email(
    report: pd.DataFrame,
    params: dict[str, Any],
    **strategy_data: Any,
) -> None:
    """Format and send the enriched weekly performance email.

    Accepts optional keyword arguments ``{strategy}_returns`` and
    ``{strategy}_holdings`` injected by the pipeline for chart and
    breakdown generation.

    Args:
        report: Output of ``compile_report``.
        params: ``portfolio_performance`` parameter block.
        **strategy_data: Optional ``{strategy}_returns`` (daily returns DataFrame)
            and ``{strategy}_holdings`` (holdings DataFrame) per strategy.
    """
    to_addr = params.get("email_to") or os.environ.get("RDD_EMAIL_TO", "")
    smtp_host = params.get("smtp_host") or os.environ.get("RDD_SMTP_HOST", "")
    smtp_port = int(params.get("smtp_port") or os.environ.get("RDD_SMTP_PORT", "465"))
    smtp_user = params.get("smtp_user") or os.environ.get("RDD_SMTP_USER", "")
    smtp_pass = params.get("smtp_pass") or os.environ.get("RDD_SMTP_PASS", "")

    if not all([to_addr, smtp_host, smtp_user, smtp_pass]):
        logger.warning("Email config incomplete — skipping send.")
        return

    # Extract per-strategy returns and holdings passed via pipeline inputs.
    daily_returns_by_strategy: dict[str, pd.DataFrame] = {
        k.replace("_returns", ""): v
        for k, v in strategy_data.items()
        if k.endswith("_returns") and isinstance(v, pd.DataFrame)
    }
    holdings_by_strategy: dict[str, pd.DataFrame] = {
        k.replace("_holdings", ""): v
        for k, v in strategy_data.items()
        if k.endswith("_holdings") and isinstance(v, pd.DataFrame)
    }

    try:
        chart_b64 = _chart_png_b64(daily_returns_by_strategy)
    except Exception:
        logger.warning(
            "Chart generation failed — email will omit chart.", exc_info=True
        )
        chart_b64 = ""

    holdings_html = _holdings_table_html(holdings_by_strategy)
    strategy_descriptions: dict[str, str] = params.get("strategy_descriptions", {})
    html = _build_html(report, chart_b64, holdings_html, strategy_descriptions)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = "RDD Weekly Portfolio Performance"
    msg["From"] = smtp_user
    msg["To"] = to_addr
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP_SSL(smtp_host, smtp_port) as server:
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, to_addr, msg.as_string())
        logger.info("Performance email sent to %s.", to_addr)
    except Exception:
        logger.error("Failed to send performance email.", exc_info=True)
