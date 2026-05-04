"""Nodes for the portfolio_performance pipeline.

Four-stage flow:
  1. compute_strategy_returns  — join holdings × OHLCV → daily portfolio returns
  2. compute_performance_metrics — Sharpe, max drawdown, cumulative return
  3. compile_report             — merge all strategy metrics into one summary
  4. send_performance_email     — format HTML and send via SMTP
"""

from __future__ import annotations

import logging
import os
import smtplib
from collections.abc import Callable
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

import pandas as pd

from rdd.schemas.portfolio_holdings import PortfolioHoldingsSchema

logger = logging.getLogger(__name__)

_TRADING_DAYS_PER_YEAR = 252


# ── helpers ───────────────────────────────────────────────────────────────────


def _load_ohlcv(ohlcv_existing: dict[str, Callable[[], pd.DataFrame]]) -> pd.DataFrame:
    """Concatenate all OHLCV ticker partitions into a single DataFrame."""
    frames = []
    for loader in ohlcv_existing.values():
        try:
            frames.append(loader())
        except Exception:
            pass
    if not frames:
        return pd.DataFrame(columns=["ticker", "date", "adj_close"])
    return pd.concat(frames, ignore_index=True)


def _load_holdings(
    holdings_existing: dict[str, Callable[[], pd.DataFrame]],
) -> pd.DataFrame:
    """Concatenate all holdings date-partitions and validate schema."""
    frames = []
    for loader in holdings_existing.values():
        try:
            frames.append(loader() if callable(loader) else loader)
        except Exception:
            pass
    if not frames:
        return pd.DataFrame(columns=["strategy", "date", "ticker", "weight"])
    df = pd.concat(frames, ignore_index=True)
    return PortfolioHoldingsSchema.validate(df)


# ── node 1: compute_strategy_returns ─────────────────────────────────────────


def compute_strategy_returns(
    holdings_existing: dict[str, Callable[[], pd.DataFrame]],
    ohlcv_existing: dict[str, Callable[[], pd.DataFrame]],
) -> pd.DataFrame:
    """Compute daily portfolio returns using buy-and-hold between rebalances.

    Weights from the most recent rebalance are forward-filled daily until the
    next rebalance event.  Daily portfolio return = Σ(weight_i × adj_close_return_i).

    Args:
        holdings_existing: Date-partitioned holdings DataFrames for one strategy.
        ohlcv_existing: Ticker-partitioned OHLCV DataFrames.

    Returns:
        DataFrame with columns ``date``, ``portfolio_return``.
        Empty if there are no holdings or no price data.
    """
    holdings = _load_holdings(holdings_existing)
    if holdings.empty:
        logger.warning("No holdings found — returning empty returns series.")
        return pd.DataFrame(columns=["date", "portfolio_return"])

    ohlcv = _load_ohlcv(ohlcv_existing)
    if ohlcv.empty:
        logger.warning("No OHLCV data found — returning empty returns series.")
        return pd.DataFrame(columns=["date", "portfolio_return"])

    # Daily adj_close returns per ticker
    ohlcv["date"] = pd.to_datetime(ohlcv["date"])
    prices = (
        ohlcv[["ticker", "date", "adj_close"]]
        .dropna(subset=["adj_close"])
        .sort_values(["ticker", "date"])
    )
    prices["daily_return"] = prices.groupby("ticker")["adj_close"].pct_change()
    returns_wide = prices.pivot(index="date", columns="ticker", values="daily_return")

    # Build daily weight matrix: forward-fill last rebalance weights
    holdings["date"] = pd.to_datetime(holdings["date"])
    rebalance_dates = sorted(holdings["date"].unique())
    all_dates = returns_wide.index.sort_values()

    weight_frames = []
    for i, rb_date in enumerate(rebalance_dates):
        next_rb = rebalance_dates[i + 1] if i + 1 < len(rebalance_dates) else all_dates.max() + pd.Timedelta(days=1)
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

    # Align columns and compute daily portfolio return
    common_tickers = weights_wide.columns.intersection(returns_wide.columns)
    weights_aligned = weights_wide[common_tickers]
    returns_aligned = returns_wide[common_tickers].reindex(weights_aligned.index)

    portfolio_returns = (weights_aligned * returns_aligned).sum(axis=1).dropna()
    result = portfolio_returns.reset_index()
    result.columns = ["date", "portfolio_return"]
    logger.info("Computed %d daily return observations.", len(result))
    return result


# ── node 2: compute_performance_metrics ──────────────────────────────────────


def compute_performance_metrics(daily_returns: pd.DataFrame) -> pd.DataFrame:
    """Compute summary performance metrics from a daily returns series.

    Args:
        daily_returns: Output of ``compute_strategy_returns`` with columns
            ``date`` and ``portfolio_return``.

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
        DataFrame with one row per strategy and a ``strategy`` index column.
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
    if val != val:
        return "n/a"
    return f"{val * 100:+.{digits}f}%"


def _fmt_float(val: float, digits: int = 2) -> str:
    if val != val:
        return "n/a"
    return f"{val:.{digits}f}"


def _build_html(report: pd.DataFrame) -> str:
    rows_html = ""
    for _, row in report.iterrows():
        rows_html += (
            f"<tr>"
            f"<td><b>{row['strategy']}</b></td>"
            f"<td>{_fmt_pct(row['cumulative_return'])}</td>"
            f"<td>{_fmt_pct(row['annualised_return'])}</td>"
            f"<td>{_fmt_pct(row['annualised_volatility'])}</td>"
            f"<td>{_fmt_float(row['sharpe_ratio'])}</td>"
            f"<td>{_fmt_pct(row['max_drawdown'])}</td>"
            f"<td>{int(row['observation_days'])}</td>"
            f"</tr>\n"
        )
    return f"""
<html><body>
<h2>Weekly Portfolio Performance Report</h2>
<table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse;font-family:monospace;">
  <thead style="background:#f0f0f0;">
    <tr>
      <th>Strategy</th>
      <th>Cumulative Return</th>
      <th>Ann. Return</th>
      <th>Ann. Volatility</th>
      <th>Sharpe</th>
      <th>Max Drawdown</th>
      <th>Days</th>
    </tr>
  </thead>
  <tbody>
{rows_html}  </tbody>
</table>
</body></html>
"""


def send_performance_email(
    report: pd.DataFrame,
    params: dict[str, Any],
) -> None:
    """Format the performance report as HTML and send via SMTP.

    SMTP credentials are read from environment variables if not present in
    params, matching the project's existing ``RDD_*`` env-var convention.

    Args:
        report: Output of ``compile_report``.
        params: ``portfolio_performance`` parameter block.
    """
    to_addr = params.get("email_to") or os.environ.get("RDD_EMAIL_TO", "")
    smtp_host = params.get("smtp_host") or os.environ.get("RDD_SMTP_HOST", "")
    smtp_port = int(params.get("smtp_port") or os.environ.get("RDD_SMTP_PORT", 465))
    smtp_user = params.get("smtp_user") or os.environ.get("RDD_SMTP_USER", "")
    smtp_pass = params.get("smtp_pass") or os.environ.get("RDD_SMTP_PASS", "")

    if not all([to_addr, smtp_host, smtp_user, smtp_pass]):
        logger.warning("Email config incomplete — skipping send.")
        return

    html = _build_html(report)
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
