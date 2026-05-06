"""Nodes for the portfolio_performance pipeline.

Five-stage flow:
  1. compute_strategy_returns  — join holdings x OHLCV -> daily portfolio returns
  2. compute_benchmark_returns — load benchmark ticker (SPY) -> daily benchmark returns
  3. compute_performance_metrics — Sharpe, max drawdown, win rate, cumulative return
  4. compile_report             — merge all strategy metrics
  5. send_performance_email     — HTML dashboard (charts, KPI table, holdings) via SMTP
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
_HOLDINGS_FULL_THRESHOLD = 15  # strategies with ≤ this many tickers get a full table

# Sector colour palette (cycles if there are more than 10 sectors)
_SECTOR_COLOURS = [
    "#4c9be8",
    "#e87c4c",
    "#4ce8a0",
    "#e8d44c",
    "#b04ce8",
    "#e84c7c",
    "#4ce8d4",
    "#8ce84c",
    "#e84ca0",
    "#4c6ce8",
]


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

        cost_by_date[rb_date] = total_cost / portfolio_size
        logger.debug(
            "Rebalance %s: total_commission=%.2f  cost_fraction=%.6f",
            rb_date.date(),
            total_cost,
            total_cost / portfolio_size,
        )
        prev_weights = rb_w

    return cost_by_date


def compute_strategy_returns(
    holdings_existing: dict[str, Callable[[], pd.DataFrame]],
    ohlcv_existing: dict[str, Callable[[], pd.DataFrame]],
    params: dict | None = None,
) -> pd.DataFrame:
    """Compute daily portfolio returns using buy-and-hold between rebalances.

    Transaction costs are deducted on each rebalance date when broker params
    are present (``commission_per_share_usd``, ``min_commission_per_order_usd``,
    ``assumed_portfolio_size_usd``).

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

    holdings["date"] = pd.to_datetime(holdings["date"])
    cutoff = pd.Timestamp.now().normalize() - pd.DateOffset(months=lookback_months)
    recent_holdings = holdings[holdings["date"] >= cutoff]
    if recent_holdings.empty:
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


# ── node 2: compute_benchmark_returns ────────────────────────────────────────


def compute_benchmark_returns(
    ohlcv_existing: dict[str, Callable[[], pd.DataFrame]],
    params: dict | None = None,
) -> pd.DataFrame:
    """Load the benchmark ticker and return its daily returns over the lookback window.

    Returns:
        DataFrame with columns ``date``, ``benchmark_return``.
    """
    p = params or {}
    ticker = p.get("benchmark_ticker", "SPY").upper()
    partition_key = ticker.lower()
    lookback_months: int = int(p.get("lookback_months", 3))

    loader = ohlcv_existing.get(partition_key)
    if loader is None:
        logger.warning("Benchmark ticker %s not found in OHLCV data.", ticker)
        return pd.DataFrame(columns=["date", "benchmark_return"])

    try:
        df = loader()
    except Exception:
        logger.warning("Could not load OHLCV for benchmark %s.", ticker, exc_info=True)
        return pd.DataFrame(columns=["date", "benchmark_return"])

    df["date"] = pd.to_datetime(df["date"])
    cutoff = pd.Timestamp.now().normalize() - pd.DateOffset(months=lookback_months)
    df = df[df["date"] >= cutoff].sort_values("date").copy()
    df["benchmark_return"] = df["adj_close"].pct_change()
    result = df[["date", "benchmark_return"]].dropna().reset_index(drop=True)
    logger.info("Benchmark %s: %d daily return observations.", ticker, len(result))
    return result


# ── node 3: compute_performance_metrics ──────────────────────────────────────


def compute_performance_metrics(daily_returns: pd.DataFrame) -> pd.DataFrame:
    """Compute summary performance metrics from a daily returns series.

    Returns:
        Single-row DataFrame with columns: ``cumulative_return``,
        ``annualised_return``, ``annualised_volatility``, ``sharpe_ratio``,
        ``max_drawdown``, ``win_rate``, ``observation_days``.
    """
    empty = pd.DataFrame(
        [
            {
                "cumulative_return": float("nan"),
                "annualised_return": float("nan"),
                "annualised_volatility": float("nan"),
                "sharpe_ratio": float("nan"),
                "max_drawdown": float("nan"),
                "win_rate": float("nan"),
                "observation_days": 0,
            }
        ]
    )
    ret_col = (
        "portfolio_return"
        if "portfolio_return"
        in (daily_returns.columns if not daily_returns.empty else [])
        else "benchmark_return"
    )
    if daily_returns.empty or ret_col not in daily_returns.columns:
        return empty

    r = daily_returns[ret_col].dropna()
    if r.empty:
        return empty

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

    win_rate = float((r > 0).sum()) / n

    return pd.DataFrame(
        [
            {
                "cumulative_return": round(cumulative, 6),
                "annualised_return": round(ann_return, 6),
                "annualised_volatility": round(ann_vol, 6),
                "sharpe_ratio": round(sharpe, 4),
                "max_drawdown": round(max_dd, 6),
                "win_rate": round(win_rate, 4),
                "observation_days": n,
            }
        ]
    )


# ── node 4: compile_report ────────────────────────────────────────────────────


def compile_report(**strategy_metrics: pd.DataFrame) -> pd.DataFrame:
    """Merge per-strategy metrics into a single comparison DataFrame."""
    rows = []
    for strategy, metrics_df in strategy_metrics.items():
        row = metrics_df.iloc[0].to_dict()
        row["strategy"] = strategy
        rows.append(row)
    report = pd.DataFrame(rows).set_index("strategy").reset_index()
    logger.info("Compiled performance report for %d strategies.", len(rows))
    return report


# ── node 5: send_performance_email ───────────────────────────────────────────


def _fmt_pct(val: float, digits: int = 2) -> str:
    if math.isnan(val):
        return "n/a"
    color = "green" if val >= 0 else "red"
    return f'<span style="color:{color}">{val * 100:+.{digits}f}%</span>'


def _fmt_float(val: float, digits: int = 2) -> str:
    if math.isnan(val):
        return "n/a"
    return f"{val:.{digits}f}"


def _fmt_strategy_name(name: str) -> str:
    return name.replace("_", " ").title()


def _returns_series(dr: pd.DataFrame) -> pd.Series | None:
    col = "portfolio_return" if "portfolio_return" in dr.columns else "benchmark_return"
    if dr.empty or col not in dr.columns:
        return None
    return dr.set_index("date")[col].dropna().sort_index()


def _date_axis(ax: Any, date_min: pd.Timestamp, date_max: pd.Timestamp) -> None:
    span_days = (date_max - date_min).days
    if span_days > 365:
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    elif span_days > 90:
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
    else:
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b '%y"))


def _fig_to_b64(fig: Any) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120)
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode()


def _chart_png_b64(
    daily_returns_by_strategy: dict[str, pd.DataFrame],
    benchmark_returns: pd.DataFrame | None = None,
    benchmark_label: str = "SPY",
) -> str:
    """Cumulative return chart with an optional dashed benchmark line."""
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.axhline(0, color="#cccccc", linewidth=0.8)

    date_min, date_max = None, None

    if benchmark_returns is not None and not benchmark_returns.empty:
        r = _returns_series(benchmark_returns)
        if r is not None:
            cum = (1 + r).cumprod() - 1
            ax.plot(
                cum.index,
                cum * 100,
                linewidth=1.5,
                linestyle="--",
                color="#888888",
                label=f"{benchmark_label} (benchmark)",
            )
            date_min = r.index.min()
            date_max = r.index.max()

    for strategy, dr in daily_returns_by_strategy.items():
        r = _returns_series(dr)
        if r is None:
            continue
        cum = (1 + r).cumprod() - 1
        ax.plot(cum.index, cum * 100, linewidth=1.8, label=_fmt_strategy_name(strategy))
        date_min = r.index.min() if date_min is None else min(date_min, r.index.min())
        date_max = r.index.max() if date_max is None else max(date_max, r.index.max())

    if date_min is not None and date_max is not None:
        _date_axis(ax, date_min, date_max)
        fig.autofmt_xdate(rotation=30, ha="right")

    ax.set_ylabel("Cumulative Return (%)")
    ax.set_title("Cumulative Return — all strategies vs benchmark")
    ax.legend(frameon=False, fontsize=8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout()
    return _fig_to_b64(fig)


def _drawdown_chart_png_b64(
    daily_returns_by_strategy: dict[str, pd.DataFrame],
    benchmark_returns: pd.DataFrame | None = None,
    benchmark_label: str = "SPY",
) -> str:
    """Rolling drawdown-from-peak chart for all strategies + benchmark."""
    fig, ax = plt.subplots(figsize=(12, 3))
    ax.axhline(0, color="#cccccc", linewidth=0.8)

    date_min, date_max = None, None

    if benchmark_returns is not None and not benchmark_returns.empty:
        r = _returns_series(benchmark_returns)
        if r is not None:
            cum = (1 + r).cumprod()
            dd = (cum / cum.cummax() - 1) * 100
            ax.fill_between(dd.index, dd, 0, alpha=0.12, color="#888888")
            ax.plot(
                dd.index,
                dd,
                linewidth=1.2,
                linestyle="--",
                color="#888888",
                label=f"{benchmark_label} (benchmark)",
            )
            date_min = r.index.min()
            date_max = r.index.max()

    for strategy, dr in daily_returns_by_strategy.items():
        r = _returns_series(dr)
        if r is None:
            continue
        cum = (1 + r).cumprod()
        dd = (cum / cum.cummax() - 1) * 100
        ax.plot(dd.index, dd, linewidth=1.5, label=_fmt_strategy_name(strategy))
        date_min = r.index.min() if date_min is None else min(date_min, r.index.min())
        date_max = r.index.max() if date_max is None else max(date_max, r.index.max())

    if date_min is not None and date_max is not None:
        _date_axis(ax, date_min, date_max)
        fig.autofmt_xdate(rotation=30, ha="right")

    ax.set_ylabel("Drawdown (%)")
    ax.set_title("Drawdown from Peak")
    ax.legend(frameon=False, fontsize=8)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    fig.tight_layout()
    return _fig_to_b64(fig)


def _sector_bar_html(sector_weights: dict[str, float]) -> str:
    """Render a CSS bar chart of sector weights, email-client compatible."""
    if not sector_weights:
        return ""
    sorted_sectors = sorted(sector_weights.items(), key=lambda x: x[1], reverse=True)
    max_w = max(w for _, w in sorted_sectors)
    max_bar_px = 180
    rows = ""
    for i, (sector, w) in enumerate(sorted_sectors):
        colour = _SECTOR_COLOURS[i % len(_SECTOR_COLOURS)]
        bar_px = int(max_bar_px * w / max_w) if max_w > 0 else 0
        rows += (
            f"<tr>"
            f"<td style='padding:2px 6px 2px 0;width:160px;font-size:11px'>{sector}</td>"
            f"<td style='padding:2px 0'>"
            f"<div style='background:{colour};height:12px;width:{bar_px}px'></div></td>"
            f"<td style='padding:2px 0 2px 6px;font-size:11px;text-align:right'>"
            f"{w * 100:.1f}%</td>"
            f"</tr>"
        )
    return (
        "<table style='border-collapse:collapse;font-family:monospace'>"
        f"<tbody>{rows}</tbody></table>"
    )


def _holdings_section_html(
    strategy: str,
    df: pd.DataFrame,
    company_info: dict[str, str],
) -> str:
    """Render holdings for one strategy — full table or sector summary."""
    latest_date = df["date"].max()
    latest = df[df["date"] == latest_date].sort_values("weight", ascending=False)
    n = len(latest)
    header = (
        f"<h3 style='margin-top:24px'>{_fmt_strategy_name(strategy)} — "
        f"{n} holdings as of {latest_date.date()}</h3>"
    )

    if n <= _HOLDINGS_FULL_THRESHOLD:
        rows = "".join(
            f"<tr><td>{r['ticker']}</td>"
            f"<td style='text-align:right'>{r['weight'] * 100:.1f}%</td></tr>"
            for _, r in latest.iterrows()
        )
        table = (
            "<table border='1' cellpadding='5' cellspacing='0' "
            "style='border-collapse:collapse;font-family:monospace;min-width:260px'>"
            "<thead style='background:#f0f0f0'><tr><th>Ticker</th><th>Weight</th></tr></thead>"
            f"<tbody>{rows}</tbody></table>"
        )
        return header + table

    # Diversified strategy: top-10 summary + sector breakdown
    top10 = latest.head(10)
    top10_html = "".join(
        f"<tr><td>{r['ticker']}</td>"
        f"<td style='text-align:right'>{r['weight'] * 100:.1f}%</td></tr>"
        for _, r in top10.iterrows()
    )
    tail_count = n - 10
    avg_tail_w = latest.iloc[10:]["weight"].mean() if tail_count > 0 else 0.0
    tail_row = (
        f"<tr style='color:#888'>"
        f"<td>… and {tail_count} others</td>"
        f"<td style='text-align:right'>avg {avg_tail_w * 100:.1f}%</td></tr>"
        if tail_count > 0
        else ""
    )
    top_table = (
        "<table border='1' cellpadding='5' cellspacing='0' "
        "style='border-collapse:collapse;font-family:monospace;min-width:260px'>"
        "<thead style='background:#f0f0f0'><tr><th>Ticker</th><th>Weight</th></tr></thead>"
        f"<tbody>{top10_html}{tail_row}</tbody></table>"
    )

    # Sector breakdown
    sector_weights: dict[str, float] = {}
    for _, r in latest.iterrows():
        sector = company_info.get(r["ticker"].upper(), "Unknown")
        sector_weights[sector] = sector_weights.get(sector, 0.0) + float(r["weight"])
    sector_html = _sector_bar_html(sector_weights)

    turnover_row = (
        f"<p style='font-size:11px;color:#666;margin:4px 0'>"
        f"Avg weight: {latest['weight'].mean() * 100:.2f}% · "
        f"Max weight: {latest['weight'].max() * 100:.2f}%</p>"
    )

    return (
        header
        + turnover_row
        + "<table style='border-collapse:collapse;width:100%'><tr>"
        + f"<td style='vertical-align:top;padding-right:32px'>{top_table}</td>"
        + "<td style='vertical-align:top'>"
        + "<p style='font-size:11px;font-weight:bold;margin:0 0 6px'>Sector allocation</p>"
        + sector_html
        + "</td></tr></table>"
    )


def _holdings_table_html(
    holdings_by_strategy: dict[str, pd.DataFrame],
    company_info: dict[str, str] | None = None,
) -> str:
    """Build the holdings breakdown section for all strategies."""
    if not holdings_by_strategy:
        return ""
    ci = company_info or {}
    sections = [
        _holdings_section_html(strategy, df, ci)
        for strategy, df in holdings_by_strategy.items()
        if not df.empty
    ]
    return "\n".join(sections)


def _kpi_table_html(
    report: pd.DataFrame,
    benchmark_cumulative: float | None = None,
    benchmark_label: str = "SPY",
) -> str:
    """KPI table with optional benchmark row and vs-benchmark column."""
    has_benchmark = benchmark_cumulative is not None and not math.isnan(
        benchmark_cumulative
    )

    def _row(
        name: str,
        cum: float,
        ann: float,
        vol: float,
        sharpe: float,
        dd: float,
        win: float,
        days: int,
        is_benchmark: bool = False,
    ) -> str:
        style = "background:#f7f7f7;font-style:italic" if is_benchmark else ""
        vs_bm = (
            f"<td style='text-align:right'>{_fmt_pct(cum - benchmark_cumulative)}</td>"
            if has_benchmark and not is_benchmark
            else (
                "<td style='text-align:right;color:#888'>—</td>"
                if has_benchmark
                else ""
            )
        )
        win_str = f"{win * 100:.0f}%" if not math.isnan(win) else "n/a"
        return (
            f"<tr style='{style}'>"
            f"<td><b>{name}</b></td>"
            f"<td style='text-align:right'>{_fmt_pct(cum)}</td>"
            f"<td style='text-align:right'>{_fmt_pct(ann)}</td>"
            f"<td style='text-align:right'>{_fmt_pct(vol)}</td>"
            f"<td style='text-align:right'>{_fmt_float(sharpe)}</td>"
            f"<td style='text-align:right'>{_fmt_pct(dd)}</td>"
            f"<td style='text-align:right'>{win_str}</td>"
            f"{vs_bm}"
            f"<td style='text-align:right'>{int(days)}</td>"
            f"</tr>\n"
        )

    vs_header = f"<th>vs {benchmark_label}</th>" if has_benchmark else ""
    header = (
        "<thead style='background:#f0f0f0'>"
        "<tr><th>Strategy</th><th>Cum. Return</th><th>Ann. Return</th>"
        "<th>Ann. Vol</th><th>Sharpe</th><th>Max Drawdown</th><th>Win Rate</th>"
        f"{vs_header}<th>Days</th></tr></thead>"
    )

    rows_html = ""
    if has_benchmark:
        rows_html += _row(
            f"{benchmark_label} (benchmark)",
            benchmark_cumulative,
            float("nan"),
            float("nan"),
            float("nan"),
            float("nan"),
            float("nan"),
            0,
            is_benchmark=True,
        )

    for _, row in report.iterrows():
        rows_html += _row(
            _fmt_strategy_name(row["strategy"]),
            row["cumulative_return"],
            row["annualised_return"],
            row["annualised_volatility"],
            row["sharpe_ratio"],
            row["max_drawdown"],
            row.get("win_rate", float("nan")),
            int(row["observation_days"]),
        )

    return (
        "<table border='1' cellpadding='6' cellspacing='0' "
        "style='border-collapse:collapse;font-family:monospace;'>"
        f"{header}<tbody>{rows_html}</tbody></table>"
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


def _img_tag(b64: str, alt: str) -> str:
    return (
        f'<img src="data:image/png;base64,{b64}" '
        f'style="max-width:100%;margin:16px 0" alt="{alt}"/>'
        if b64
        else ""
    )


def _build_html(
    report: pd.DataFrame,
    chart_b64: str,
    drawdown_b64: str,
    holdings_html: str,
    strategy_descriptions: dict[str, str] | None = None,
    benchmark_cumulative: float | None = None,
    benchmark_label: str = "SPY",
) -> str:
    desc_html = _descriptions_html(strategy_descriptions or {})
    return f"""
<html>
<body style="font-family:Arial,sans-serif;max-width:940px;margin:auto;padding:24px">
  <h2 style="border-bottom:2px solid #333;padding-bottom:8px">
    RDD Weekly Portfolio Performance
  </h2>

  <h3>Cumulative Return</h3>
  {_img_tag(chart_b64, "Cumulative return chart")}

  <h3>Drawdown from Peak</h3>
  {_img_tag(drawdown_b64, "Drawdown chart")}

  <h3>Key Performance Indicators</h3>
  {_kpi_table_html(report, benchmark_cumulative, benchmark_label)}

  <h3>Current Holdings</h3>
  {holdings_html}

  {desc_html}

  <p style="color:#888;font-size:11px;margin-top:32px">
    Generated by RDD · {pd.Timestamp.now().strftime("%Y-%m-%d %H:%M UTC")}
  </p>
</body>
</html>
"""


def _parse_strategy_data(
    strategy_data: dict[str, Any],
) -> tuple[
    dict[str, pd.DataFrame],
    dict[str, pd.DataFrame],
    pd.DataFrame | None,
    dict[str, str],
    float | None,
]:
    """Unpack the ``**strategy_data`` kwargs into typed components.

    Returns:
        (daily_returns_by_strategy, holdings_by_strategy, benchmark_returns,
         ticker_sector, benchmark_cumulative)
    """
    daily_returns_by_strategy: dict[str, pd.DataFrame] = {
        k.replace("_returns", ""): v
        for k, v in strategy_data.items()
        if k.endswith("_returns")
        and k != "benchmark_returns"
        and isinstance(v, pd.DataFrame)
    }

    holdings_by_strategy: dict[str, pd.DataFrame] = {}
    for k, v in strategy_data.items():
        if not k.endswith("_holdings"):
            continue
        raw = v
        if isinstance(raw, dict):
            frames = []
            for loader in raw.values():
                with contextlib.suppress(Exception):
                    frames.append(loader() if callable(loader) else loader)
            raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if isinstance(raw, pd.DataFrame) and not raw.empty:
            raw = raw.copy()
            raw["date"] = pd.to_datetime(raw["date"])
            holdings_by_strategy[k.replace("_holdings", "")] = raw

    benchmark_returns: pd.DataFrame | None = strategy_data.get("benchmark_returns")

    ticker_sector: dict[str, str] = {}
    for loader in (strategy_data.get("company_info") or {}).values():
        with contextlib.suppress(Exception):
            df = loader() if callable(loader) else loader
            for _, row in df.iterrows():
                if pd.notna(row.get("sector")):
                    ticker_sector[str(row["ticker"]).upper()] = str(row["sector"])

    benchmark_cumulative: float | None = None
    if benchmark_returns is not None and not benchmark_returns.empty:
        r = _returns_series(benchmark_returns)
        if r is not None and not r.empty:
            benchmark_cumulative = float((1 + r).prod() - 1)

    return (
        daily_returns_by_strategy,
        holdings_by_strategy,
        benchmark_returns,
        ticker_sector,
        benchmark_cumulative,
    )


def send_performance_email(
    report: pd.DataFrame,
    params: dict[str, Any],
    **strategy_data: Any,
) -> None:
    """Format and send the weekly performance email with the full HTML dashboard.

    Keyword arguments injected by the pipeline:
      - ``{strategy}_returns``: daily returns DataFrame per strategy
      - ``{strategy}_holdings``: holdings DataFrame per strategy
      - ``benchmark_returns``: daily benchmark returns DataFrame
      - ``company_info``: dict[str, Callable] of company_info partition loaders
    """
    to_addr = params.get("email_to") or os.environ.get("RDD_EMAIL_TO", "")
    smtp_host = params.get("smtp_host") or os.environ.get("RDD_SMTP_HOST", "")
    smtp_port = int(params.get("smtp_port") or os.environ.get("RDD_SMTP_PORT", "465"))
    smtp_user = params.get("smtp_user") or os.environ.get("RDD_SMTP_USER", "")
    smtp_pass = params.get("smtp_pass") or os.environ.get("RDD_SMTP_PASS", "")

    if not all([to_addr, smtp_host, smtp_user, smtp_pass]):
        logger.warning("Email config incomplete — skipping send.")
        return

    benchmark_label: str = params.get("benchmark_ticker", "SPY")
    (
        daily_returns_by_strategy,
        holdings_by_strategy,
        benchmark_returns,
        ticker_sector,
        benchmark_cumulative,
    ) = _parse_strategy_data(strategy_data)

    try:
        chart_b64 = _chart_png_b64(
            daily_returns_by_strategy, benchmark_returns, benchmark_label
        )
    except Exception:
        logger.warning("Cumulative return chart failed.", exc_info=True)
        chart_b64 = ""

    try:
        drawdown_b64 = _drawdown_chart_png_b64(
            daily_returns_by_strategy, benchmark_returns, benchmark_label
        )
    except Exception:
        logger.warning("Drawdown chart failed.", exc_info=True)
        drawdown_b64 = ""

    holdings_html = _holdings_table_html(holdings_by_strategy, ticker_sector)
    strategy_descriptions: dict[str, str] = params.get("strategy_descriptions", {})

    html = _build_html(
        report,
        chart_b64,
        drawdown_b64,
        holdings_html,
        strategy_descriptions,
        benchmark_cumulative,
        benchmark_label,
    )

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
