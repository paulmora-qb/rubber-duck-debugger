#!/usr/bin/env python3
"""Send daily ingest report email via Gmail SMTP.

Called by run_daily_ingest.sh with triplet args:
    send_report.py [--ohlcv-dir DIR] <pipeline> <status> <log_path> [...]

Status values: ok | fail | skip
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import smtplib
import ssl
import sys
from datetime import date, datetime, timedelta
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
import requests

mpl.use("Agg")

_logger = logging.getLogger(__name__)
_LOG_TAIL = 50
_LOGS_DIR = Path(__file__).parent.parent / "logs"
_MANIFEST = _LOGS_DIR / "run_manifest.jsonl"
_MEMBERSHIP_CACHE = _LOGS_DIR / "ticker_membership.json"
_MEMBERSHIP_TTL_DAYS = 7
_HISTORY_DAYS = 30


# ---------------------------------------------------------------------------
# Ticker index membership (S&P 500 / NASDAQ 100), cached locally
# ---------------------------------------------------------------------------


def _fetch_membership() -> dict[str, list[str]]:
    """Fetch S&P 500 and NASDAQ 100 tickers from the same sources as the pipeline."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        )
    }

    def _get(url: str, **kwargs: object) -> list[pd.DataFrame]:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        return pd.read_html(io.StringIO(resp.text), **kwargs)

    def _normalise(t: str) -> str:
        return t.replace(".", "-")

    sp500: list[str] = (
        _get("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", attrs={"id": "constituents"})[0]["Symbol"]
        .map(_normalise)
        .tolist()
    )

    ndx100: list[str] = []
    for tbl in _get("https://en.wikipedia.org/wiki/Nasdaq-100"):
        if "Ticker" in tbl.columns:
            ndx100 = tbl["Ticker"].map(_normalise).tolist()
            break

    return {"sp500": sorted(sp500), "nasdaq100": sorted(ndx100)}


def _load_membership() -> dict[str, set[str]]:
    """Return {index_name: set(tickers)}, refreshing the cache if stale."""
    if _MEMBERSHIP_CACHE.exists():
        age_days = (datetime.now() - datetime.fromtimestamp(_MEMBERSHIP_CACHE.stat().st_mtime)).days
        if age_days < _MEMBERSHIP_TTL_DAYS:
            data = json.loads(_MEMBERSHIP_CACHE.read_text())
            return {k: set(v) for k, v in data.items()}

    _LOGS_DIR.mkdir(parents=True, exist_ok=True)
    data = _fetch_membership()
    _MEMBERSHIP_CACHE.write_text(json.dumps(data))
    return {k: set(v) for k, v in data.items()}


# ---------------------------------------------------------------------------
# Per-run stock counts
# ---------------------------------------------------------------------------


def _count_by_index(
    ohlcv_dir: str | None, membership: dict[str, set[str]]
) -> dict[str, int]:
    """Count parquets modified today, broken down by index membership."""
    counts: dict[str, int] = {"sp500": 0, "nasdaq100": 0, "total": 0}
    if not ohlcv_dir:
        return counts
    p = Path(ohlcv_dir)
    if not p.exists():
        return counts
    today = date.today()
    for f in p.glob("*.parquet"):
        if date.fromtimestamp(f.stat().st_mtime) != today:
            continue
        ticker = f.stem.upper()
        counts["total"] += 1
        if ticker in membership.get("sp500", set()):
            counts["sp500"] += 1
        if ticker in membership.get("nasdaq100", set()):
            counts["nasdaq100"] += 1
    return counts


# ---------------------------------------------------------------------------
# Historical backfill
# ---------------------------------------------------------------------------


def _backfill_manifest(
    ohlcv_dir: str | None,
    membership: dict[str, set[str]],
    existing: dict[date, dict],
    days: int = _HISTORY_DAYS,
) -> None:
    """Populate manifest with per-date counts inferred from parquet date columns.

    Reads only the date column from each parquet to build a {date: set(tickers)}
    mapping, then writes manifest entries for any trading days not already present.
    """
    if not ohlcv_dir:
        return
    p = Path(ohlcv_dir)
    if not p.exists():
        return

    cutoff = date.today() - timedelta(days=days)
    date_tickers: dict[date, set[str]] = {}

    for f in p.glob("*.parquet"):
        ticker = f.stem.upper()
        try:
            df = pd.read_parquet(f, columns=["date"])
            for d in df["date"].dt.date.unique():
                if d >= cutoff:
                    date_tickers.setdefault(d, set()).add(ticker)
        except Exception:
            _logger.warning("Failed to read parquet %s", f, exc_info=True)
            continue

    for d, tickers in sorted(date_tickers.items()):
        if d in existing:
            continue
        counts = {
            "sp500": len(tickers & membership.get("sp500", set())),
            "nasdaq100": len(tickers & membership.get("nasdaq100", set())),
            "total": len(tickers),
        }
        _append_manifest(d, counts, "ok")


# ---------------------------------------------------------------------------
# Run manifest
# ---------------------------------------------------------------------------


def _append_manifest(run_date: date, counts: dict[str, int], status: str) -> None:
    """Append a run record to the JSONL manifest."""
    _MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    with _MANIFEST.open("a") as fh:
        fh.write(
            json.dumps(
                {
                    "date": run_date.isoformat(),
                    "n_sp500": counts["sp500"],
                    "n_nasdaq100": counts["nasdaq100"],
                    "n_total": counts["total"],
                    "status": status,
                }
            )
            + "\n"
        )


def _load_manifest(days: int = _HISTORY_DAYS) -> dict[date, dict]:
    """Return {date: record} for the last N days."""
    if not _MANIFEST.exists():
        return {}
    cutoff = date.today() - timedelta(days=days)
    records: dict[date, dict] = {}
    for line in _MANIFEST.read_text().splitlines():
        if not line.strip():
            continue
        try:
            r = json.loads(line)
            d = date.fromisoformat(r["date"])
            if d >= cutoff:
                records[d] = r
        except (json.JSONDecodeError, KeyError, ValueError):
            continue
    return records


# ---------------------------------------------------------------------------
# Chart
# ---------------------------------------------------------------------------


def _make_chart(history: dict[date, dict], membership: dict[str, set[str]]) -> bytes:
    """Render a dual-line chart (S&P 500 vs NASDAQ 100 stocks fetched) as PNG bytes."""
    today = date.today()
    dates = sorted(d for d in history if d <= today)

    sp500_counts = [history[d].get("n_sp500", 0) for d in dates]
    ndx100_counts = [history[d].get("n_nasdaq100", 0) for d in dates]

    sp500_expected = len(membership.get("sp500", set()))
    ndx100_expected = len(membership.get("nasdaq100", set()))

    fig, ax = plt.subplots(figsize=(10, 4))

    if dates:
        ax.plot(dates, sp500_counts, color="#1f77b4", marker="o", markersize=4, label="S&P 500")
        ax.plot(dates, ndx100_counts, color="#ff7f0e", marker="s", markersize=4, label="NASDAQ 100")

    if sp500_expected:
        ax.axhline(sp500_expected, color="#1f77b4", linestyle="--", linewidth=0.8, alpha=0.6,
                   label=f"S&P 500 universe ({sp500_expected})")
    if ndx100_expected:
        ax.axhline(ndx100_expected, color="#ff7f0e", linestyle="--", linewidth=0.8, alpha=0.6,
                   label=f"NASDAQ 100 universe ({ndx100_expected})")

    ax.set_title(f"Stocks fetched per run — last {_HISTORY_DAYS} days", fontsize=10)
    ax.set_ylabel("Stocks fetched")
    ax.set_xlabel("Run date")
    ax.legend(fontsize=8, loc="lower right")
    ax.grid(axis="y", linestyle="--", alpha=0.4)
    ax.tick_params(axis="x", labelrotation=30, labelsize=7)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=120)
    plt.close(fig)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------


def _tail(path: str, n: int = _LOG_TAIL) -> str:
    """Return the last n lines of a log file."""
    p = Path(path)
    if not p.exists():
        return "(log file not found)"
    lines = p.read_text().splitlines()
    return "\n".join(lines[-n:]) if lines else "(empty log)"


def _subject(results: dict[str, str]) -> str:
    """Build email subject line."""
    failed = [p for p, s in results.items() if s == "fail"]
    if not failed:
        return "[RDD] Daily ingest OK"
    return f"[RDD] Daily ingest FAILED: {', '.join(failed)}"


def _html_body(
    results: dict[str, str],
    log_paths: dict[str, str],
    counts: dict[str, int],
) -> str:
    """Build the HTML email body."""
    icons = {"ok": "✓", "skip": "—", "fail": "✗"}
    rows = "".join(
        f"<tr><td>{icons.get(s, '?')}</td><td><b>{p}</b></td><td>{s.upper()}</td></tr>"
        for p, s in results.items()
    )
    n_total = counts.get("total", 0)
    n_sp500 = counts.get("sp500", 0)
    n_ndx100 = counts.get("nasdaq100", 0)
    coverage = (
        f"{n_total} stocks &nbsp;|&nbsp; S&amp;P 500: {n_sp500} &nbsp;|&nbsp; NASDAQ 100: {n_ndx100}"
        if n_total
        else "—"
    )
    log_sections = "".join(
        f"<h3>{p}</h3><pre style='font-size:11px'>{_tail(log_paths.get(p, ''))}</pre>"
        for p, s in results.items()
        if s == "fail"
    )
    return f"""
<html><body style="font-family:sans-serif;max-width:900px">
<h2>Daily Ingest Report</h2>
<table border="1" cellpadding="6" cellspacing="0" style="border-collapse:collapse">
  <tr style="background:#f0f0f0"><th>Status</th><th>Pipeline</th><th>Result</th></tr>
  {rows}
</table>
<p><b>Fetched today:</b> {coverage}</p>
<p><img src="cid:chart" alt="Stocks fetched per run" style="max-width:820px"/></p>
{log_sections}
</body></html>
"""


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Send daily ingest report email."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--ohlcv-dir", default=None, help="Path to data/raw/ohlcv directory")
    parser.add_argument("triplets", nargs="*")
    args = parser.parse_args()

    if not args.triplets or len(args.triplets) % 3 != 0:
        sys.exit("Usage: send_report.py [--ohlcv-dir DIR] <pipeline> <status> <log_path> ...")

    results: dict[str, str] = {}
    log_paths: dict[str, str] = {}
    for i in range(0, len(args.triplets), 3):
        name, status, log_path = args.triplets[i], args.triplets[i + 1], args.triplets[i + 2]
        results[name] = status
        log_paths[name] = log_path

    membership = _load_membership()
    counts = _count_by_index(args.ohlcv_dir, membership)
    overall = "fail" if any(s == "fail" for s in results.values()) else "ok"
    _append_manifest(date.today(), counts, overall)

    history = _load_manifest()
    _backfill_manifest(args.ohlcv_dir, membership, history)
    history = _load_manifest()  # reload after backfill
    chart_png = _make_chart(history, membership)

    to_addr = os.environ["RDD_EMAIL_TO"]
    smtp_host = os.environ.get("RDD_SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("RDD_SMTP_PORT", "465"))
    smtp_user = os.environ["RDD_SMTP_USER"]
    smtp_pass = os.environ["RDD_SMTP_PASS"]

    msg = MIMEMultipart("related")
    msg["Subject"] = _subject(results)
    msg["From"] = smtp_user
    msg["To"] = to_addr

    alt = MIMEMultipart("alternative")
    alt.attach(MIMEText(_html_body(results, log_paths, counts), "html"))
    msg.attach(alt)

    img = MIMEImage(chart_png)
    img.add_header("Content-ID", "<chart>")
    img.add_header("Content-Disposition", "inline", filename="chart.png")
    msg.attach(img)

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_host, smtp_port, context=ctx, timeout=30) as conn:
        conn.login(smtp_user, smtp_pass)
        conn.send_message(msg)

    sys.stdout.write(f"Report sent to {to_addr}\n")


if __name__ == "__main__":
    main()
