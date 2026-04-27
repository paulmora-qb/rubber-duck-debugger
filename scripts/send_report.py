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
import os
import smtplib
import ssl
import sys
from datetime import date, timedelta
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import matplotlib
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np

matplotlib.use("Agg")

_LOG_TAIL = 50
_MANIFEST = Path(__file__).parent.parent / "logs" / "run_manifest.jsonl"
_HISTORY_DAYS = 30


def _tail(path: str, n: int = _LOG_TAIL) -> str:
    """Return the last n lines of a log file."""
    p = Path(path)
    if not p.exists():
        return "(log file not found)"
    lines = p.read_text().splitlines()
    return "\n".join(lines[-n:]) if lines else "(empty log)"


def _count_tickers_fetched(ohlcv_dir: str | None) -> tuple[int, int]:
    """Count parquets modified today vs total — proxy for today's fetch coverage."""
    if not ohlcv_dir:
        return 0, 0
    p = Path(ohlcv_dir)
    if not p.exists():
        return 0, 0
    files = list(p.glob("*.parquet"))
    today = date.today()
    n_fetched = sum(1 for f in files if date.fromtimestamp(f.stat().st_mtime) == today)
    return n_fetched, len(files)


def _append_manifest(run_date: date, n_fetched: int, n_total: int, status: str) -> None:
    """Append a run record to the JSONL manifest."""
    _MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    with _MANIFEST.open("a") as fh:
        fh.write(
            json.dumps(
                {
                    "date": run_date.isoformat(),
                    "n_fetched": n_fetched,
                    "n_total": n_total,
                    "status": status,
                }
            )
            + "\n"
        )


def _load_manifest(days: int = _HISTORY_DAYS) -> dict[date, dict]:
    """Return {date: record} for the last N days from the JSONL manifest."""
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


def _make_heatmap(history: dict[date, dict]) -> bytes:
    """Render a GitHub-style calendar heatmap and return PNG bytes."""
    today = date.today()
    start = today - timedelta(days=_HISTORY_DAYS - 1)
    dates = [start + timedelta(days=i) for i in range(_HISTORY_DAYS)]

    n_weeks = (len(dates) + 6) // 7 + 1
    cmap = plt.colormaps["RdYlGn"]

    fig, ax = plt.subplots(figsize=(12, 3))
    seen_months: set[int] = set()

    for d in dates:
        col = (d - start).days // 7
        row = d.weekday()  # 0=Mon … 6=Sun
        rec = history.get(d)

        if rec and rec.get("n_total", 0) > 0:
            pct = rec["n_fetched"] / rec["n_total"]
            color = cmap(pct)
            label = f"{int(pct * 100)}%"
        elif d.weekday() < 5:
            color = "#d0d0d0"  # weekday with no run (laptop off)
            label = ""
        else:
            color = "#f5f5f5"  # weekend
            label = ""

        rect = mpatches.FancyBboxPatch(
            (col + 0.05, 6 - row + 0.05),
            0.9,
            0.9,
            boxstyle="round,pad=0.05",
            linewidth=0,
            facecolor=color,
        )
        ax.add_patch(rect)
        if label:
            ax.text(col + 0.5, 6 - row + 0.5, label, ha="center", va="center", fontsize=6)

        if d.month not in seen_months:
            ax.text(col + 0.5, 7.4, d.strftime("%b"), ha="center", fontsize=7)
            seen_months.add(d.month)

    for i, name in enumerate(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]):
        ax.text(-0.5, 6 - i + 0.5, name, ha="right", va="center", fontsize=7)

    ax.set_xlim(-1, n_weeks)
    ax.set_ylim(0, 8)
    ax.axis("off")
    ax.set_title(f"OHLCV ingest coverage — last {_HISTORY_DAYS} days", fontsize=9, pad=4)

    legend_items = [
        mpatches.Patch(color=cmap(1.0), label="100%"),
        mpatches.Patch(color=cmap(0.5), label="~50%"),
        mpatches.Patch(color=cmap(0.0), label="<10%"),
        mpatches.Patch(color="#d0d0d0", label="missed"),
    ]
    ax.legend(handles=legend_items, loc="lower right", fontsize=6, ncol=4, framealpha=0.7)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", dpi=120)
    plt.close(fig)
    return buf.getvalue()


def _subject(results: dict[str, str]) -> str:
    """Build email subject line."""
    failed = [p for p, s in results.items() if s == "fail"]
    if not failed:
        return "[RDD] Daily ingest OK"
    return f"[RDD] Daily ingest FAILED: {', '.join(failed)}"


def _html_body(
    results: dict[str, str],
    log_paths: dict[str, str],
    n_fetched: int,
    n_total: int,
) -> str:
    """Build the HTML email body."""
    icons = {"ok": "✓", "skip": "—", "fail": "✗"}
    rows = "".join(
        f"<tr><td>{icons.get(s, '?')}</td><td><b>{p}</b></td><td>{s.upper()}</td></tr>"
        for p, s in results.items()
    )
    coverage = (
        f"{n_fetched} / {n_total} stocks ({int(n_fetched / n_total * 100)}%)"
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
<p><b>Stocks fetched today:</b> {coverage}</p>
<p><img src="cid:heatmap" alt="Coverage heatmap" style="max-width:820px"/></p>
{log_sections}
</body></html>
"""


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

    n_fetched, n_total = _count_tickers_fetched(args.ohlcv_dir)
    overall = "fail" if any(s == "fail" for s in results.values()) else "ok"
    _append_manifest(date.today(), n_fetched, n_total, overall)

    history = _load_manifest()
    heatmap_png = _make_heatmap(history)

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
    alt.attach(MIMEText(_html_body(results, log_paths, n_fetched, n_total), "html"))
    msg.attach(alt)

    img = MIMEImage(heatmap_png)
    img.add_header("Content-ID", "<heatmap>")
    img.add_header("Content-Disposition", "inline", filename="heatmap.png")
    msg.attach(img)

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_host, smtp_port, context=ctx, timeout=30) as conn:
        conn.login(smtp_user, smtp_pass)
        conn.send_message(msg)

    sys.stdout.write(f"Report sent to {to_addr}\n")


if __name__ == "__main__":
    main()
