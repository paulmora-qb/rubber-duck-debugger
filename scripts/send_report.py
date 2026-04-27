#!/usr/bin/env python3
"""Send daily ingest report email via Gmail SMTP.

Called by run_daily_ingest.sh with triplet args:
    send_report.py <pipeline> <status> <log_path> [<pipeline> <status> <log_path> ...]

Status values: ok | fail | skip
"""

import os
import smtplib
import ssl
import sys
from email.message import EmailMessage
from pathlib import Path

_LOG_TAIL = 50


def _tail(path: str, n: int = _LOG_TAIL) -> str:
    p = Path(path)
    if not p.exists():
        return "(log file not found)"
    lines = p.read_text().splitlines()
    return "\n".join(lines[-n:]) if lines else "(empty log)"


def _subject(results: dict[str, str]) -> str:
    failed = [p for p, s in results.items() if s == "fail"]
    if not failed:
        return "[RDD] Daily ingest OK"
    return f"[RDD] Daily ingest FAILED: {', '.join(failed)}"


def _body(results: dict[str, str], log_paths: dict[str, str]) -> str:
    icons = {"ok": "✓", "skip": "—", "fail": "✗"}
    lines = ["Daily ingest report\n"]
    for pipeline, status in results.items():
        lines.append(f"  {icons.get(status, '?')}  {pipeline}: {status.upper()}")

    failed = [p for p, s in results.items() if s == "fail"]
    if failed:
        lines.append("\n--- Logs (last 50 lines per failed pipeline) ---")
        for p in failed:
            lines.append(f"\n=== {p} ===")
            lines.append(_tail(log_paths.get(p, "")))

    return "\n".join(lines)


def main() -> None:
    """Send daily ingest report email."""
    args = sys.argv[1:]
    if not args or len(args) % 3 != 0:
        sys.exit("Usage: send_report.py <pipeline> <status> <log_path> ...")

    results: dict[str, str] = {}
    log_paths: dict[str, str] = {}
    for i in range(0, len(args), 3):
        name, status, log_path = args[i], args[i + 1], args[i + 2]
        results[name] = status
        log_paths[name] = log_path

    to_addr = os.environ["RDD_EMAIL_TO"]
    smtp_host = os.environ.get("RDD_SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("RDD_SMTP_PORT", "587"))
    smtp_user = os.environ["RDD_SMTP_USER"]
    smtp_pass = os.environ["RDD_SMTP_PASS"]

    msg = EmailMessage()
    msg["Subject"] = _subject(results)
    msg["From"] = smtp_user
    msg["To"] = to_addr
    msg.set_content(_body(results, log_paths))

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_host, smtp_port, context=ctx, timeout=30) as conn:
        conn.login(smtp_user, smtp_pass)
        conn.send_message(msg)

    sys.stdout.write(f"Report sent to {to_addr}\n")


if __name__ == "__main__":
    main()
