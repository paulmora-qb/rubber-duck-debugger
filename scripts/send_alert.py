#!/usr/bin/env python3
"""Send a minimal failure alert email via Gmail SMTP.

Called by cron scripts via a bash EXIT trap whenever a job exits non-zero
before its normal completion path.

Usage:
    send_alert.py --subject "..." --log /path/to/job.log
"""

from __future__ import annotations

import argparse
import os
import smtplib
import ssl
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

_LOG_TAIL = 80


def _tail(path: str, n: int = _LOG_TAIL) -> str:
    """Return the last *n* lines of *path*, or a placeholder if missing."""
    p = Path(path)
    if not p.exists():
        return "(log file not found)"
    lines = p.read_text().splitlines()
    return "\n".join(lines[-n:]) if lines else "(empty log)"


def _build_html(subject: str, log_tail: str) -> str:
    """Return an HTML email body for a failure alert."""
    log_section = (
        f"<h3>Last log lines</h3>"
        f"<pre style='font-size:11px;background:#f8f8f8;padding:12px'>{log_tail}</pre>"
        if log_tail
        else ""
    )
    return (
        "<html><body style='font-family:sans-serif;max-width:800px'>"
        f"<h2 style='color:#c0392b'>{subject}</h2>"
        f"{log_section}"
        "</body></html>"
    )


def main() -> None:
    """Send failure alert email."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subject", required=True, help="Email subject line")
    parser.add_argument("--log", default=None, help="Path to the job log file")
    args = parser.parse_args()

    to_addr = os.environ.get("RDD_EMAIL_TO", "")
    smtp_host = os.environ.get("RDD_SMTP_HOST", "smtp.gmail.com")
    smtp_port = int(os.environ.get("RDD_SMTP_PORT", "465"))
    smtp_user = os.environ.get("RDD_SMTP_USER", "")
    smtp_pass = os.environ.get("RDD_SMTP_PASS", "")

    if not all([to_addr, smtp_user, smtp_pass]):
        sys.exit(
            "Missing SMTP credentials (RDD_EMAIL_TO / RDD_SMTP_USER / RDD_SMTP_PASS)"
        )

    log_tail = _tail(args.log) if args.log else ""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = args.subject
    msg["From"] = smtp_user
    msg["To"] = to_addr
    msg.attach(MIMEText(_build_html(args.subject, log_tail), "html"))

    ctx = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_host, smtp_port, context=ctx, timeout=30) as conn:
        conn.login(smtp_user, smtp_pass)
        conn.send_message(msg)

    sys.stdout.write(f"Alert sent to {to_addr}\n")


if __name__ == "__main__":
    main()
