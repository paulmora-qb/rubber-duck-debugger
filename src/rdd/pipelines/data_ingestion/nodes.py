"""Nodes for the data ingestion pipeline."""

from __future__ import annotations

import io
import logging
from collections.abc import Callable
from typing import Any

import pandas as pd
import requests
import yfinance as yf

logger = logging.getLogger(__name__)

_SP500_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
_NASDAQ100_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"


_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}


def _normalise_ticker(ticker: str) -> str:
    """Replace dots with hyphens to match yfinance conventions (BRK.B → BRK-B)."""
    return ticker.replace(".", "-")


def _read_html_with_headers(url: str, **kwargs: Any) -> list[pd.DataFrame]:
    """Fetch a URL with a browser User-Agent then parse HTML tables."""
    resp = requests.get(url, headers=_HEADERS, timeout=30)
    resp.raise_for_status()
    return pd.read_html(io.StringIO(resp.text), **kwargs)


def _fetch_sp500() -> list[str]:
    tables = _read_html_with_headers(_SP500_URL, attrs={"id": "constituents"})
    return tables[0]["Symbol"].map(_normalise_ticker).tolist()


def _fetch_nasdaq100() -> list[str]:
    tables = _read_html_with_headers(_NASDAQ100_URL)
    for tbl in tables:
        if "Ticker" in tbl.columns:
            return tbl["Ticker"].map(_normalise_ticker).tolist()
    msg = "Could not find Ticker column in any Nasdaq-100 Wikipedia table"
    raise ValueError(msg)


def fetch_ticker_universe(params: dict[str, Any]) -> list[str]:
    """Fetch the equity universe from configured index sources.

    Pulls constituents from S&P 500 and/or NASDAQ 100. Deduplicates across
    sources and returns a sorted list of yfinance-compatible ticker symbols.

    Args:
        params: ``data_ingestion`` parameter block from ``params_data_ingestion.yml``.

    Returns:
        Sorted, deduplicated list of ticker symbols.
    """
    sources = params["index_sources"]
    tickers: set[str] = set()

    if sources.get("sp500"):
        fetched = _fetch_sp500()
        logger.info("S&P 500: %d tickers", len(fetched))
        tickers.update(fetched)

    if sources.get("nasdaq100"):
        fetched = _fetch_nasdaq100()
        logger.info("NASDAQ 100: %d tickers", len(fetched))
        tickers.update(fetched)

    result = sorted(tickers)
    logger.info("Total unique tickers in universe: %d", len(result))
    return result


def _wide_to_long(raw: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    """Reshape yfinance multi-ticker wide output to a long-format DataFrame.

    Args:
        raw: Wide DataFrame from ``yf.download`` with MultiIndex columns
            (price_type, ticker).
        tickers: List of tickers that were requested.

    Returns:
        Long DataFrame with columns [ticker, date, open, high, low, close,
        adj_close, volume].
    """
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = ["_".join(c).strip().lower() for c in raw.columns]
    else:
        # Single-ticker download — columns are plain price names
        raw.columns = [c.lower().replace(" ", "_") for c in raw.columns]
        raw = raw.copy()
        raw.insert(0, "ticker", tickers[0])
        raw.index.name = "date"
        return raw.reset_index()

    raw = raw.reset_index().rename(columns={"Date": "date", "Datetime": "date"}).copy()
    raw = raw.rename(columns={"index": "date"})

    rows = []
    for ticker in tickers:
        cols = {
            "date": "date",
            f"open_{ticker.lower()}": "open",
            f"high_{ticker.lower()}": "high",
            f"low_{ticker.lower()}": "low",
            f"close_{ticker.lower()}": "close",
            f"adj close_{ticker.lower()}": "adj_close",
            f"volume_{ticker.lower()}": "volume",
        }
        present = {k: v for k, v in cols.items() if k in raw.columns}
        if len(present) <= 1:
            continue
        chunk = raw[list(present.keys())].rename(columns=present)
        chunk.insert(0, "ticker", ticker)
        rows.append(chunk)

    if not rows:
        return pd.DataFrame(
            columns=["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]
        )
    return pd.concat(rows, ignore_index=True)


def ingest_ohlcv(
    ticker_universe: list[str],
    existing_ohlcv: dict[str, Callable[[], pd.DataFrame]],
    params: dict[str, Any],
) -> dict[str, pd.DataFrame]:
    """Download OHLCV bars from yfinance and merge with any existing stored data.

    Determines the effective fetch window per ticker:
    - First run (no existing partition): fetches from ``params.start_date``.
    - Subsequent runs: fetches from the day after the latest stored date.

    Downloads are batched in groups of ``params.batch_size`` tickers to stay
    within yfinance's practical limits.

    Args:
        ticker_universe: List of ticker symbols to ingest.
        existing_ohlcv: Mapping of ticker → lazy loader returned by Kedro's
            ``PartitionedDataset``. Call ``loader()`` to materialise the DataFrame.
        params: ``data_ingestion`` parameter block.

    Returns:
        Mapping of ticker → merged DataFrame for the ``PartitionedDataset`` to persist.
    """
    default_start = pd.Timestamp(params["start_date"])
    batch_size = int(params["batch_size"])
    end_date = pd.Timestamp.today().normalize()

    # Determine per-ticker effective start date
    ticker_starts: dict[str, pd.Timestamp] = {}
    existing_data: dict[str, pd.DataFrame] = {}

    for ticker in ticker_universe:
        partition_key = ticker.lower()
        if partition_key in existing_ohlcv:
            try:
                df = existing_ohlcv[partition_key]()
                if not df.empty and "date" in df.columns:
                    existing_data[ticker] = df
                    ticker_starts[ticker] = pd.Timestamp(df["date"].max()) + pd.Timedelta(days=1)
                    continue
            except Exception:
                logger.warning("Could not load existing partition for %s — will re-fetch.", ticker)
        ticker_starts[ticker] = default_start

    global_start = min(ticker_starts.values())

    if global_start >= end_date:
        logger.info("All tickers are up to date. Nothing to fetch.")
        return {t.lower(): existing_data[t] for t in ticker_universe if t in existing_data}

    logger.info(
        "Fetching %d tickers from %s to %s in batches of %d.",
        len(ticker_universe),
        global_start.date(),
        end_date.date(),
        batch_size,
    )

    # Download in batches and accumulate new bars per ticker
    new_bars: dict[str, pd.DataFrame] = {}
    for i in range(0, len(ticker_universe), batch_size):
        batch = ticker_universe[i : i + batch_size]
        logger.info(
            "Downloading batch %d/%d (%s … %s)",
            i // batch_size + 1,
            -(-len(ticker_universe) // batch_size),
            batch[0],
            batch[-1],
        )
        try:
            raw = yf.download(
                batch,
                start=global_start,
                end=end_date + pd.Timedelta(days=1),
                auto_adjust=False,
                progress=False,
            )
        except Exception:
            logger.warning("yfinance download failed for batch starting at %s — skipping.", batch[0], exc_info=True)
            continue

        if raw.empty:
            continue

        long_df = _wide_to_long(raw, batch)
        long_df["date"] = pd.to_datetime(long_df["date"])
        long_df = long_df.dropna(subset=["close"])

        for ticker in batch:
            chunk = long_df[long_df["ticker"] == ticker].copy()
            if not chunk.empty:
                new_bars[ticker] = chunk

    # Merge new bars with existing, validate, and build output dict
    result: dict[str, pd.DataFrame] = {}

    for ticker in ticker_universe:
        new = new_bars.get(ticker)
        old = existing_data.get(ticker)

        if new is None and old is None:
            continue

        if new is not None and old is not None:
            # Keep only rows strictly newer than what's already stored
            cutoff = ticker_starts[ticker]
            new = new[new["date"] >= cutoff]
            merged = (
                pd.concat([old, new], ignore_index=True)
                .drop_duplicates(subset=["date"])
                .sort_values("date")
                .reset_index(drop=True)
            )
        else:
            merged = (new if new is not None else old).sort_values("date").reset_index(drop=True)

        result[ticker.lower()] = merged

    logger.info("Ingestion complete. %d tickers written.", len(result))
    return result
