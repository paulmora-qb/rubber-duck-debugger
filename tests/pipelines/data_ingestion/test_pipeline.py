"""Integration test for the data_ingestion pipeline.

Runs both nodes (fetch_ticker_universe → ingest_ohlcv) end-to-end via
SequentialRunner with a fully in-memory DataCatalog.  All network calls
are mocked at the module boundary so this is safe for CI.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest
from kedro.io import DataCatalog, MemoryDataset
from kedro.runner import SequentialRunner

from rdd.pipelines.data_ingestion.pipeline import create_pipeline

# ---------------------------------------------------------------------------
# Minimal HTML that satisfies each index-source scraper
# ---------------------------------------------------------------------------

_SP500_HTML = """
<html><body>
<table id="constituents">
<tr><th>Symbol</th><th>Security</th></tr>
<tr><td>AAPL</td><td>Apple Inc.</td></tr>
<tr><td>MSFT</td><td>Microsoft</td></tr>
</table>
</body></html>
"""


def _mock_http_response(html: str) -> MagicMock:
    resp = MagicMock()
    resp.text = html
    resp.raise_for_status.return_value = None
    return resp


def _make_yf_response(tickers: list[str], dates: pd.DatetimeIndex) -> pd.DataFrame:
    """Minimal yfinance-style multi-ticker wide DataFrame."""
    close = 100.0
    data: dict[tuple[str, str], list[float]] = {}
    for ticker in tickers:
        data[("Open", ticker)] = [close * 1.005] * len(dates)
        data[("High", ticker)] = [close * 1.01] * len(dates)
        data[("Low", ticker)] = [close * 0.99] * len(dates)
        data[("Close", ticker)] = [close] * len(dates)
        data[("Adj Close", ticker)] = [close * 0.98] * len(dates)
        data[("Volume", ticker)] = [1_000_000.0] * len(dates)
    cols = pd.MultiIndex.from_tuples(data.keys(), names=["Price", "Ticker"])
    df = pd.DataFrame(list(data.values()), index=cols).T
    df.index = dates
    df.index.name = "Date"
    return df


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def pipeline():
    """The data_ingestion Kedro pipeline."""
    return create_pipeline()


@pytest.fixture
def params():
    """Minimal parameter block: SP 500 only, tiny batch."""
    return {
        "start_date": "2023-01-01",
        "batch_size": 10,
        "index_sources": {"sp500": True, "nasdaq100": False},
    }


@pytest.fixture
def in_memory_catalog(params):
    """Fully in-memory DataCatalog — no filesystem I/O."""
    return DataCatalog(
        {
            "params:data_ingestion": MemoryDataset(data=params),
            "ticker_universe": MemoryDataset(),
            "raw_ohlcv_existing": MemoryDataset(data={}),
            "raw_ohlcv": MemoryDataset(),
        }
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_pipeline_runs_successfully(mocker, pipeline, in_memory_catalog) -> None:
    """Full pipeline executes without error and writes OHLCV output."""
    tickers = ["AAPL", "MSFT"]
    dates = pd.date_range("2023-01-03", periods=3, freq="B")

    mocker.patch(
        "rdd.pipelines.data_ingestion.nodes.requests.get",
        return_value=_mock_http_response(_SP500_HTML),
    )
    mocker.patch(
        "rdd.pipelines.data_ingestion.nodes.yf.download",
        return_value=_make_yf_response(tickers, dates),
    )

    SequentialRunner().run(pipeline, in_memory_catalog)

    result = in_memory_catalog.load("raw_ohlcv")
    assert isinstance(result, dict)
    assert len(result) > 0


def test_pipeline_output_schema(mocker, pipeline, in_memory_catalog) -> None:
    """Output DataFrames contain the expected OHLCV columns."""
    tickers = ["AAPL", "MSFT"]
    dates = pd.date_range("2023-01-03", periods=3, freq="B")

    mocker.patch(
        "rdd.pipelines.data_ingestion.nodes.requests.get",
        return_value=_mock_http_response(_SP500_HTML),
    )
    mocker.patch(
        "rdd.pipelines.data_ingestion.nodes.yf.download",
        return_value=_make_yf_response(tickers, dates),
    )

    SequentialRunner().run(pipeline, in_memory_catalog)

    result = in_memory_catalog.load("raw_ohlcv")
    expected_cols = {
        "ticker",
        "date",
        "open",
        "high",
        "low",
        "close",
        "adj_close",
        "volume",
    }
    for key, df in result.items():
        assert expected_cols.issubset(df.columns), f"{key}: missing columns"
        assert not df.empty, f"{key}: empty DataFrame"


def test_pipeline_passes_ticker_universe_between_nodes(
    mocker, pipeline, in_memory_catalog
) -> None:
    """Output tickers match the universe fetched by node-1.

    Kedro releases intermediate MemoryDatasets after the last consumer, so we
    verify the handoff indirectly: the tickers present in raw_ohlcv output
    correspond to those scraped from the index HTML.
    """
    tickers = ["AAPL", "MSFT"]
    dates = pd.date_range("2023-01-03", periods=2, freq="B")

    mocker.patch(
        "rdd.pipelines.data_ingestion.nodes.requests.get",
        return_value=_mock_http_response(_SP500_HTML),
    )
    mocker.patch(
        "rdd.pipelines.data_ingestion.nodes.yf.download",
        return_value=_make_yf_response(tickers, dates),
    )

    SequentialRunner().run(pipeline, in_memory_catalog)

    result = in_memory_catalog.load("raw_ohlcv")
    # Both tickers from the HTML should appear in the output (keys are lowercase)
    assert "aapl" in result
    assert "msft" in result


_SP500_HTML_SINGLE = """
<html><body>
<table id="constituents">
<tr><th>Symbol</th><th>Security</th></tr>
<tr><td>AAPL</td><td>Apple Inc.</td></tr>
</table>
</body></html>
"""


def test_pipeline_with_existing_data_does_incremental_fetch(
    mocker, pipeline, params, ohlcv_df
) -> None:
    """Pipeline only fetches new bars when existing data is present.

    Uses a single-ticker HTML so every ticker in the universe has existing data
    — this ensures global_start is pushed past the existing max date rather than
    falling back to the default start_date.
    """
    ticker = "AAPL"
    existing_df = ohlcv_df.copy()
    existing_df["ticker"] = ticker
    last_existing = pd.Timestamp(existing_df["date"].max())

    catalog = DataCatalog(
        {
            "params:data_ingestion": MemoryDataset(data=params),
            "ticker_universe": MemoryDataset(),
            "raw_ohlcv_existing": MemoryDataset(
                data={ticker.lower(): lambda: existing_df}
            ),
            "raw_ohlcv": MemoryDataset(),
        }
    )

    new_dates = pd.date_range(last_existing + pd.Timedelta(days=1), periods=2, freq="B")
    mock_dl = _make_yf_response([ticker], new_dates)
    dl_spy = mocker.patch(
        "rdd.pipelines.data_ingestion.nodes.yf.download", return_value=mock_dl
    )
    mocker.patch(
        "rdd.pipelines.data_ingestion.nodes.requests.get",
        return_value=_mock_http_response(_SP500_HTML_SINGLE),
    )

    SequentialRunner().run(pipeline, catalog)

    call_kwargs = dl_spy.call_args.kwargs
    assert pd.Timestamp(call_kwargs["start"]) > last_existing
