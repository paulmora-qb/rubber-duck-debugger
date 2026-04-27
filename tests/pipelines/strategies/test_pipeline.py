"""Integration tests for the strategies pipeline.

Runs the full strategies pipeline (momentum → trend → mean_reversion →
assemble) via SequentialRunner with a fully in-memory DataCatalog.
No network calls, no filesystem I/O.
"""

from __future__ import annotations

import pytest
from kedro.io import DataCatalog, MemoryDataset
from kedro.runner import SequentialRunner

from rdd.pipelines.strategies.pipeline import create_pipeline
from tests.conftest import make_price_ohlcv

_UPTREND = [100.0 + i * 0.5 for i in range(260)]
_DOWNTREND = [230.0 - i * 0.5 for i in range(260)]


@pytest.fixture
def pipeline():
    """The strategies Kedro pipeline."""
    return create_pipeline()


@pytest.fixture
def params() -> dict:
    return {
        "momentum_windows": {"1m": 21, "3m": 63, "6m": 126, "12m": 252},
        "momentum_min_bullish": 3,
        "trend_short_window": 50,
        "trend_long_window": 200,
        "rsi_window": 14,
        "rsi_oversold": 30.0,
        "rsi_overbought": 70.0,
        "bb_window": 20,
        "bb_std": 2.0,
    }


@pytest.fixture
def catalog(params) -> DataCatalog:
    """In-memory catalog with two tickers of sufficient history."""
    aapl_df = make_price_ohlcv(_UPTREND, ticker="AAPL")
    msft_df = make_price_ohlcv(_DOWNTREND, ticker="MSFT")
    return DataCatalog(
        {
            "params:strategies": MemoryDataset(data=params),
            "raw_ohlcv": MemoryDataset(
                data={
                    "aapl": lambda: aapl_df,
                    "msft": lambda: msft_df,
                }
            ),
            "momentum_signals": MemoryDataset(),
            "trend_signals": MemoryDataset(),
            "mean_reversion_signals": MemoryDataset(),
            "stock_analyses": MemoryDataset(),
        }
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_pipeline_runs_without_error(pipeline, catalog) -> None:
    """Full pipeline executes end-to-end without raising."""
    SequentialRunner().run(pipeline, catalog)


def test_pipeline_produces_stock_analyses(pipeline, catalog) -> None:
    SequentialRunner().run(pipeline, catalog)

    result = catalog.load("stock_analyses")
    assert isinstance(result, dict)
    assert len(result) == 2
    assert "aapl" in result
    assert "msft" in result


def test_pipeline_output_structure(pipeline, catalog) -> None:
    """Each entry has the expected top-level keys and signal list shape."""
    SequentialRunner().run(pipeline, catalog)

    result = catalog.load("stock_analyses")
    for entry in result.values():
        assert {"ticker", "generated_at", "signals"} <= entry.keys()
        for signal in entry["signals"]:
            assert {"strategy", "direction", "metrics"} <= signal.keys()
            assert signal["direction"] in {"bullish", "bearish", "neutral"}


def test_pipeline_emits_all_three_strategies(pipeline, catalog) -> None:
    SequentialRunner().run(pipeline, catalog)

    result = catalog.load("stock_analyses")
    for entry in result.values():
        strategies = {s["strategy"] for s in entry["signals"]}
        assert strategies == {"momentum", "trend", "mean_reversion"}


def test_pipeline_ticker_is_uppercase(pipeline, catalog) -> None:
    SequentialRunner().run(pipeline, catalog)

    result = catalog.load("stock_analyses")
    assert result["aapl"]["ticker"] == "AAPL"
    assert result["msft"]["ticker"] == "MSFT"


def test_pipeline_skips_tickers_with_insufficient_data(pipeline, params) -> None:
    """A ticker with only 5 rows produces no signals but does not crash."""
    short_df = make_price_ohlcv([100.0 + i for i in range(5)], ticker="SHORT")
    full_df = make_price_ohlcv(_UPTREND, ticker="AAPL")

    catalog = DataCatalog(
        {
            "params:strategies": MemoryDataset(data=params),
            "raw_ohlcv": MemoryDataset(
                data={
                    "aapl": lambda: full_df,
                    "short": lambda: short_df,
                }
            ),
            "momentum_signals": MemoryDataset(),
            "trend_signals": MemoryDataset(),
            "mean_reversion_signals": MemoryDataset(),
            "stock_analyses": MemoryDataset(),
        }
    )

    SequentialRunner().run(pipeline, catalog)

    result = catalog.load("stock_analyses")
    assert "aapl" in result
    assert "short" not in result
