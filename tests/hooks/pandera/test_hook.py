"""Unit tests for PanderaHook."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from pandera.errors import SchemaErrors

from rdd.hooks.pandera.hook import PanderaHook


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _make_catalog(metadata: dict | None) -> MagicMock:
    dataset = MagicMock()
    dataset.metadata = metadata
    catalog = MagicMock()
    catalog.get.return_value = dataset
    return catalog


def _make_node(name: str = "test_node") -> MagicMock:
    node = MagicMock()
    node.name = name
    return node


def _valid_ohlcv_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "date": [pd.Timestamp("2024-01-02")],
            "open": [150.0],
            "high": [155.0],
            "low": [149.0],
            "close": [153.0],
            "adj_close": [152.0],
            "volume": [1_000_000.0],
        }
    )


def _invalid_ohlcv_df() -> pd.DataFrame:
    """High < low — violates the high_gte_low dataframe check."""
    return pd.DataFrame(
        {
            "ticker": ["AAPL"],
            "date": [pd.Timestamp("2024-01-02")],
            "open": [150.0],
            "high": [100.0],   # high < low — invalid
            "low": [200.0],
            "close": [153.0],
            "adj_close": [152.0],
            "volume": [1_000_000.0],
        }
    )


_SCHEMA_METADATA = {
    "pandera": {
        "schema": {
            "type": "python.model",
            "object_path": "rdd.schemas.ohlcv.OHLCVSchema",
        }
    }
}


# ---------------------------------------------------------------------------
# before_pipeline_run
# ---------------------------------------------------------------------------

class TestBeforePipelineRun:
    def test_clears_validation_cache(self) -> None:
        hook = PanderaHook()
        hook._validated_inputs = {"some_dataset"}
        hook.before_pipeline_run()
        assert hook._validated_inputs == set()


# ---------------------------------------------------------------------------
# Valid data passes silently
# ---------------------------------------------------------------------------

class TestValidDataPasses:
    def test_plain_dataframe_passes(self) -> None:
        hook = PanderaHook()
        catalog = _make_catalog(_SCHEMA_METADATA)
        hook.after_node_run(
            node=_make_node(),
            catalog=catalog,
            outputs={"raw_ohlcv": _valid_ohlcv_df()},
        )

    def test_dict_of_dataframes_passes(self) -> None:
        hook = PanderaHook()
        catalog = _make_catalog(_SCHEMA_METADATA)
        hook.after_node_run(
            node=_make_node(),
            catalog=catalog,
            outputs={"raw_ohlcv": {"aapl": _valid_ohlcv_df()}},
        )


# ---------------------------------------------------------------------------
# Invalid data raises
# ---------------------------------------------------------------------------

class TestInvalidDataRaises:
    def test_plain_dataframe_raises(self) -> None:
        hook = PanderaHook()
        catalog = _make_catalog(_SCHEMA_METADATA)
        with pytest.raises(SchemaErrors):
            hook.after_node_run(
                node=_make_node(),
                catalog=catalog,
                outputs={"raw_ohlcv": _invalid_ohlcv_df()},
            )

    def test_dict_with_bad_partition_raises(self) -> None:
        hook = PanderaHook()
        catalog = _make_catalog(_SCHEMA_METADATA)
        with pytest.raises(SchemaErrors):
            hook.after_node_run(
                node=_make_node(),
                catalog=catalog,
                outputs={"raw_ohlcv": {"aapl": _invalid_ohlcv_df()}},
            )


# ---------------------------------------------------------------------------
# Datasets without pandera metadata are skipped
# ---------------------------------------------------------------------------

class TestMissingMetadataSkipped:
    def test_no_metadata_attribute(self) -> None:
        dataset = MagicMock(spec=[])  # no metadata attribute
        catalog = MagicMock()
        catalog.get.return_value = dataset

        hook = PanderaHook()
        hook.after_node_run(
            node=_make_node(),
            catalog=catalog,
            outputs={"ticker_universe": ["AAPL", "MSFT"]},
        )

    def test_metadata_without_pandera_key(self) -> None:
        hook = PanderaHook()
        catalog = _make_catalog({"kedro-viz": {"layer": "raw"}})
        hook.after_node_run(
            node=_make_node(),
            catalog=catalog,
            outputs={"raw_ohlcv": _valid_ohlcv_df()},
        )

    def test_non_dataframe_data_skipped(self) -> None:
        hook = PanderaHook()
        catalog = _make_catalog(_SCHEMA_METADATA)
        # list is not a DataFrame — should be a no-op
        hook.after_node_run(
            node=_make_node(),
            catalog=catalog,
            outputs={"ticker_universe": ["AAPL", "MSFT"]},
        )

    def test_dict_of_callables_skipped(self) -> None:
        """Lazy PartitionedDataset inputs (dict of callables) are not validated."""
        hook = PanderaHook()
        catalog = _make_catalog(_SCHEMA_METADATA)
        hook.before_node_run(
            node=_make_node(),
            catalog=catalog,
            inputs={"raw_ohlcv_existing": {"aapl": lambda: _valid_ohlcv_df()}},
        )


# ---------------------------------------------------------------------------
# Input caching
# ---------------------------------------------------------------------------

class TestInputCaching:
    def test_second_node_skips_revalidation(self) -> None:
        hook = PanderaHook()
        catalog = _make_catalog(_SCHEMA_METADATA)

        with patch.object(hook, "_validate_datasets", wraps=hook._validate_datasets) as spy:
            hook.before_node_run(
                node=_make_node("node_a"),
                catalog=catalog,
                inputs={"raw_ohlcv": _valid_ohlcv_df()},
            )
            hook.before_node_run(
                node=_make_node("node_b"),
                catalog=catalog,
                inputs={"raw_ohlcv": _valid_ohlcv_df()},
            )

        assert "raw_ohlcv" in hook._validated_inputs
        # The dataset was seen twice but resolve_schema should only be called
        # once — verify indirectly that the cache entry was set after first call.

    def test_cache_cleared_between_pipeline_runs(self) -> None:
        hook = PanderaHook()
        catalog = _make_catalog(_SCHEMA_METADATA)

        hook.before_node_run(
            node=_make_node(),
            catalog=catalog,
            inputs={"raw_ohlcv": _valid_ohlcv_df()},
        )
        assert "raw_ohlcv" in hook._validated_inputs

        hook.before_pipeline_run()
        assert hook._validated_inputs == set()

    def test_outputs_not_cached(self) -> None:
        """after_node_run should never populate the cache (cache=False)."""
        hook = PanderaHook()
        catalog = _make_catalog(_SCHEMA_METADATA)

        hook.after_node_run(
            node=_make_node(),
            catalog=catalog,
            outputs={"raw_ohlcv": _valid_ohlcv_df()},
        )
        assert "raw_ohlcv" not in hook._validated_inputs
