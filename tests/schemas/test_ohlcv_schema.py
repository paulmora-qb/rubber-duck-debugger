"""Unit tests for OHLCVSchema."""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
import pytest

from rdd.schemas.ohlcv import OHLCVSchema


def test_valid_data_passes(ohlcv_df: pd.DataFrame) -> None:
    OHLCVSchema.validate(ohlcv_df)


def test_nullable_open_passes(ohlcv_df: pd.DataFrame) -> None:
    ohlcv_df.loc[0, "open"] = None
    OHLCVSchema.validate(ohlcv_df)


def test_ticker_null_fails(ohlcv_df: pd.DataFrame) -> None:
    ohlcv_df.loc[0, "ticker"] = None
    with pytest.raises(pa.errors.SchemaError):
        OHLCVSchema.validate(ohlcv_df)


def test_negative_close_fails(ohlcv_df: pd.DataFrame) -> None:
    ohlcv_df.loc[0, "close"] = -1.0
    with pytest.raises(pa.errors.SchemaError):
        OHLCVSchema.validate(ohlcv_df)


def test_high_lt_low_fails(ohlcv_df: pd.DataFrame) -> None:
    ohlcv_df.loc[0, "high"] = ohlcv_df.loc[0, "low"] - 1.0
    with pytest.raises(pa.errors.SchemaError):
        OHLCVSchema.validate(ohlcv_df)


def test_close_above_high_fails(ohlcv_df: pd.DataFrame) -> None:
    ohlcv_df.loc[0, "close"] = ohlcv_df.loc[0, "high"] + 1.0
    with pytest.raises(pa.errors.SchemaError):
        OHLCVSchema.validate(ohlcv_df)


def test_close_below_low_fails(ohlcv_df: pd.DataFrame) -> None:
    ohlcv_df.loc[0, "close"] = ohlcv_df.loc[0, "low"] - 1.0
    with pytest.raises(pa.errors.SchemaError):
        OHLCVSchema.validate(ohlcv_df)


def test_extra_column_fails(ohlcv_df: pd.DataFrame) -> None:
    ohlcv_df["extra"] = 1
    # strict=True raises SchemaErrors (plural) for unexpected columns
    with pytest.raises(pa.errors.SchemaErrors):
        OHLCVSchema.validate(ohlcv_df)


def test_schema_fields_have_descriptions() -> None:
    schema = OHLCVSchema.to_schema()
    for col_name, col in schema.columns.items():
        assert col.description, f"Column '{col_name}' is missing a description"
