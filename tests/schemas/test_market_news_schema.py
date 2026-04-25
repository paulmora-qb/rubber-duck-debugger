"""Tests for MarketNewsSchema."""

from __future__ import annotations

import pandas as pd
import pandera.errors
import pytest

from rdd.schemas.market_news import MarketNewsSchema
from tests.conftest import make_market_news_df


class TestMarketNewsSchemaValid:
    def test_valid_data_passes(self, market_news_df):
        MarketNewsSchema.validate(market_news_df)

    def test_null_summary_passes(self, market_news_df):
        market_news_df["summary"] = None
        MarketNewsSchema.validate(market_news_df)

    def test_null_image_passes(self, market_news_df):
        market_news_df["image"] = None
        MarketNewsSchema.validate(market_news_df)


class TestMarketNewsSchemaInvalid:
    def test_null_article_id_fails(self, market_news_df):
        market_news_df.loc[0, "article_id"] = None
        with pytest.raises((pandera.errors.SchemaError, pandera.errors.SchemaErrors)):
            MarketNewsSchema.validate(market_news_df)

    def test_null_headline_fails(self, market_news_df):
        market_news_df.loc[0, "headline"] = None
        with pytest.raises((pandera.errors.SchemaError, pandera.errors.SchemaErrors)):
            MarketNewsSchema.validate(market_news_df)

    def test_null_source_fails(self, market_news_df):
        market_news_df.loc[0, "source"] = None
        with pytest.raises((pandera.errors.SchemaError, pandera.errors.SchemaErrors)):
            MarketNewsSchema.validate(market_news_df)

    def test_null_url_fails(self, market_news_df):
        market_news_df.loc[0, "url"] = None
        with pytest.raises((pandera.errors.SchemaError, pandera.errors.SchemaErrors)):
            MarketNewsSchema.validate(market_news_df)

    def test_null_category_fails(self, market_news_df):
        market_news_df.loc[0, "category"] = None
        with pytest.raises((pandera.errors.SchemaError, pandera.errors.SchemaErrors)):
            MarketNewsSchema.validate(market_news_df)

    def test_extra_column_fails(self, market_news_df):
        market_news_df["extra"] = "x"
        with pytest.raises(pandera.errors.SchemaErrors):
            MarketNewsSchema.validate(market_news_df)


def test_schema_fields_have_descriptions():
    schema = MarketNewsSchema.to_schema()
    for col_name, col in schema.columns.items():
        assert col.description, f"Column '{col_name}' is missing a description"


def test_make_market_news_df_is_valid():
    """Ensure the shared test helper always produces schema-valid data."""
    for n in (1, 5, 20):
        df = make_market_news_df(n=n)
        MarketNewsSchema.validate(df)
