"""Tests for CompanyNewsSchema."""

from __future__ import annotations

import pandera.errors
import pytest

from rdd.schemas.company_news import CompanyNewsSchema
from tests.conftest import make_company_news_df


class TestCompanyNewsSchemaValid:
    def test_valid_data_passes(self, company_news_df):
        CompanyNewsSchema.validate(company_news_df)

    def test_null_summary_passes(self, company_news_df):
        company_news_df["summary"] = None
        CompanyNewsSchema.validate(company_news_df)


class TestCompanyNewsSchemaInvalid:
    def test_null_article_id_fails(self, company_news_df):
        company_news_df.loc[0, "article_id"] = None
        with pytest.raises((pandera.errors.SchemaError, pandera.errors.SchemaErrors)):
            CompanyNewsSchema.validate(company_news_df)

    def test_null_ticker_fails(self, company_news_df):
        company_news_df.loc[0, "ticker"] = None
        with pytest.raises((pandera.errors.SchemaError, pandera.errors.SchemaErrors)):
            CompanyNewsSchema.validate(company_news_df)

    def test_null_datetime_fails(self, company_news_df):
        company_news_df.loc[0, "datetime"] = None
        with pytest.raises((pandera.errors.SchemaError, pandera.errors.SchemaErrors)):
            CompanyNewsSchema.validate(company_news_df)

    def test_null_headline_fails(self, company_news_df):
        company_news_df.loc[0, "headline"] = None
        with pytest.raises((pandera.errors.SchemaError, pandera.errors.SchemaErrors)):
            CompanyNewsSchema.validate(company_news_df)

    def test_null_source_fails(self, company_news_df):
        company_news_df.loc[0, "source"] = None
        with pytest.raises((pandera.errors.SchemaError, pandera.errors.SchemaErrors)):
            CompanyNewsSchema.validate(company_news_df)

    def test_null_url_fails(self, company_news_df):
        company_news_df.loc[0, "url"] = None
        with pytest.raises((pandera.errors.SchemaError, pandera.errors.SchemaErrors)):
            CompanyNewsSchema.validate(company_news_df)

    def test_extra_column_fails(self, company_news_df):
        company_news_df["extra"] = "x"
        with pytest.raises(pandera.errors.SchemaErrors):
            CompanyNewsSchema.validate(company_news_df)


def test_schema_fields_have_descriptions():
    schema = CompanyNewsSchema.to_schema()
    for col_name, col in schema.columns.items():
        assert col.description, f"Column '{col_name}' is missing a description"


def test_make_company_news_df_is_valid():
    """Ensure the shared test helper always produces schema-valid data."""
    for n in (1, 5, 20):
        df = make_company_news_df(n=n)
        CompanyNewsSchema.validate(df)
