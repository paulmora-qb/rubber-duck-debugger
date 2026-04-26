"""Tests for CompanyNewsSchema validation."""

from __future__ import annotations

import pandera.pandas as pa
import pytest

from rdd.schemas.company_news import CompanyNewsSchema
from tests.conftest import make_company_news_df


class TestCompanyNewsSchema:
    def test_valid_df_passes(self) -> None:
        CompanyNewsSchema.validate(make_company_news_df())

    def test_nullable_fields_accept_none(self) -> None:
        for col in ["headline", "summary", "source", "url", "category"]:
            df = make_company_news_df()
            df[col] = None
            CompanyNewsSchema.validate(df)

    def test_ticker_not_nullable(self) -> None:
        df = make_company_news_df()
        df["ticker"] = None
        with pytest.raises((pa.errors.SchemaError, pa.errors.SchemaErrors)):
            CompanyNewsSchema.validate(df)

    def test_published_at_not_nullable(self) -> None:
        df = make_company_news_df()
        df["published_at"] = None
        with pytest.raises((pa.errors.SchemaError, pa.errors.SchemaErrors)):
            CompanyNewsSchema.validate(df)

    def test_extra_columns_rejected(self) -> None:
        df = make_company_news_df()
        df["unexpected_col"] = "x"
        with pytest.raises((pa.errors.SchemaError, pa.errors.SchemaErrors)):
            CompanyNewsSchema.validate(df)

    def test_published_at_coerced_from_string(self) -> None:
        df = make_company_news_df()
        df["published_at"] = df["published_at"].astype(str)
        CompanyNewsSchema.validate(df)
