"""Tests for CompanyInfoSchema validation."""

from __future__ import annotations

import pandera.pandas as pa
import pytest

from rdd.schemas.company_info import CompanyInfoSchema
from tests.conftest import make_company_info_df


class TestCompanyInfoSchema:
    def test_valid_row_passes(self) -> None:
        CompanyInfoSchema.validate(make_company_info_df())

    def test_nullable_fields_accept_none(self) -> None:
        df = make_company_info_df()
        for col in [
            "name",
            "sector",
            "industry",
            "market_cap",
            "employees",
            "country",
            "currency",
            "exchange",
        ]:
            bad = df.copy()
            bad[col] = None
            CompanyInfoSchema.validate(bad)

    def test_ticker_not_nullable(self) -> None:
        df = make_company_info_df()
        df["ticker"] = None
        with pytest.raises((pa.errors.SchemaError, pa.errors.SchemaErrors)):
            CompanyInfoSchema.validate(df)

    def test_fetched_at_not_nullable(self) -> None:
        df = make_company_info_df()
        df["fetched_at"] = None
        with pytest.raises((pa.errors.SchemaError, pa.errors.SchemaErrors)):
            CompanyInfoSchema.validate(df)

    def test_negative_market_cap_rejected(self) -> None:
        df = make_company_info_df()
        df["market_cap"] = -1.0
        with pytest.raises((pa.errors.SchemaError, pa.errors.SchemaErrors)):
            CompanyInfoSchema.validate(df)

    def test_negative_employees_rejected(self) -> None:
        df = make_company_info_df()
        df["employees"] = -1.0
        with pytest.raises((pa.errors.SchemaError, pa.errors.SchemaErrors)):
            CompanyInfoSchema.validate(df)

    def test_extra_columns_rejected(self) -> None:
        df = make_company_info_df()
        df["unexpected_col"] = "x"
        with pytest.raises((pa.errors.SchemaError, pa.errors.SchemaErrors)):
            CompanyInfoSchema.validate(df)
