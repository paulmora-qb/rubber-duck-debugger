"""Company information pipeline — combines company_info, company_news, and company_financials."""

from kedro.pipeline import Pipeline

from rdd.pipelines.company_information.company_financials.pipeline import (
    create_pipeline as company_financials,
)
from rdd.pipelines.company_information.company_info.pipeline import (
    create_pipeline as company_info,
)
from rdd.pipelines.company_information.company_news.pipeline import (
    create_pipeline as company_news,
)


def create_pipeline(**_kwargs) -> Pipeline:
    """Create the combined company information pipeline."""
    return company_info() + company_news() + company_financials()
