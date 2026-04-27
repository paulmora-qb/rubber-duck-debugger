"""Project pipeline registry."""

from kedro.pipeline import Pipeline

from rdd.pipelines.company_financials.pipeline import (
    create_pipeline as company_financials,
)
from rdd.pipelines.company_info.pipeline import create_pipeline as company_info
from rdd.pipelines.company_news.pipeline import create_pipeline as company_news
from rdd.pipelines.data_ingestion.pipeline import create_pipeline as data_ingestion


def register_pipelines() -> dict[str, Pipeline]:
    """Register all project pipelines."""
    di = data_ingestion()
    ci = company_info()
    cn = company_news()
    cf = company_financials()
    return {
        "__default__": di + ci + cn + cf,
        "data_ingestion": di,
        "company_info": ci,
        "company_news": cn,
        "company_financials": cf,
    }
