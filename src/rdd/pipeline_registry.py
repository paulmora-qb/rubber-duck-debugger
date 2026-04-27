"""Project pipeline registry."""

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
from rdd.pipelines.company_information.pipeline import (
    create_pipeline as company_information,
)
from rdd.pipelines.data_ingestion.pipeline import create_pipeline as data_ingestion


def register_pipelines() -> dict[str, Pipeline]:
    """Register all project pipelines."""
    di = data_ingestion()
    ci_all = company_information()
    return {
        "__default__": di + ci_all,
        "data_ingestion": di,
        "company_information": ci_all,
        "company_info": company_info(),
        "company_news": company_news(),
        "company_financials": company_financials(),
    }
