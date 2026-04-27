"""Project pipeline registry."""

from kedro.pipeline import Pipeline

from rdd.pipelines.company_info.pipeline import create_pipeline as company_info
from rdd.pipelines.company_news.pipeline import create_pipeline as company_news
from rdd.pipelines.data_ingestion.pipeline import create_pipeline as data_ingestion
from rdd.pipelines.strategies.pipeline import create_pipeline as strategies


def register_pipelines() -> dict[str, Pipeline]:
    """Register all project pipelines."""
    di = data_ingestion()
    ci = company_info()
    cn = company_news()
    st = strategies()
    return {
        "__default__": di + ci + cn + st,
        "data_ingestion": di,
        "company_info": ci,
        "company_news": cn,
        "strategies": st,
    }
