"""Project pipeline registry."""

from kedro.pipeline import Pipeline

from rdd.pipelines.analyst_consensus.pipeline import (
    create_pipeline as analyst_consensus,
)
from rdd.pipelines.company_financials.pipeline import (
    create_pipeline as company_financials,
)
from rdd.pipelines.company_info.pipeline import create_pipeline as company_info
from rdd.pipelines.company_news.pipeline import create_pipeline as company_news
from rdd.pipelines.data_ingestion.pipeline import create_pipeline as data_ingestion
from rdd.pipelines.earnings_history.pipeline import (
    create_pipeline as earnings_history,
)
from rdd.pipelines.strategies.pipeline import create_pipeline as strategies
from rdd.pipelines.valuation_ratios.pipeline import (
    create_pipeline as valuation_ratios,
)


def register_pipelines() -> dict[str, Pipeline]:
    """Register all project pipelines."""
    di = data_ingestion()
    ci = company_info()
    cn = company_news()
    cf = company_financials()
    vr = valuation_ratios()
    ac = analyst_consensus()
    eh = earnings_history()
    st = strategies()
    return {
        "__default__": di + ci + cn + cf + vr + ac + eh + st,
        "data_ingestion": di,
        "company_info": ci,
        "company_news": cn,
        "company_financials": cf,
        "valuation_ratios": vr,
        "analyst_consensus": ac,
        "earnings_history": eh,
        "strategies": st,
    }
