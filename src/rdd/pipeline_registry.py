"""Project pipeline registry."""

from kedro.pipeline import Pipeline

from rdd.pipelines.data_ingestion.analyst_consensus.pipeline import (
    create_pipeline as analyst_consensus,
)
from rdd.pipelines.data_ingestion.company_financials.pipeline import (
    create_pipeline as company_financials,
)
from rdd.pipelines.data_ingestion.company_info.pipeline import (
    create_pipeline as company_info,
)
from rdd.pipelines.data_ingestion.company_news.pipeline import (
    create_pipeline as company_news,
)
from rdd.pipelines.data_ingestion.earnings_history.pipeline import (
    create_pipeline as earnings_history,
)
from rdd.pipelines.data_ingestion.stock_prices.pipeline import (
    create_pipeline as stock_prices,
)
from rdd.pipelines.data_ingestion.valuation_ratios.pipeline import (
    create_pipeline as valuation_ratios,
)
from rdd.pipelines.feature_engineering.news_analysis.pipeline import (
    create_pipeline as news_analysis,
)
from rdd.pipelines.feature_engineering.strategies.pipeline import (
    create_pipeline as strategies,
)


def register_pipelines() -> dict[str, Pipeline]:
    """Register all project pipelines."""
    sp = stock_prices()
    ci = company_info()
    cn = company_news()
    cf = company_financials()
    vr = valuation_ratios()
    ac = analyst_consensus()
    eh = earnings_history()
    st = strategies()
    na = news_analysis()
    return {
        "__default__": sp + ci + cn + cf + vr + ac + eh + st + na,
        "stock_prices": sp,
        "company_info": ci,
        "company_news": cn,
        "company_financials": cf,
        "valuation_ratios": vr,
        "analyst_consensus": ac,
        "earnings_history": eh,
        "strategies": st,
        "news_analysis": na,
    }
