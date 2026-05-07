"""Project pipeline registry."""

from kedro.pipeline import Pipeline

from rdd.pipelines.backtest.pipeline import (
    create_price_strategies_pipeline as backtest_price_strategies,
)
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
    create_pipeline as signals,
)
from rdd.pipelines.strategies.ai_fundamental_screen.pipeline import (
    create_pipeline as ai_fundamental_screen,
)
from rdd.pipelines.strategies.portfolio_performance.pipeline import (
    create_pipeline as portfolio_performance,
)
from rdd.pipelines.strategies.price_strategies.pipeline import (
    create_pipeline as price_strategies,
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
    sg = signals()
    na = news_analysis()
    afs = ai_fundamental_screen()
    ps = price_strategies()
    bps = backtest_price_strategies()
    pp = portfolio_performance(
        variants=[
            "ai_fundamental_screen",
            "donchian_breakout",
            "high_52w",
            "cross_sect_momentum",
            "obv_momentum",
            "adx_trend",
        ]
    )
    return {
        "__default__": sp + ci + cn + cf + vr + ac + eh + sg + na + afs + pp,
        "stock_prices": sp,
        "company_info": ci,
        "company_news": cn,
        "company_financials": cf,
        "valuation_ratios": vr,
        "analyst_consensus": ac,
        "earnings_history": eh,
        "signals": sg,
        "news_analysis": na,
        "ai_fundamental_screen": afs,
        "price_strategies": ps,
        "backtest_price_strategies": bps,
        "portfolio_performance": pp,
    }
