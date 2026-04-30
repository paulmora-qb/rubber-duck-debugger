"""Portfolio construction pipeline — three Claude agent stages."""

from kedro.pipeline import Pipeline, node

from rdd.pipelines.portfolio_construction.nodes import (
    construct_portfolio,
    rebalance_portfolio,
    score_tickers,
)


def create_pipeline(**_kwargs) -> Pipeline:
    """Create the portfolio_construction pipeline."""
    return Pipeline(
        nodes=[
            node(
                func=score_tickers,
                inputs=[
                    "raw_valuation_ratios",
                    "raw_analyst_consensus",
                    "raw_earnings_history",
                    "raw_company_info",
                    "raw_company_financials_quarterly",
                    "stock_analyses",
                    "raw_news_analysis_existing",
                    "params:portfolio_construction",
                ],
                outputs="portfolio_ticker_scores",
                name="score_tickers",
            ),
            node(
                func=construct_portfolio,
                inputs=[
                    "portfolio_ticker_scores",
                    "raw_company_info",
                    "params:portfolio_construction",
                ],
                outputs="portfolio_allocation",
                name="construct_portfolio",
            ),
            node(
                func=rebalance_portfolio,
                inputs=[
                    "portfolio_allocation",
                    "live_portfolio",
                    "portfolio_ticker_scores",
                    "params:portfolio_construction",
                ],
                outputs="portfolio_trades",
                name="rebalance_portfolio",
            ),
        ]
    )
