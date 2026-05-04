"""Claude Fundamental strategy pipeline — three Claude agent stages."""

from kedro.pipeline import Pipeline, node

from rdd.pipelines.strategies.claude_fundamental.nodes import (
    construct_portfolio,
    rebalance_portfolio,
    record_holdings,
    score_tickers,
)


def create_pipeline(**_kwargs) -> Pipeline:
    """Create the claude_fundamental strategy pipeline."""
    return Pipeline(
        nodes=[
            node(
                func=score_tickers,
                inputs=[
                    "raw_valuation_ratios_existing",
                    "raw_analyst_consensus",
                    "raw_earnings_history",
                    "raw_company_info",
                    "raw_company_financials_quarterly_existing",
                    "stock_analyses",
                    "raw_news_analysis_existing",
                    "params:claude_fundamental",
                ],
                outputs="portfolio_ticker_scores",
                name="score_tickers",
            ),
            node(
                func=construct_portfolio,
                inputs=[
                    "portfolio_ticker_scores",
                    "raw_company_info",
                    "params:claude_fundamental",
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
                    "params:claude_fundamental",
                ],
                outputs="portfolio_trades",
                name="rebalance_portfolio",
            ),
            node(
                func=record_holdings,
                inputs=[
                    "portfolio_allocation",
                    "params:claude_fundamental",
                ],
                outputs="claude_fundamental.holdings",
                name="record_holdings",
            ),
        ]
    )
