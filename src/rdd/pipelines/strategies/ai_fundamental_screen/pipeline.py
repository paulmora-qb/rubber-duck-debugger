"""AI Fundamental Screen strategy pipeline — three Claude agent stages."""

from kedro.pipeline import Pipeline, node

from rdd.pipelines.strategies.ai_fundamental_screen.nodes import (
    construct_portfolio,
    rebalance_portfolio,
    record_holdings,
    score_tickers,
)


def create_pipeline(variant: str = "monthly", **_kwargs) -> Pipeline:
    """Create the ai_fundamental_screen strategy pipeline.

    Args:
        variant: Strategy cadence — ``"monthly"`` (1st of month) or
            ``"weekly"`` (every Friday).  Each variant has its own catalog
            entries, params file, and holdings path so they are tracked
            independently.
    """
    name = f"ai_fundamental_screen_{variant}"
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
                    f"params:{name}",
                ],
                outputs=f"{name}_ticker_scores",
                name="score_tickers",
            ),
            node(
                func=construct_portfolio,
                inputs=[
                    f"{name}_ticker_scores",
                    "raw_company_info",
                    f"params:{name}",
                ],
                outputs=f"{name}_allocation",
                name="construct_portfolio",
            ),
            node(
                func=rebalance_portfolio,
                inputs=[
                    f"{name}_allocation",
                    "live_portfolio",
                    f"{name}_ticker_scores",
                    f"params:{name}",
                ],
                outputs=f"{name}_trades",
                name="rebalance_portfolio",
            ),
            node(
                func=record_holdings,
                inputs=[
                    f"{name}_allocation",
                    f"params:{name}",
                ],
                outputs=f"{name}.holdings",
                name="record_holdings",
            ),
        ]
    )
