"""News analysis pipeline — GenAI bull/bear agent discussion."""

from kedro.pipeline import Pipeline, node

from rdd.pipelines.feature_engineering.news_analysis.nodes import analyze_news


def create_pipeline(**_kwargs) -> Pipeline:
    """Create the news analysis pipeline."""
    return Pipeline(
        nodes=[
            node(
                func=analyze_news,
                inputs=[
                    "raw_company_news",
                    "raw_news_analysis_existing",
                    "params:news_analysis",
                ],
                outputs="raw_news_analysis",
                name="analyze_news",
            ),
        ]
    )
