"""Valuation ratios derivation pipeline."""

from kedro.pipeline import Pipeline, node

from rdd.pipelines.valuation_ratios.nodes import compute_valuation_ratios


def create_pipeline(**_kwargs) -> Pipeline:
    """Create the valuation ratios pipeline."""
    return Pipeline(
        nodes=[
            node(
                func=compute_valuation_ratios,
                inputs=[
                    "raw_company_financials_quarterly_existing",
                    "raw_company_info_existing",
                ],
                outputs="raw_valuation_ratios",
                name="compute_valuation_ratios",
            ),
        ]
    )
