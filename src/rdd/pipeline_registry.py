"""Project pipeline registry."""

from kedro.pipeline import Pipeline

from rdd.pipelines.data_ingestion.pipeline import create_pipeline as data_ingestion


def register_pipelines() -> dict[str, Pipeline]:
    """Register all project pipelines."""
    return {
        "__default__": data_ingestion(),
        "data_ingestion": data_ingestion(),
    }
