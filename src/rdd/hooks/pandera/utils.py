"""Utilities for resolving Pandera schemas from catalog metadata config."""

from __future__ import annotations

import functools
import importlib

from pandera.api.pandas.model import DataFrameModel


def resolve_schema(schema_config: dict) -> type[DataFrameModel]:
    """Return a DataFrameModel class from a catalog metadata schema block.

    Only ``type: python.model`` is supported — the ``object_path`` must point
    to a class that subclasses ``pandera.pandas.DataFrameModel``.
    """
    if schema_config["type"] == "python.model":
        return resolve_dataframe_model(schema_config["object_path"])
    raise ValueError(f"Unsupported schema config type: {schema_config['type']!r}")


@functools.cache
def resolve_dataframe_model(object_path: str) -> type[DataFrameModel]:
    """Import and return a DataFrameModel class by dotted path.

    Results are cached so each schema class is imported at most once per
    process, regardless of how many datasets reference it.
    """
    module_path, _, class_name = object_path.rpartition(".")
    module = importlib.import_module(module_path)
    return getattr(module, class_name)
