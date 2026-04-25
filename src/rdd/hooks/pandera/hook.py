"""Kedro hook that runs Pandera validation on catalog datasets at node boundaries."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from kedro.framework.hooks import hook_impl
from kedro.io import CatalogProtocol
from kedro.pipeline import Node
from pandera.errors import SchemaError, SchemaErrors

from rdd.hooks.pandera.utils import resolve_schema


class PanderaHook:
    """Validate datasets against their Pandera schema at node boundaries.

    Attach a schema to any catalog dataset via its ``metadata`` block:

    .. code-block:: yaml

        my_dataset:
          type: pandas.ParquetDataset
          filepath: data/my_dataset.parquet
          metadata:
            pandera:
              schema:
                type: python.model
                object_path: rdd.schemas.my_module.MySchema

    Validation runs automatically:
    - ``before_node_run`` — validates node inputs (catches bad upstream data)
    - ``after_node_run``  — validates node outputs (catches node logic bugs)

    Inputs already validated in a given pipeline run are cached and skipped on
    subsequent nodes that consume the same dataset.
    """

    def __init__(self) -> None:
        self._validated_inputs: set[str] = set()

    @property
    def _log(self) -> logging.Logger:
        return logging.getLogger(__name__)

    @hook_impl
    def before_pipeline_run(self) -> None:
        self._validated_inputs.clear()

    @hook_impl
    def before_node_run(
        self,
        node: Node,
        catalog: CatalogProtocol,
        inputs: dict[str, Any],
    ) -> None:
        self._validate_datasets(node, catalog, inputs, cache=True)

    @hook_impl
    def after_node_run(
        self,
        node: Node,
        catalog: CatalogProtocol,
        outputs: dict[str, Any],
    ) -> None:
        self._validate_datasets(node, catalog, outputs, cache=False)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _validate_datasets(
        self,
        node: Node,
        catalog: CatalogProtocol,
        datasets: dict[str, Any],
        *,
        cache: bool,
    ) -> None:
        for name, data in datasets.items():
            if cache and name in self._validated_inputs:
                self._log.debug(
                    "(pandera) Skipping '%s' — already validated this run", name
                )
                continue

            schema_config = self._get_schema_config(catalog, name)
            if schema_config is None:
                continue

            schema_cls = resolve_schema(schema_config)
            validate_kwargs: dict[str, Any] = (
                catalog.get(name).metadata.get("pandera", {}).get("validate_kwargs", {})  # type: ignore[union-attr]
            )

            frames = self._extract_frames(data)
            if not frames:
                continue

            failed = False
            for partition_key, df in frames.items():
                label = f"{name}[{partition_key}]" if partition_key else name
                try:
                    schema_cls.validate(df, lazy=True, **validate_kwargs)
                except (SchemaError, SchemaErrors) as exc:
                    self._log.error(
                        "(pandera) '%s' failed schema validation in node '%s':\n%s",
                        label,
                        node.name,
                        exc,
                    )
                    failed = True
                    raise

            if cache and not failed:
                self._validated_inputs.add(name)
                self._log.info("(pandera) '%s' passed schema validation", name)

    def _get_schema_config(
        self, catalog: CatalogProtocol, name: str
    ) -> dict | None:
        dataset = catalog.get(name)
        metadata = getattr(dataset, "metadata", None)
        if not isinstance(metadata, dict):
            return None
        pandera_cfg = metadata.get("pandera")
        if not isinstance(pandera_cfg, dict):
            return None
        return pandera_cfg.get("schema")

    @staticmethod
    def _extract_frames(data: Any) -> dict[str, pd.DataFrame]:
        """Return a flat mapping of label → DataFrame suitable for validation.

        Handles:
        - ``pd.DataFrame``               → ``{"": df}``
        - ``dict[str, pd.DataFrame]``    → each value that is a DataFrame
        - anything else                  → ``{}`` (silently skipped)
        """
        if isinstance(data, pd.DataFrame):
            return {"": data}
        if isinstance(data, dict):
            return {k: v for k, v in data.items() if isinstance(v, pd.DataFrame)}
        return {}
