"""Guard against dead catalog entries and unused parameter namespaces.

Every key defined in conf/base/catalog/**/*.yml must appear as an input or
output in at least one registered pipeline.  Every top-level key in
conf/base/parameters/**/*.yml must be referenced as ``params:<key>`` by at
least one node.

These tests fail fast when a catalog entry or parameter block is added without
a corresponding pipeline wire-up, or when a pipeline is removed without
cleaning up its config.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from rdd.pipeline_registry import register_pipelines

_CONF_BASE = Path(__file__).parent.parent / "conf" / "base"


def _yaml_top_level_keys(glob: str) -> set[str]:
    """Return every top-level key across all YAML files matching *glob*."""
    keys: set[str] = set()
    for path in _CONF_BASE.glob(glob):
        data = yaml.safe_load(path.read_text()) or {}
        keys.update(data.keys())
    return keys


def _referenced_names() -> tuple[set[str], set[str]]:
    """Return ``(catalog_names, param_names)`` referenced across all pipelines.

    Iterates over every node in every registered pipeline to collect the full
    set of dataset and parameter names, then splits them into catalog refs
    (anything not prefixed with ``params:``) and parameter refs (the part
    after ``params:``).
    """
    all_names: set[str] = set()
    for pipeline in register_pipelines().values():
        for node in pipeline.nodes:
            all_names.update(node.inputs)
            all_names.update(node.outputs)

    param_names = {n.removeprefix("params:") for n in all_names if n.startswith("params:")}
    catalog_names = {n for n in all_names if not n.startswith("params:")}
    return catalog_names, param_names


def test_no_dead_catalog_entries() -> None:
    """Every catalog entry must be an input or output of at least one pipeline node."""
    catalog_keys = _yaml_top_level_keys("catalog/**/*.yml")
    used_catalog, _ = _referenced_names()
    dead = catalog_keys - used_catalog
    assert not dead, (
        f"Catalog entries not referenced in any pipeline: {sorted(dead)}\n"
        "Either wire them into a pipeline or remove them from the catalog."
    )


def test_no_dead_parameters() -> None:
    """Every top-level parameter namespace must be used by at least one pipeline node."""
    param_keys = _yaml_top_level_keys("parameters/**/*.yml")
    _, used_params = _referenced_names()
    dead = param_keys - used_params
    assert not dead, (
        f"Parameter namespaces not referenced in any pipeline: {sorted(dead)}\n"
        "Either reference them as 'params:<key>' in a pipeline node or remove them."
    )
