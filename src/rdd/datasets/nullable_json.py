"""NullableJSONDataset — returns {} when the target file does not exist."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from kedro.io import AbstractDataset


class NullableJSONDataset(AbstractDataset):
    """JSONDataset that returns an empty dict instead of raising when the file is missing.

    Used for datasets that may not exist on the first pipeline run (e.g. the
    live portfolio state before any trades have been executed).
    """

    def __init__(self, filepath: str) -> None:
        """Initialise with a file path to the target JSON file."""
        self._filepath = Path(filepath)

    def _load(self) -> dict[str, Any]:
        if not self._filepath.exists():
            return {}
        with open(self._filepath) as f:
            return json.load(f)

    def _save(self, data: dict[str, Any]) -> None:
        self._filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(self._filepath, "w") as f:
            json.dump(data, f, indent=2)

    def _describe(self) -> dict[str, Any]:
        return {"filepath": str(self._filepath)}

    def _exists(self) -> bool:
        return self._filepath.exists()
