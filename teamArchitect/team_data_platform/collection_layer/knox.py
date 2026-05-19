from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

from ..storage_layer.json_store import atomic_write_json, load_json


class KnoxSourceService:
    """Test-only source provider that mimics pull-based reads from Knox."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def register_record(
        self,
        source_system: str,
        record_id: str,
        payload: dict[str, Any],
    ) -> None:
        atomic_write_json(
            self.root / source_system / f"{record_id}.json",
            deepcopy(payload),
        )

    def pull_record(self, source_system: str, record_id: str) -> dict[str, Any] | None:
        document = load_json(self.root / source_system / f"{record_id}.json", None)
        if document is None:
            return None
        return deepcopy(document)

    def delete_record(self, source_system: str, record_id: str) -> None:
        path = self.root / source_system / f"{record_id}.json"
        if path.exists():
            path.unlink()