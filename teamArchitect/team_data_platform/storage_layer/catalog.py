from __future__ import annotations

from pathlib import Path
from typing import Any

from .json_store import atomic_write_json, list_json_files, load_json


class IcebergCatalogRepository:
    """File-backed stand-in for the PostgreSQL Iceberg catalog."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def upsert_entry(self, record_id: str, entry: dict[str, Any]) -> None:
        atomic_write_json(self.root / f"{record_id}.json", entry)

    def get_entry(self, record_id: str) -> dict[str, Any] | None:
        return load_json(self.root / f"{record_id}.json", None)

    def list_entries(self) -> list[dict[str, Any]]:
        return [load_json(path, {}) for path in list_json_files(self.root)]

    def delete_entry(self, record_id: str) -> None:
        path = self.root / f"{record_id}.json"
        if path.exists():
            path.unlink()